package org.apache.hadoop.fs.azurebfs.services;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import com.sun.management.OperatingSystemMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HUNDRED_D;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ZERO;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ZERO_D;
import static org.apache.hadoop.fs.azurebfs.constants.InternalConstants.READ_THREAD_POOL_PREFIX;
import static org.apache.hadoop.fs.azurebfs.constants.InternalConstants.SHARED_THREAD_POOL_PREFIX;
import static org.apache.hadoop.fs.azurebfs.constants.InternalConstants.WRITE_THREAD_POOL_PREFIX;
import static org.apache.hadoop.util.BlockingThreadPoolExecutorService.newDaemonThreadFactory;

/**
 * Singleton Class to manage thread pools for read and write operations.
 */
public final class AbfsSharedThreadPoolManager {

  private static volatile AbfsSharedThreadPoolManager
      abfsSharedThreadPoolManager;
  private static final Logger LOG = LoggerFactory
      .getLogger(AbfsSharedThreadPoolManager.class);
  private static final ReentrantLock LOCK = new ReentrantLock();

  private static BlockingThreadPoolExecutorService writeExecutorService;
  private static ThreadPoolExecutor readThreadPoolExecutorService;
  private static ThreadPoolExecutor sharedExecutorService;

  private static ListeningExecutorService writeThreadPoolExecutor;
  private static ListeningExecutorService readThreadPoolExecutor;
  private static ListeningExecutorService sharedThreadPoolExecutor;

  private static ScheduledExecutorService cpuMonitorExecutorService;
  private int cpuThreshold;
  private int threadPoolUpscalePercentage;
  private int threadPoolDownscalePercentage;

  private int writeCorePoolSize;
  private int writeQueueSize;
  private int readCorePoolSize;
  private int sharedCorePoolSize;
  private int sharedMaxPoolSize;

  private long writeThreadPoolTTLMillis;
  private long sharedThreadPoolTTLMillis;

  private final ConcurrentHashMap<String, TrackableTask> readTasksMap = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Future<Void>> readFuturesMap = new ConcurrentHashMap<>();

  private AbfsSharedThreadPoolManager() {

  }

  public static AbfsSharedThreadPoolManager getInstance(AbfsConfiguration configuration) {
    if (abfsSharedThreadPoolManager == null) {
      LOCK.lock();
      try {
        if (abfsSharedThreadPoolManager == null) {
          abfsSharedThreadPoolManager = new AbfsSharedThreadPoolManager();
          abfsSharedThreadPoolManager.init(configuration);
        }
      } finally {
        LOCK.unlock();
      }
    }
    return abfsSharedThreadPoolManager;
  }

  private void init(AbfsConfiguration configuration) {
    writeCorePoolSize = configuration.getWriteConcurrentRequestCount();
    writeQueueSize = configuration.getMaxWriteRequestsToQueue();
    readCorePoolSize = configuration.getReadConcurrentRequestCount();
    sharedCorePoolSize = configuration.getMinSharedThreadPoolSize();
    sharedMaxPoolSize = configuration.getMaxSharedThreadPoolSize();

    writeThreadPoolTTLMillis = 10L * 1000L;
    sharedThreadPoolTTLMillis = configuration.getSharedThreadPoolKeepAliveMillis();

    cpuThreshold = configuration.getSharedThreadPoolCpuThresholdPercentage();
    threadPoolUpscalePercentage = configuration.getSharedThreadPoolUpscalePercentage();
    threadPoolDownscalePercentage = configuration.getSharedThreadPoolDownscalePercentage();

    /*
     * Default Thread Pool For Write Operations. Same semantics as trunk. Fixed Size
     * There won't be any waiting on the queue as the queue size is 0.
     * There will be an indefinite wait on semaphore if tasks are queued beyond size.
     * This is to avoid OOM errors due to excessive write tasks getting queued up.
     */
    writeExecutorService = BlockingThreadPoolExecutorService.newInstance(
        writeCorePoolSize,
        writeQueueSize, // To avoid waiting on queue
        writeThreadPoolTTLMillis, TimeUnit.MILLISECONDS,
        WRITE_THREAD_POOL_PREFIX
    );
    writeThreadPoolExecutor = MoreExecutors.listeningDecorator(writeExecutorService);

    /*
     * Default Thread Pool For Read Operations. Fixed thread pool with size 8.
     * Uses an unbounded queue. If tasks are submitted beyond the pool size they will wait on queue.
     * No wait on semaphore needed as read tasks cannot be queued indefinitely.
     * They will be limited by memory utilization of the system.
     */
    readThreadPoolExecutorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(
        readCorePoolSize, newDaemonThreadFactory(READ_THREAD_POOL_PREFIX));
    readThreadPoolExecutor = MoreExecutors.listeningDecorator(readThreadPoolExecutorService);
    readThreadPoolExecutorService.prestartAllCoreThreads();

    /*
     * Shared Thread Pool for both read and write operations when their own pools are exhausted.
     * Can scale up and down between min and max size, based on resource utilization.
     * Uses Synchronous Queue so that tasks will wait for a thread to be free.
     * While submitting tasks to shared pool, only read tasks will be added to the queue.
     * Write tasks will be added only if they can be immediately picked, else they will go nd wait on write pool itself.
     */
    sharedExecutorService = new ThreadPoolExecutor(
        sharedCorePoolSize,
        Integer.MAX_VALUE,
        sharedThreadPoolTTLMillis, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(),
        newDaemonThreadFactory(SHARED_THREAD_POOL_PREFIX));
    sharedThreadPoolExecutor = MoreExecutors.listeningDecorator(sharedExecutorService);

    if (configuration.isSharedThreadPoolDynamicScalingEnabled()) {
      cpuMonitorExecutorService = Executors.newSingleThreadScheduledExecutor(
          runnable -> {
            Thread t = new Thread(runnable, "ReadAheadV2-CPU-Monitor");
            t.setDaemon(true);
            return t;
          });
      cpuMonitorExecutorService.scheduleAtFixedRate(this::adjustThreadPool,
          configuration.getSharedThreadPoolCpuMonitoringIntervalMillis(),
          configuration.getSharedThreadPoolCpuMonitoringIntervalMillis(),
          TimeUnit.MILLISECONDS);
    }

    LOG.debug("AbfsSharedThreadPoolManager initialized with writeCorePoolSize: {}, "
        + "writeQueueSize: {}, readCorePoolSize: {}, sharedCorePoolSize: {}",
        writeCorePoolSize, writeQueueSize, readCorePoolSize, sharedCorePoolSize);
  }

  public ListenableFuture<Void> submitWriteTask(Callable<Void> task) {
    if (writeExecutorService.getActiveCount() < writeCorePoolSize) {
      LOG.debug("Submitting write task to write thread pool");
      return writeThreadPoolExecutor.submit(task);
    } else if (sharedExecutorService.getActiveCount() < sharedExecutorService.getCorePoolSize() && memoryIsBelowThreshhold()) {
      LOG.debug("Submitting write task to shared thread pool");
      return sharedThreadPoolExecutor.submit(task);
    } else {
      LOG.debug("Submitting write task to write thread pool for waiting");
      return writeThreadPoolExecutor.submit(task);
    }
  }

  public void submitReadTask(String key, Callable<Void> task) {
    TrackableTask trackableTask = new TrackableTask(task);
    ListenableFuture<Void> future;
    if (readThreadPoolExecutorService.getActiveCount() < readCorePoolSize) {
      LOG.debug("Submitting read task for key {} to read thread pool as it has idle threads", key);
      future = readThreadPoolExecutor.submit(trackableTask);
    } else if (sharedExecutorService.getActiveCount() < sharedExecutorService.getCorePoolSize()) {
      LOG.debug("Submitting read task for key {} to shared thread pool as it has idle threads", key);
      future = sharedThreadPoolExecutor.submit(trackableTask);
    } else if (readThreadPoolExecutorService.getQueue().size() < sharedExecutorService.getQueue().size()) {
      LOG.debug("Submitting read task for key {} to read thread pool queue", key);
      future = readThreadPoolExecutor.submit(trackableTask);
    } else {
      LOG.debug("Submitting read task for key {} to shared thread pool queue", key);
      future = sharedThreadPoolExecutor.submit(trackableTask);
    }
    readTasksMap.put(key, trackableTask);
    readFuturesMap.put(key, future);
  }

  public synchronized boolean tryCancelReadTask(String key) {
    Future<Void> future = readFuturesMap.get(key);
    if (future == null) {
      return false;
    }
    boolean isCancelled = future.cancel(false);
    if (isCancelled && readTasksMap.get(key).isQueued() && future.isCancelled()) {
      LOG.debug("Read task for key: {} cancelled successfully", key);
      readTasksMap.remove(key);
      readFuturesMap.remove(key);
      return true;
    } else {
      LOG.debug("Read task for key: {} could not be cancelled", key);
      return false;
    }
  }

  public void evictReadTask(String key) {
    readTasksMap.remove(key);
    readFuturesMap.remove(key);
  }

  private void adjustThreadPool() {
    int currentPoolSize = sharedExecutorService.getCorePoolSize();
    double cpuLoad = getCpuLoad() * HUNDRED_D;
    int newThreadPoolSize = currentPoolSize;
    LOG.debug("Current CPU load: {}, Current Pool size: {}",
        cpuLoad, currentPoolSize);
    if (cpuLoad < cpuThreshold) {
      // Submit more background tasks.
      newThreadPoolSize = Math.min(sharedMaxPoolSize, (int) Math.ceil(
              (currentPoolSize * (HUNDRED_D + threadPoolUpscalePercentage))
                  / HUNDRED_D));
    } else if (cpuLoad > cpuThreshold) {
      newThreadPoolSize = Math.max(sharedCorePoolSize, (int) Math.ceil(
              (currentPoolSize * (HUNDRED_D - threadPoolDownscalePercentage))
                  / HUNDRED_D));
    }
    if (newThreadPoolSize != currentPoolSize) {
      LOG.info("Adjusting shared thread pool size from {} to {}",
          currentPoolSize, newThreadPoolSize);
      sharedExecutorService.setCorePoolSize(newThreadPoolSize);
    }
  }

  private double getCpuLoad() {
    OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(
        OperatingSystemMXBean.class);
    double cpuLoad = osBean.getProcessCpuLoad();
    if (cpuLoad < ZERO) {
      return ZERO_D;
    }
    return cpuLoad;
  }

  private boolean memoryIsBelowThreshhold() {
    double memoryLoad = getMemoryLoad() * HUNDRED_D;
    LOG.debug("Current Memory load: {}", memoryLoad);
    return memoryLoad < cpuThreshold;
  }

  public static double getMemoryLoad() {
    MemoryMXBean osBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memoryUsage = osBean.getHeapMemoryUsage();
    return (double) memoryUsage.getUsed() / memoryUsage.getMax();
  }

  @VisibleForTesting
  public long getWriteThreadPoolActiveTaskCount() {
    return writeExecutorService.getActiveCount();
  }

  @VisibleForTesting
  public long getWriteThreadPoolAvailablePermitsCount() {
    return writeExecutorService.getAvailablePermits();
  }

  @VisibleForTesting
  public long getWriteThreadPoolWaitingPermits() {
    return writeExecutorService.getWaitingCount();
  }

  @VisibleForTesting
  public long getReadThreadPoolActiveTaskCount() {
    return readThreadPoolExecutorService.getActiveCount();
  }

  @VisibleForTesting
  public long getReadThreadPoolCompletedTaskCount() {
    return readThreadPoolExecutorService.getCompletedTaskCount();
  }

  @VisibleForTesting
  public long getReadThreadPoolTotalTaskCount() {
    return readThreadPoolExecutorService.getTaskCount();
  }

  @VisibleForTesting
  public long getReadThreadPoolQueueSize() {
    return readThreadPoolExecutorService.getQueue().size();
  }

  @VisibleForTesting
  public long getSharedThreadPoolActiveTaskCount() {
    return sharedExecutorService.getActiveCount();
  }

  @VisibleForTesting
  public long getSharedThreadPoolCompletedTaskCount() {
    return sharedExecutorService.getCompletedTaskCount();
  }

  @VisibleForTesting
  public long getSharedThreadPoolTotalTaskCount() {
    return sharedExecutorService.getTaskCount();
  }

  @VisibleForTesting
  public long getSharedThreadPoolQueueSize() {
    return sharedExecutorService.getQueue().size();
  }

  @VisibleForTesting
  static void testHardResetThreadPoolManager() {
    LOCK.lock();
    try {
      writeExecutorService.shutdownNow();
      readThreadPoolExecutorService.shutdownNow();
      sharedExecutorService.shutdownNow();
      abfsSharedThreadPoolManager = null;
    } finally {
      LOCK.unlock();
    }
  }

  @VisibleForTesting
  static AbfsSharedThreadPoolManager returnInstance() {
    return abfsSharedThreadPoolManager;
  }

  @VisibleForTesting
  public synchronized boolean isReadTaskInProgress(String key) {
    TrackableTask readTask = readTasksMap.get(key);
    if (readTask != null) {
      return readTask.isRunning();
    }
    return false;
  }

  @VisibleForTesting
  public synchronized boolean isReadTaskInQueue(String key) {
    TrackableTask readTask = readTasksMap.get(key);
    if (readTask != null) {
      return readTask.isQueued();
    }
    return false;
  }
}
