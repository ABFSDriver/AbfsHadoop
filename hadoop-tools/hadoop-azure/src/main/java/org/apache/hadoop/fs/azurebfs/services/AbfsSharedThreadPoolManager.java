package org.apache.hadoop.fs.azurebfs.services;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;

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

  private int writeCorePoolSize;
  private int writeQueueSize;
  private int readCorePoolSize;
  private int sharedCorePoolSize;

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
    readCorePoolSize = 8;
    sharedCorePoolSize = configuration.getMinSharedThreadPoolSize();
    writeThreadPoolTTLMillis = 10L * 1000L;
    sharedThreadPoolTTLMillis = configuration.getSharedThreadPoolKeepAliveMillis();

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
  }

  public synchronized ListenableFuture<Void> submitWriteTask(Callable<Void> task) {
    if (writeExecutorService.getActiveCount() < writeCorePoolSize) {
      LOG.debug("Submitting write task to write thread pool");
      return writeThreadPoolExecutor.submit(task);
    } else if (sharedExecutorService.getActiveCount() < sharedExecutorService.getCorePoolSize()) {
      LOG.debug("Submitting write task to shared thread pool");
      return sharedThreadPoolExecutor.submit(task);
    } else {
      LOG.debug("Submitting write task to write thread pool for waiting");
      return writeThreadPoolExecutor.submit(task);
    }
  }

  public synchronized void submitReadTask(String key, Callable<Void> task) {
    TrackableTask readTask = new TrackableTask(task);
    readTasksMap.put(key, readTask);
    ListenableFuture<Void> future;
    if (readThreadPoolExecutorService.getActiveCount() < readCorePoolSize) {
      LOG.debug("Submitting read task for key {} to read thread pool", key);
      future = readThreadPoolExecutor.submit(readTask);
    } else {
      LOG.debug("Submitting read task for key {} to shared thread pool", key);
      future = sharedThreadPoolExecutor.submit(readTask);
    }
    readFuturesMap.put(key, future);
  }

  public synchronized boolean isReadTaskInProgress(String key) {
    TrackableTask readTask = readTasksMap.get(key);
    if (readTask != null) {
      return readTask.isRunning();
    }
    return false;
  }

  public synchronized boolean isReadTaskInQueue(String key) {
    TrackableTask readTask = readTasksMap.get(key);
    if (readTask != null) {
      return readTask.isQueued();
    }
    return false;
  }

  public synchronized boolean removeReadTask(String key) {
    Future<Void> future = readFuturesMap.get(key);
    if (future == null) {
      return false;
    }
    boolean isCancelled = future.cancel(false);
    if (isCancelled) {
      LOG.debug("Read task for key: {} cancelled successfully", key);
      readTasksMap.remove(key);
      readFuturesMap.remove(key);
      return true;
    } else {
      LOG.debug("Read task for key: {} could not be cancelled", key);
      return false;
    }
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
  public long getWriteThreadPoolTotalPermits() {
    return writeExecutorService.getPermitCount();
  }

  @VisibleForTesting
  public long getWriteThreadPoolWaitingPermits() {
    return writeExecutorService.getWaitingCount();
  }

  @VisibleForTesting
  public long getSharedThreadPoolActiveTaskCount() {
    return sharedExecutorService.getActiveCount();
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
}
