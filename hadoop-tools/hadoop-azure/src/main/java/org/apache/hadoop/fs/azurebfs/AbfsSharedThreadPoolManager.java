package org.apache.hadoop.fs.azurebfs;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;

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

  private BlockingThreadPoolExecutorService writeExecutorService;
  private ListeningExecutorService writeThreadPoolExecutor;
  private ThreadPoolExecutor readThreadPoolExecutor;
  private ThreadPoolExecutor sharedExecutorService;
  private ListeningExecutorService sharedThreadPoolExecutor;

  private final ConcurrentHashMap<Object, Callable<Void>> readTaskMap = new ConcurrentHashMap<>();

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
    /*
     * Default Thread Pool For Write Operations. Same semantics as trunk. Fixed Size
     * There won't be any waiting on the queue as the queue size is 0.
     * There will be an indefinite wait on semaphore if tasks are queued beyond size.
     * This is to avoid OOM errors due to excessive write tasks getting queued up.
     */

    int maxWriteThreads = configuration.getWriteConcurrentRequestCount();
    int maxWriteQueueSize = configuration.getMaxWriteRequestsToQueue();
    writeExecutorService = BlockingThreadPoolExecutorService.newInstance(
        maxWriteThreads,
        0, // To avoid waiting on queue
        10L,
        TimeUnit.SECONDS,
        "abfs-write"
    );
    writeThreadPoolExecutor = MoreExecutors.listeningDecorator(writeExecutorService);

    /*
     * Default Thread Pool For Read Operations. Fixed thread pool with size 8.
     * Uses an unbounded queue. If tasks are submitted beyond the pool size they will wait on queue.
     * No wait on semaphore needed as read tasks cannot be queued indefinitely.
     * They will be limited by memory utilization of the system.
     */
    readThreadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(
        8, newDaemonThreadFactory("abfs-read"));

    /*
     * Shared Thread Pool for both read and write operations when their own pools are exhausted.
     * Can scale up and down between min and max size, based on resource utilization.
     * Uses Synchronous Queue so that tasks will wait for a thread to be free.
     * While submitting tasks to shared pool, only read tasks will be added to the queue.
     * Write tasks will be added only if they can be immediately picked, else they will go nd wait on write pool itself.
     */
    sharedExecutorService = new ThreadPoolExecutor(
        configuration.bbb(),
        Integer.MAX_VALUE,
        configuration.getSharedThreadPoolKeepAliveMillis(),
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(),
        newDaemonThreadFactory("abfs-shared"));
    sharedThreadPoolExecutor = MoreExecutors.listeningDecorator(sharedExecutorService);
  }

  public ListenableFuture<Void> submitWriteTask(Callable<Void> task) {
    if (writeExecutorService.getWaitingCount() == 0) {
      LOG.debug("Submitting write task to write thread pool");
      return writeThreadPoolExecutor.submit(task);
    } else if (sharedExecutorService.getActiveCount() < sharedExecutorService.getCorePoolSize()) {
      LOG.debug("Submitting write task to shared thread pool");
      return sharedThreadPoolExecutor.submit(task);
    } else {
      return writeThreadPoolExecutor.submit(task);
    }
  }

  public void submitReadTask(Runnable task, boolean canWait) {
    if (readThreadPoolExecutor.getActiveCount() < readThreadPoolExecutor.getMaximumPoolSize()) {
      LOG.debug("Submitting read task to read thread pool");
      readThreadPoolExecutor.submit(task);
    } else {
      LOG.debug("Submitting read task to shared thread pool");
      sharedThreadPoolExecutor.submit(task);
    }
  }
}
