package org.apache.hadoop.fs.azurebfs;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.SemaphoredDelegatingExecutor;

/**
 * Singleton Class to manage thread pools for read and write operations.
 */
public final class AbfsThreadPoolManager {

  private static volatile AbfsThreadPoolManager abfsThreadPoolManager;
  private static final Logger LOG = LoggerFactory
      .getLogger(AbfsThreadPoolManager.class);
  private static final ReentrantLock LOCK = new ReentrantLock();

  private ListeningExecutorService writeThreadPoolExecutor;
  private BlockingThreadPoolExecutorService readThreadPoolExecutor;
  private SemaphoredDelegatingExecutor sharedThreadPoolExecutor;

  private final ConcurrentHashMap<Object, Runnable> readTaskMap = new ConcurrentHashMap<>();

  private AbfsThreadPoolManager() {

  }

  public static AbfsThreadPoolManager getInstance(AbfsConfiguration configuration) {
    if (abfsThreadPoolManager == null) {
      LOCK.lock();
      try {
        if (abfsThreadPoolManager == null) {
          abfsThreadPoolManager = new AbfsThreadPoolManager();
          abfsThreadPoolManager.init(configuration);
        }
      } finally {
        LOCK.unlock();
      }
    }
    return abfsThreadPoolManager;
  }

  private void init(AbfsConfiguration configuration) {
    // Initialize Write Thread Pool Executor
    int maxWriteThreads = configuration.getWriteConcurrentRequestCount();
    int maxWriteQueueSize = configuration.getMaxWriteRequestsToQueue();
    writeThreadPoolExecutor = MoreExecutors.listeningDecorator(
      new SemaphoredDelegatingExecutor(BlockingThreadPoolExecutorService.newInstance(
        maxWriteThreads,
        maxWriteQueueSize,
        10L, TimeUnit.SECONDS,
        "abfs-write"
      ), maxWriteQueueSize, true, null)
    );

    readThreadPoolExecutor = BlockingThreadPoolExecutorService.newInstance(
        8, 8,
        10L, TimeUnit.SECONDS,
        "abfs-read");

    int minThreadPoolSize = 2 * Runtime.getRuntime().availableProcessors();
    sharedThreadPoolExecutor = new SemaphoredDelegatingExecutor(BlockingThreadPoolExecutorService.newInstance(
        minThreadPoolSize, minThreadPoolSize,
        10L, TimeUnit.SECONDS,
        "abfs-shared"), maxWriteQueueSize, true, null);
  }

  public ListenableFuture<Void> submitWriteTask(Callable<Void> task) {
    return writeThreadPoolExecutor.submit(task);
  }

  public boolean submitReadTask(Object key, Runnable task) {
    LOG.debug("Submitting read task for key: {}", key);
    readTaskMap.put(key, task);
    if (readThreadPoolExecutor.getAvailablePermits() > 0) {
      LOG.debug("Available permits for read thread pool: {}",
          readThreadPoolExecutor.getAvailablePermits());
      readThreadPoolExecutor.execute(task);
      return true;
    }
    return false;
  }

  public void removeReadTask(Object key) {
    LOG.debug("Removing read task for key: {}", key);
    Runnable task = readTaskMap.remove(key);
    if (task != null) {
      readThreadPoolExecutor.removeTask(task);
    }
  }
}
