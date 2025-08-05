package org.apache.hadoop.fs.azurebfs.services;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.enums.AbfsPrefetchMetricsEnum;

/**
 * The AbfsPrefetchMetricsAnalyzer class is responsible for analyzing and managing
 * metrics related to prefetching in Azure Blob File System (ABFS). It uses a
 * singleton pattern to ensure a single shared instance is used across the application.
 */
public class AbfsPrefetchMetricsAnalyzer {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsPrefetchMetricsAnalyzer.class);
  private static final ReentrantLock LOCK = new ReentrantLock();

  private final int windowSize;
  private final double throttlingThreshold;
  private final Map<AbfsPrefetchMetricsEnum, AtomicLong[]> metricBuffers = new EnumMap<>(AbfsPrefetchMetricsEnum.class);
  private final Map<AbfsPrefetchMetricsEnum, int[]> slotSeconds = new EnumMap<>(AbfsPrefetchMetricsEnum.class);

  private final ScheduledExecutorService scheduler =
      Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r);
        t.setDaemon(true);
        t.setName("AbfsPrefetchMetricsAggregator");
        return t;
      });

  //required for testing purpose
  private volatile ScheduledFuture<?> aggregatorTask;

  private volatile boolean skipPrefetch = false;

  // Singleton instance of the class.
  private static AbfsPrefetchMetricsAnalyzer singleton;

  /**
   * Retrieves the singleton instance of AbfsPrefetchMetricsAnalyzer.
   * If the instance does not exist, it initializes it based on the configuration.
   *
   * @param abfsConfiguration The configuration object for ABFS.
   * @return The singleton instance of AbfsPrefetchMetricsAnalyzer.
   */
  @VisibleForTesting
  static synchronized AbfsPrefetchMetricsAnalyzer getInstance(
      AbfsConfiguration abfsConfiguration) {

    if(singleton != null){
      return singleton;
    }

    AbfsPrefetchMetricsAnalyzer abfsPrefetchMetricsAnalyzer = null;
    // If singleton is enabled, use a static instance of the class throughout
    if (abfsConfiguration.isPrefetchSkippingEnabled()) {
      abfsPrefetchMetricsAnalyzer = AbfsPrefetchMetricsAnalyzer.initializeSingleton(abfsConfiguration);
      abfsPrefetchMetricsAnalyzer.startMetricAggregator();
      LOG.trace("AbfsPrefetchMetricsAnalyzer singleton instance created.");
    }

    return abfsPrefetchMetricsAnalyzer;
  }

  /**
   * Initializes the singleton instance of AbfsPrefetchMetricsAnalyzer.
   *
   * @param abfsConfiguration The configuration object for ABFS.
   * @return The initialized singleton instance.
   */
  static AbfsPrefetchMetricsAnalyzer initializeSingleton(AbfsConfiguration abfsConfiguration) {
    if (singleton == null) {
      LOCK.lock();
      try {
        if (singleton == null) {
          singleton = new AbfsPrefetchMetricsAnalyzer(abfsConfiguration);
          LOG.trace("Initialized AbfsPrefetchMetricsAnalyzer singleton with windowSize={} and throttlingThreshold={}",
              singleton.windowSize, singleton.throttlingThreshold);
        }
      }
      finally {
        LOCK.unlock();
      }
    }
    return singleton;
  }

  /**
   * Constructor for AbfsPrefetchMetricsAnalyzer.
   * Initializes metrics buffers and starts the metrics aggregator task.
   *
   * @param abfsConfiguration The configuration object for ABFS.
   */
  @VisibleForTesting
  public AbfsPrefetchMetricsAnalyzer(AbfsConfiguration abfsConfiguration) {
    this.windowSize = abfsConfiguration.getPrefetchMetricsDefaultSpan();
    this.throttlingThreshold = abfsConfiguration.getThrottlingThreshold();

    // Initialize metric buffers and slot seconds for each metric type.
    for (AbfsPrefetchMetricsEnum metric : AbfsPrefetchMetricsEnum.values()) {
      AtomicLong[] buffer = new AtomicLong[windowSize+1];
      int[] generations = new int[windowSize+1];
      for (int i = 0; i < windowSize+1; i++) {
        buffer[i] = new AtomicLong(0);
        generations[i] = -1; // Initialize with invalid generation.
      }
      metricBuffers.put(metric, buffer);
      slotSeconds.put(metric, generations);
    }
    startMetricAggregator();
  }

  private long getCurrentSecond() {
    return System.currentTimeMillis() / 1000;
  }

  /**
   * Increments the value of a specific metric for the current time slot.
   *
   * @param metric The metric to increment.
   */
  @VisibleForTesting
  public void incrementMetricValue(AbfsPrefetchMetricsEnum metric) {
    long currentSecond = getCurrentSecond();
    int index = (int) (currentSecond % (windowSize + 1));

    AtomicLong[] buffer = metricBuffers.get(metric);
    int[] slot = slotSeconds.get(metric);

    // Reset the buffer if the time slot has changed and save the current second.
    if (slot[index] != currentSecond) {
      buffer[index].set(0);
      slot[index] = (int) currentSecond;
    }
    buffer[index].incrementAndGet();
  }

  /**
   * Calculates the sum of a specific metric over the configured time window.
   *
   * @param metric The metric to calculate the sum for.
   * @return The sum of the metric values.
   */
  @VisibleForTesting
  public long getSumForMetric(AbfsPrefetchMetricsEnum metric) {
    long currentSecond = getCurrentSecond();
    long cutoffStart = currentSecond - windowSize + 1; // Last N-1 seconds.

    AtomicLong[] buffer = metricBuffers.get(metric);
    int[] slot = slotSeconds.get(metric);

    long sum = 0;
    for (int i = 0; i < windowSize + 1; i++) {
      if (slot[i] >= cutoffStart && slot[i] <= currentSecond) {
        sum += buffer[i].get();
      }
    }
    return sum;
  }

  /**
   * Starts the scheduled task to aggregate metrics and update the skipPrefetch flag.
   * The task runs every second and checks the total number of requests
   */
  private void startMetricAggregator() {
    LOG.trace("Starting AbfsPrefetchMetricsAggregator scheduled task.");
    aggregatorTask = scheduler.scheduleAtFixedRate(() -> {
      try {
        long totalReqs = getSumForMetric(AbfsPrefetchMetricsEnum.TOTAL_NUMBER_OF_REQUESTS);
        long totalReadReqs = getSumForMetric(AbfsPrefetchMetricsEnum.TOTAL_NUMBER_OF_READ_REQUESTS);
        long egressThrottled = getSumForMetric(AbfsPrefetchMetricsEnum.EGRESS_THROTTLED);
        long iopsThrottled = getSumForMetric(AbfsPrefetchMetricsEnum.IOPS_THROTTLED);

        // Calculate throttling rates based on last window metrics
        // If total requests are zero but throttling occurred, skip prefetch due to anomalous throttling.
        // If requests are too few to reliably compute a throttling rate, treat rate as 0.0.
        // Only skip prefetch if anomalous throttling OR throttling rate exceeds threshold.
        boolean isIopsAnomalous = (totalReqs == 0 && iopsThrottled != 0);
        boolean isIopsSampleSmall = isIopsAnomalous || (totalReqs < 10 && iopsThrottled < 5);
        double iopsThrottlingRate = isIopsSampleSmall ? 0.0 : (iopsThrottled * 100.0) / totalReqs;

        boolean isEgressAnomalous = (totalReadReqs == 0 && egressThrottled != 0);
        boolean isEgressSampleSmall = isEgressAnomalous || (totalReadReqs < 10 && egressThrottled < 5);
        double egressThrottlingRate = isEgressSampleSmall ? 0.0 : (egressThrottled * 100.0) / totalReadReqs;

        skipPrefetch =
            isIopsAnomalous || isEgressAnomalous ||
                (iopsThrottlingRate >= throttlingThreshold) ||
                (egressThrottlingRate >= throttlingThreshold);

        System.out.println(totalReqs + ": " + totalReadReqs + ": " + egressThrottled +": "+ egressThrottlingRate+ ": "+ skipPrefetch);
      } catch (Exception e) {
        LOG.error("Exception in AbfsPrefetchMetricsAggregator: ", e);
      }
    }, 0, 1, TimeUnit.SECONDS);
  }

  /**
   * Checks whether prefetching should be skipped based on the current metrics.
   *
   * @return True if prefetching should be skipped, false otherwise.
   */
  public boolean shouldSkipPrefetch() {
    return skipPrefetch;
  }

  @VisibleForTesting
  public void shutdown() {
    if (aggregatorTask != null) {
      aggregatorTask.cancel(false);
    }
    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(3, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      scheduler.shutdownNow();
    }
  }
}
