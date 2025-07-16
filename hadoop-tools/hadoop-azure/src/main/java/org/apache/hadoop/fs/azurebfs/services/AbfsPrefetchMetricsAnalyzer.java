package org.apache.hadoop.fs.azurebfs.services;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.enums.AbfsPrefetchMetricsEnum;

public class AbfsPrefetchMetricsAnalyzer {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsPrefetchMetricsAnalyzer.class);

  private static final ReentrantLock LOCK = new ReentrantLock();
  private final int windowSize;
  private final double throttlingThreshold;
  public static final AtomicLong PREFETCH_SKIPPED = new AtomicLong();
  private final Map<AbfsPrefetchMetricsEnum, AtomicLong[]> metricBuffers = new EnumMap<>(AbfsPrefetchMetricsEnum.class);
  private final Map<AbfsPrefetchMetricsEnum, long[]> metricTimestamps = new EnumMap<>(AbfsPrefetchMetricsEnum.class);
  private final AtomicReference<Double> cachedThrottlingRate = new AtomicReference<>(0.0);
  private final ScheduledExecutorService scheduler =
      Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r);
        t.setDaemon(true);
        t.setName("AbfsPrefetchMetricsAggregator");
        return t;
      });


  private volatile boolean skipPrefetch = false;

  private static AbfsPrefetchMetricsAnalyzer singleton;

  static synchronized AbfsPrefetchMetricsAnalyzer getInstance(
      AbfsConfiguration abfsConfiguration) {
    AbfsPrefetchMetricsAnalyzer abfsPrefetchMetricsAnalyzer = null;

    // If singleton is enabled use a static instance of the abfsPrefetchMetricsAnalyzer class for all accounts
    if (abfsConfiguration.isPrefetchSkippingEnabled()) {
      abfsPrefetchMetricsAnalyzer = AbfsPrefetchMetricsAnalyzer.initializeSingleton(abfsConfiguration);
    }

    return abfsPrefetchMetricsAnalyzer;
  }

  static AbfsPrefetchMetricsAnalyzer initializeSingleton(AbfsConfiguration abfsConfiguration) {
    if (singleton == null) {
      LOCK.lock();
      try {
        if (singleton == null) {
          singleton = new AbfsPrefetchMetricsAnalyzer(abfsConfiguration);
          LOG.trace("");
        }
      }
      finally {
        LOCK.unlock();
      }
    }
    return singleton;
  }

  public AbfsPrefetchMetricsAnalyzer(AbfsConfiguration abfsConfiguration) {
    // Use config values for window size and threshold
    this.windowSize = abfsConfiguration.getPrefetchMetricsDefaultSpan();
    this.throttlingThreshold = abfsConfiguration.getThrottlingThreshold();

    for (AbfsPrefetchMetricsEnum metric : AbfsPrefetchMetricsEnum.values()) {
      AtomicLong[] buffer = new AtomicLong[windowSize];
      long[] timestamps = new long[windowSize];
      for (int i = 0; i < windowSize; i++) {
        buffer[i] = new AtomicLong(0);
      }
      metricBuffers.put(metric, buffer);
      metricTimestamps.put(metric, timestamps);
    }
    startMetricAggregator();
  }

  public void incrementMetricValue(AbfsPrefetchMetricsEnum metric) {
    long currentSecond = System.currentTimeMillis() / 1000;
    int index = (int) (currentSecond % windowSize);

    AtomicLong[] buffer = metricBuffers.get(metric);
    long[] timestamps = metricTimestamps.get(metric);

    synchronized (buffer) {
      if (timestamps[index] != currentSecond) {
        buffer[index].set(0);
        timestamps[index] = currentSecond;
      }
      buffer[index].incrementAndGet();
    }
    for (Map.Entry<AbfsPrefetchMetricsEnum, AtomicLong[]> entry : metricBuffers.entrySet()) {
      System.out.println(entry.getKey() + ": " +
          Arrays.toString(
              Arrays.stream(entry.getValue()).mapToLong(AtomicLong::get).toArray()));
    }
  }

  public void incrementPrefetchSkipped() {
    PREFETCH_SKIPPED.incrementAndGet();
  }

  private long getSumForMetric(AbfsPrefetchMetricsEnum metric) {
    long currentSecond = System.currentTimeMillis() / 1000;
    AtomicLong[] buffer = metricBuffers.get(metric);
    long[] timestamps = metricTimestamps.get(metric);

    long sum = 0;
    synchronized (buffer) {
      for (int i = 0; i < windowSize; i++) {
        if (timestamps[i] >= currentSecond - windowSize + 1) {
          sum += buffer[i].get();
        }
      }
    }
    return sum;
  }

  public void startMetricAggregator() {
    scheduler.scheduleAtFixedRate(() -> {
      try {
        long totalReqs = getSumForMetric(AbfsPrefetchMetricsEnum.TOTAL_NUMBER_OF_REQUESTS);
        long totalReadReqs = getSumForMetric(AbfsPrefetchMetricsEnum.TOTAL_NUMBER_OF_READ_REQUESTS);
        long egressThrottled = getSumForMetric(AbfsPrefetchMetricsEnum.EGRESS_THROTTLED);
        long iopsThrottled = getSumForMetric(AbfsPrefetchMetricsEnum.IOPS_THROTTLED);

        double iopsThrottlingRate = (totalReqs == 0) ? 0.0 : (iopsThrottled * 100.0) / totalReqs;
        double egressThrottlingRate = (totalReadReqs == 0) ? 0.0 : (egressThrottled * 100.0) / totalReadReqs;
        //System.out.println("total:"+ total);
        //cachedThrottlingRate.set(rate);

        // Update skipPrefetch based on threshold
        skipPrefetch = (egressThrottlingRate >= throttlingThreshold
            || iopsThrottlingRate >= throttlingThreshold);

      } catch (Exception e) {
        e.printStackTrace(); // use LOG.warn
      }
    }, 0, 1, TimeUnit.SECONDS);
  }

  public boolean shouldSkipPrefetch() {
    return skipPrefetch;
  }

}
