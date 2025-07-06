/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import com.sun.management.OperatingSystemMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadBufferStatus;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.lang.System.currentTimeMillis;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ZERO;

public class ReadBufferManagerV2 implements ReadBufferManager {

  // Static variables and constants
  private static final Logger LOGGER = LoggerFactory.getLogger(ReadBufferManagerV2.class);
  private static final double INCREMENT_FACTOR = 1.33;
  private static final double DECREMENT_FACTOR = 0.66;
  private static final double DEFAULT_CPU_THRESHOLD = 0.5;
  private static final double DEFAULT_MEMORY_THRESHOLD = 0.5;
  private static final ReentrantLock LOCK = new ReentrantLock();

  // Thread Pool Configurations
  private int minThreadPoolSize;
  private int maxThreadPoolSize;
  private double cpuThreshold;
  private int executorServiceKeepAliveTimeInSec;
  private ScheduledExecutorService cpuMonitor;
  private int cpuMonitorIntervalInSec;
  private ThreadPoolExecutor workerPool;
  private final List<ReadBufferWorker> workerRefs = new ArrayList<>();

  // Buffer Pool Configurations
  private int minBufferPoolSize;
  private int maxBufferPoolSize;
  private double memoryThreshold;
  private static int thresholdAgeMilliseconds;
  private ScheduledExecutorService memoryMonitor;
  private int numberOfActiveBuffers = 0;
  private byte[][] bufferPool;
  private Stack<Integer> availableBufferList = new Stack<>();
  private int blockSize;

  // Buffer Manager Structures
  private Queue<ReadBuffer> readAheadQueue = new LinkedList<>();
  private LinkedList<ReadBuffer> inProgressList = new LinkedList<>();
  private LinkedList<ReadBuffer> completedReadList = new LinkedList<>();

  // Singleton instance creation. Hide instance creation from outside.
  private static ReadBufferManagerV2 bufferManager;
  private ReadBufferManagerV2() {
    LOGGER.trace("Creating ReadBufferManager with HADOOP-18546 patch");
  }

  public static ReadBufferManagerV2 getBufferManager(final AbfsConfiguration abfsConfiguration) {
    if (bufferManager == null) {
      LOCK.lock();
      try {
        if (bufferManager == null) {
          bufferManager = new ReadBufferManagerV2();
          bufferManager.setConfigs(abfsConfiguration);
          bufferManager.init();
        }
      } finally {
        LOCK.unlock();
      }
    }
    return bufferManager;
  }

  @Override
  public void queueReadAhead(final AbfsInputStream stream,
      final long requestedOffset,
      final int requestedLength,
      final TracingContext tracingContext) {

  }

  @Override
  public int getBlock(final AbfsInputStream stream,
      final long position,
      final int length,
      final byte[] buffer) throws IOException {
    return 0;
  }

  @Override
  public void purgeBuffersForStream(final AbfsInputStream stream) {

  }

  @Override
  public ReadBuffer getNextBlockToRead() throws InterruptedException {
    return null;
  }

  @Override
  public void doneReading(final ReadBuffer buffer,
      final ReadBufferStatus result,
      final int bytesActuallyRead) {

  }

  @Override
  public void testResetReadBufferManager() {

  }

  @Override
  public int getThresholdAgeMilliseconds() {
    return 0;
  }

  @Override
  public int getCompletedReadListSize() {
    return 0;
  }

  @Override
  public void callTryEvict() {

  }

  @Override
  public void setThresholdAgeMilliseconds(final int thresholdAgeMs) {

  }

  @Override
  public void testMimicFullUseAndAddFailedBuffer(final ReadBuffer buf) {

  }

  @Override
  public int getNumBuffers() {
    return 0;
  }

  @Override
  public List<ReadBuffer> getInProgressCopiedList() {
    return Collections.emptyList();
  }

  @Override
  public List<ReadBuffer> getReadAheadQueueCopy() {
    return Collections.emptyList();
  }

  @Override
  public List<ReadBuffer> getCompletedReadListCopy() {
    return Collections.emptyList();
  }

  @Override
  public List<Integer> getFreeListCopy() {
    return Collections.emptyList();
  }

  @Override
  public int getReadAheadBlockSize() {
    return 0;
  }

  @Override
  public void testResetReadBufferManager(final int readAheadBlockSize,
      final int thresholdAgeMilliseconds) {

  }

  private void setConfigs(final AbfsConfiguration abfsConfiguration) {
    minThreadPoolSize = abfsConfiguration.getMinReadAheadV2ThreadPoolSize();
    maxThreadPoolSize = abfsConfiguration.getMaxReadAheadV2ThreadPoolSize();
    cpuThreshold = abfsConfiguration.getReadAheadV2CpuUsageThresholdPercent();
    executorServiceKeepAliveTimeInSec = abfsConfiguration.getReadAheadExecutorServiceTTLInMilliSeconds();
    cpuMonitorIntervalInSec = abfsConfiguration.getReadAheadV2CpuMonitoringIntervalMilliseconds();

    minBufferPoolSize = abfsConfiguration.getMinReadAheadV2BufferPoolSize();
    maxBufferPoolSize = abfsConfiguration.getMaxReadAheadV2BufferPoolSize();
    memoryThreshold = abfsConfiguration.getReadAheadV2MemoryUsageThresholdPercent();
    thresholdAgeMilliseconds = abfsConfiguration.getReadAheadV2CachedBufferTTLMilliseconds();
    blockSize = abfsConfiguration.getReadAheadBlockSize();
  }

  private void init() {
    // Initialize Buffer Pool and its monitor thread.
    bufferPool = new byte[maxBufferPoolSize][];
    for (int i = 0; i < minBufferPoolSize; i++) {
      bufferPool[i] = new byte[blockSize];  // same buffers are reused. The byte array never goes back to GC
      availableBufferList.add(i);
      numberOfActiveBuffers++;
    }
    memoryMonitor = Executors.newSingleThreadScheduledExecutor();
    memoryMonitor.scheduleAtFixedRate(this::scheduledEviction,
        thresholdAgeMilliseconds, thresholdAgeMilliseconds, TimeUnit.MILLISECONDS);

    // Initialize Thread Pool and its monitor thread.
    workerPool = new ThreadPoolExecutor(minThreadPoolSize, minThreadPoolSize,
        executorServiceKeepAliveTimeInSec, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
    workerPool.allowCoreThreadTimeOut(true);
    for (int i = 0; i < minThreadPoolSize; i++) {
      ReadBufferWorker worker = new ReadBufferWorker(i, this);
      workerRefs.add(worker);
      workerPool.submit(worker);
    }
    ReadBufferWorker.UNLEASH_WORKERS.countDown();
    cpuMonitor = Executors.newSingleThreadScheduledExecutor();
    cpuMonitor.scheduleAtFixedRate(this::adjustThreadPool, cpuMonitorIntervalInSec,
        cpuMonitorIntervalInSec, TimeUnit.SECONDS);
    LOGGER.debug("ReadBufferManagerV2 initialized with {} buffers and {} worker threads",
        numberOfActiveBuffers, workerPool.getCorePoolSize());cc
  }

  private boolean tryMemoryUpscale() {
    MemoryMXBean osBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memoryUsage = osBean.getHeapMemoryUsage();
    double memoryLoad = (double) memoryUsage.getUsed() / memoryUsage.getMax();
    if (memoryLoad < memoryThreshold && numberOfActiveBuffers < maxBufferPoolSize) {
      // Create and Add more buffers in freeList.
      bufferPool[numberOfActiveBuffers] = new byte[blockSize];
      availableBufferList.add(numberOfActiveBuffers);
      numberOfActiveBuffers++;
      LOGGER.debug("Current Memory Usage: {}. Incrementing buffer pool size by 1 to {}", memoryUsage, numberOfActiveBuffers);
      return true;
    }
    LOGGER.debug("Could not Upscale memory. Total buffers: {} Memory Usage: {}",
        numberOfActiveBuffers, memoryUsage);
    return false;
  }

  private void scheduledEviction() {
    for (ReadBuffer buf : completedReadList) {
      if (currentTimeMillis() - buf.getTimeStamp() > thresholdAgeMilliseconds) {
        // If the buffer is older than thresholdAge, evict it.
        if (evict(buf)) {
          LOGGER.debug("Evicted buffer idx {}; was used for file {} offset {} length {}",
              buf.getBufferindex(), buf.getStream().getPath(), buf.getOffset(), buf.getLength());
        }
      }
    }
  }

  private void adjustThreadPool() {
    OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(
        OperatingSystemMXBean.class);
    double cpuLoad = osBean.getSystemCpuLoad();
    int currentPoolSize = workerPool.getCorePoolSize();
    int newThreadPoolSize = currentPoolSize;
    LOGGER.debug("Current CPU load: {} Current worker pool size: {}", cpuLoad, currentPoolSize);
    if (cpuLoad < cpuThreshold) {
      // Increase worker thread count by increment factor.
      newThreadPoolSize = Math.min((int) (currentPoolSize * INCREMENT_FACTOR), maxThreadPoolSize);
      workerPool.setCorePoolSize(newThreadPoolSize);
      workerPool.setMaximumPoolSize(newThreadPoolSize);
      // Create new Worker Threads
      for (int i = currentPoolSize; i < newThreadPoolSize; i++) {
        ReadBufferWorker worker = new ReadBufferWorker(i, this);
        workerRefs.add(worker);
        workerPool.submit(worker);
      }
      LOGGER.debug("Increased worker pool size from {} to {}", currentPoolSize, newThreadPoolSize);
    } else if (cpuLoad > cpuThreshold) {
      newThreadPoolSize = Math.max((int) (currentPoolSize * DECREMENT_FACTOR), minThreadPoolSize);
      workerPool.setCorePoolSize(newThreadPoolSize);
      workerPool.setMaximumPoolSize(newThreadPoolSize);
      // Signal the extra workers to stop
      for (int i = newThreadPoolSize; i < currentPoolSize; i++) {
        if (workerRefs.size() > 0) {
          ReadBufferWorker worker = workerRefs.remove(workerRefs.size() - 1);
          worker.requestStop();
        }
      }
      LOGGER.debug("Decreased worker pool size from {} to {}", currentPoolSize, newThreadPoolSize);
    } else {
      LOGGER.debug("No change in worker pool size. CPU load: {} Pool size: {}", cpuLoad, currentPoolSize);
    }
  }

  private boolean evict(final ReadBuffer buf) {
    // As failed ReadBuffers (bufferIndx = -1) are saved in completedReadList,
    // avoid adding it to availableList.
    if (buf.getBufferindex() != -1) {
      availableBufferList.push(buf.getBufferindex());
    }
    LOGGER.debug("Evicting buffer idx {}; was used for file {} offset {} length {}",
        buf.getBufferindex(), buf.getStream().getPath(), buf.getOffset(), buf.getLength());
    completedReadList.remove(buf);
    buf.setTracingContext(null);
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Evicting buffer idx {}; was used for file {} offset {} length {}",
          buf.getBufferindex(), buf.getStream().getPath(), buf.getOffset(), buf.getLength());
    }
    return true;
  }
}
