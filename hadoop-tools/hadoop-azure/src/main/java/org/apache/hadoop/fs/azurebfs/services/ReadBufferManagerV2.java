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

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadBufferStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.classification.VisibleForTesting;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

/**
 * The Improved Read Buffer Manager for Rest AbfsClient.
 */
final class ReadBufferManagerV2 implements ReadBufferManager {
  // Internal constants
  private static final Logger LOGGER = LoggerFactory.getLogger(ReadBufferManagerV2.class);
  private static final ReentrantLock LOCK = new ReentrantLock();

  // Thread Pool Configurations
  private static int minThreadPoolSize;
  private static int maxThreadPoolSize;
  private static int cpuMonitoringIntervalInMilliSec;
  private static int cpuThreshold;
  private static int threadPoolUpscalePercentage;
  private static int threadPoolDownscalePercentage;
  private static int executorServiceKeepAliveTimeInMilliSec;

  private ThreadPoolExecutor workerPool;
  private final List<ReadBufferWorker> workerRefs = new ArrayList<>();

  // Buffer Pool Configurations
  private static int minBufferPoolSize;
  private static int maxBufferPoolSize;
  private static int memoryMonitoringIntervalInMilliSec;
  private static double memoryThreshold;
  private static int thresholdAgeMilliseconds;

  private int numberOfActiveBuffers = 0;
  private byte[][] bufferPool;
  private Stack<Integer> availableBufferList = new Stack<>();

  // Buffer Manager Structures
  private Queue<ReadBuffer> readAheadQueue = new LinkedList<>();
  private LinkedList<ReadBuffer> inProgressList = new LinkedList<>();
  private LinkedList<ReadBuffer> completedReadList = new LinkedList<>();
  private static boolean isDynamicScalingEnabled;
  private static int blockSize = 4 * ONE_MB;


  /**
   * Private constructor to prevent instantiation as this needs to be singleton.
   */
  private ReadBufferManagerV2() {
    LOGGER.trace("Creating readbuffer manager with HADOOP-18546 patch");
  }
  private static ReadBufferManagerV2 bufferManager;

  public static ReadBufferManagerV2 getBufferManager() {
    if (bufferManager == null) {
      LOCK.lock();
      try {
        if (bufferManager == null) {
          bufferManager = new ReadBufferManagerV2();
          bufferManager.init();
        }
      } finally {
        LOCK.unlock();
      }
    }
    return bufferManager;
  }

  /**
   * Set the ReadBufferManagerV2 configurations based on the provided before singleton initialization.
   * @param abfsConfiguration the configuration to set for the ReadBufferManagerV2.
   */
  public static void setReadBufferManagerConfigs(final int readAheadBlockSize,
      final AbfsConfiguration abfsConfiguration) {
    // Set Configs only before initializations.
    if (bufferManager == null) {
      minThreadPoolSize = abfsConfiguration.getMinReadAheadV2ThreadPoolSize();
      maxThreadPoolSize = abfsConfiguration.getMaxReadAheadV2ThreadPoolSize();
      cpuMonitoringIntervalInMilliSec = abfsConfiguration.getReadAheadV2CpuMonitoringIntervalMilliseconds();
      cpuThreshold = abfsConfiguration.getReadAheadV2CpuUsageThresholdPercent();
      threadPoolUpscalePercentage = abfsConfiguration.getReadAheadV2ThreadPoolUpscalePercentage();
      threadPoolDownscalePercentage = abfsConfiguration.getReadAheadV2ThreadPoolDownscalePercentage();
      executorServiceKeepAliveTimeInMilliSec = abfsConfiguration.getReadAheadExecutorServiceTTLInMilliSeconds();

      minBufferPoolSize = abfsConfiguration.getMinReadAheadV2BufferPoolSize();
      maxBufferPoolSize = abfsConfiguration.getMaxReadAheadV2BufferPoolSize();
      memoryMonitoringIntervalInMilliSec = abfsConfiguration.getReadAheadV2MemoryMonitoringIntervalMilliseconds();
      memoryThreshold = abfsConfiguration.getReadAheadV2MemoryUsageThresholdPercent();
      thresholdAgeMilliseconds = abfsConfiguration.getReadAheadV2CachedBufferTTLMilliseconds();
      isDynamicScalingEnabled = abfsConfiguration.isReadAheadV2DynamicScalingEnabled();
      blockSize = readAheadBlockSize;
    }
  }

  /**
   * Initialize the singleton ReadBufferManagerV2.
   */
  private void init() {
    // Initialize Buffer Pool
    bufferPool = new byte[maxBufferPoolSize][];
    for (int i = 0; i < minBufferPoolSize; i++) {
      bufferPool[i] = new byte[blockSize];  // same buffers are reused. The byte array never goes back to GC
      availableBufferList.add(i);
      numberOfActiveBuffers++;
    }
    ScheduledExecutorService memoryMonitorThread
        = Executors.newSingleThreadScheduledExecutor();
    memoryMonitorThread.scheduleAtFixedRate(this::scheduledEviction,
        memoryMonitoringIntervalInMilliSec, memoryMonitoringIntervalInMilliSec, TimeUnit.MILLISECONDS);

    // Initialize a Fixed Size Thread Pool with minThreadPoolSize threads
    workerPool = new ThreadPoolExecutor(minThreadPoolSize, maxThreadPoolSize,
        executorServiceKeepAliveTimeInMilliSec, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    workerPool.allowCoreThreadTimeOut(true);
    for (int i = 0; i < minThreadPoolSize; i++) {
      ReadBufferWorker worker = new ReadBufferWorker(i, getBufferManager());
      workerRefs.add(worker);
      workerPool.submit(worker);
    }
    ReadBufferWorker.UNLEASH_WORKERS.countDown();

    if (isDynamicScalingEnabled) {
      ScheduledExecutorService cpuMonitorThread
          = Executors.newSingleThreadScheduledExecutor();
      cpuMonitorThread.scheduleAtFixedRate(this::adjustThreadPool,
          cpuMonitoringIntervalInMilliSec, cpuMonitoringIntervalInMilliSec,
          TimeUnit.MILLISECONDS);
      LOGGER.debug(
          "ReadBufferManagerV2 initialized with {} buffers and {} worker threads with min {} and max {}",
          numberOfActiveBuffers, workerPool.getCorePoolSize(),
          minThreadPoolSize, maxThreadPoolSize);
    }
  }

  /**
   * {@link AbfsInputStream} calls this method to queueing read-ahead.
   * @param stream which read-ahead is requested from.
   * @param requestedOffset The offset in the file which should be read.
   * @param requestedLength The length to read.
   */
  @Override
  public void queueReadAhead(final AbfsInputStream stream, final long requestedOffset,
      final int requestedLength, TracingContext tracingContext) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Start Queueing readAhead for file: {}, offset: {}, length: {}",
          stream.getPath(), requestedOffset, requestedLength);
    }
    ReadBuffer buffer;
    synchronized (this) {
      if (isAlreadyQueued(stream, requestedOffset)) {
        // Already queued for this offset, so skip queuing.
        return;
      }
      if (availableBufferList.isEmpty() && !tryMemoryUpscale() && !tryEvict()) {
        // No buffers are available and more buffers cannot be created. Skip queuing.
        return;
      }

      // Create a new ReadBuffer to keep the prefetched data and queue.
      buffer = new ReadBuffer();
      buffer.setStream(stream); // To map buffer with stream for closing stream
      buffer.setOffset(requestedOffset);
      buffer.setLength(0);
      buffer.setRequestedLength(requestedLength);
      buffer.setStatus(ReadBufferStatus.NOT_AVAILABLE);
      buffer.setLatch(new CountDownLatch(1));
      buffer.setTracingContext(tracingContext);

      if (availableBufferList.empty()) {
        /*
         * By now there should be at least one buffer available.
         * This is to double sure that after upscaling or eviction,
         * we still have free buffer available. If not, we skip queueing.
         */
        return;
      }
      Integer bufferIndex = availableBufferList.pop();
      buffer.setBuffer(bufferPool[bufferIndex]);
      buffer.setBufferindex(bufferIndex);
      readAheadQueue.add(buffer);
      notifyAll();
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Done q-ing readAhead for file: {}, offset: {}, buffer idx: {}",
            stream.getPath(), requestedOffset, buffer.getBufferindex());
      }
    }
  }

  /**
   * {@link AbfsInputStream} calls this method read any bytes already available in a buffer (thereby saving a
   * remote read). This returns the bytes if the data already exists in buffer. If there is a buffer that is reading
   * the requested offset, then this method blocks until that read completes. If the data is queued in a read-ahead
   * but not picked up by a worker thread yet, then it cancels that read-ahead and reports cache miss. This is because
   * depending on worker thread availability, the read-ahead may take a while - the calling thread can do its own
   * read to get the data faster (compared to the read waiting in queue for an indeterminate amount of time).
   *
   * @param stream of the file to read bytes for
   * @param position the offset in the file to do a read for
   * @param length   the length to read
   * @param buffer   the buffer to read data into. Note that the buffer will be written into from offset 0.
   * @return the number of bytes read
   */
  @Override
  public int getBlock(final AbfsInputStream stream, final long position, final int length, final byte[] buffer)
      throws IOException {
    // not synchronized, so have to be careful with locking
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(
          "getBlock request for file: {}, for position: {} an length: {}, from thread: {} received",
          stream.getPath(), position, length, Thread.currentThread().getName());
    }

    // Wait for any in-progress read to complete.
    waitForProcess(stream, position);

    int bytesRead = 0;
    synchronized (this) {
      bytesRead = getBlockFromCompletedQueue(stream, position, length, buffer);
    }
    if (bytesRead > 0) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace(
            "Done read from Cache for the file: {}, position: {}, length: {}",
            stream.getPath(), position, bytesRead);
      }
      return bytesRead;
    }

    // otherwise, just say we got nothing - calling thread can do its own read
    return 0;
  }

  /**
   * {@link ReadBufferWorker} thread calls this to get the next buffer that it should work on.
   * @return {@link ReadBuffer}
   * @throws InterruptedException if thread is interrupted
   */
  @Override
  public ReadBuffer getNextBlockToRead() throws InterruptedException {
    ReadBuffer buffer = null;
    synchronized (this) {
      // Blocking Call to wait for prefetch to be queued.
      while (readAheadQueue.size() == 0) {
        wait();
      }

      buffer = readAheadQueue.remove();
      notifyAll();
      if (buffer == null) {
        return null;
      }
      buffer.setStatus(ReadBufferStatus.READING_IN_PROGRESS);
      inProgressList.add(buffer);
    }
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("ReadBufferWorker picked file with eTag: {},  for offset: {}",
          buffer, buffer.getOffset());
    }
    return buffer;
  }

  /**
   * {@link ReadBufferWorker} thread calls this method to post completion.   *
   * @param buffer            the buffer whose read was completed
   * @param result            the {@link ReadBufferStatus} after the read operation in the worker thread
   * @param bytesActuallyRead the number of bytes that the worker thread was actually able to read
   */
  @Override
  public void doneReading(final ReadBuffer buffer, final ReadBufferStatus result,
      final int bytesActuallyRead) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("ReadBufferWorker completed prefetch for file with eTag: {}, for offset: {}, with status: {} and bytes read: {}",
          buffer,  buffer.getOffset(), result, bytesActuallyRead);
    }
    synchronized (this) {
      // If this buffer has already been purged during
      // close of InputStream then we don't update the lists.
      if (inProgressList.contains(buffer)) {
        inProgressList.remove(buffer);
        if (result == ReadBufferStatus.AVAILABLE && bytesActuallyRead > 0) {
          // Successful read, so update the buffer status and length
          buffer.setStatus(ReadBufferStatus.AVAILABLE);
          buffer.setLength(bytesActuallyRead);
        } else {
          // Failed read, reuse buffer for next read, this buffer will be
          // evicted later based on eviction policy.
          availableBufferList.push(buffer.getBufferindex());
        }
        // completed list also contains FAILED read buffers
        // for sending exception message to clients.
        buffer.setStatus(result);
        buffer.setTimeStamp(currentTimeMillis());
        completedReadList.add(buffer);
      }
    }

    //outside the synchronized, since anyone receiving a wake-up from the latch must see safe-published results
    buffer.getLatch().countDown(); // wake up waiting threads (if any)
  }

  /**
   * Purging the buffers associated with an {@link AbfsInputStream}
   * from {@link ReadBufferManagerV2} when stream is closed.
   * @param stream input stream.
   */
  public synchronized void purgeBuffersForStream(AbfsInputStream stream) {
    LOGGER.debug("Purging stale buffers for AbfsInputStream {} ", stream);
    readAheadQueue.removeIf(readBuffer -> readBuffer.getStream() == stream);
    purgeList(stream, completedReadList);
  }

  private boolean isAlreadyQueued(final AbfsInputStream stream, final long requestedOffset) {
    // returns true if any part of the buffer is already queued
    return (isInList(readAheadQueue, stream, requestedOffset)
        || isInList(inProgressList, stream, requestedOffset)
        || isInList(completedReadList, stream, requestedOffset));
  }

  private boolean isInList(final Collection<ReadBuffer> list, final AbfsInputStream stream,
      final long requestedOffset) {
    return (getFromList(list, stream, requestedOffset) != null);
  }

  private ReadBuffer getFromList(final Collection<ReadBuffer> list, final AbfsInputStream stream,
      final long requestedOffset) {
    for (ReadBuffer buffer : list) {
      if (buffer.getStream() == stream) {
        if (buffer.getStatus() == ReadBufferStatus.AVAILABLE
            && requestedOffset >= buffer.getOffset()
            && requestedOffset < buffer.getOffset() + buffer.getLength()) {
          return buffer;
        } else if (requestedOffset >= buffer.getOffset()
            && requestedOffset
            < buffer.getOffset() + buffer.getRequestedLength()) {
          return buffer;
        }
      }
    }
    return null;
  }

  /**
   * If any buffer in the completed list can be reclaimed then reclaim it and return the buffer to free list.
   * The objective is to find just one buffer - there is no advantage to evicting more than one.
   * @return whether the eviction succeeded - i.e., were we able to free up one buffer
   */
  private synchronized boolean tryEvict() {
    ReadBuffer nodeToEvict = null;
    if (completedReadList.size() <= 0) {
      return false;  // there are no evict-able buffers
    }

    long currentTimeInMs = currentTimeMillis();

    // first, try buffers where all bytes have been consumed (approximated as first and last bytes consumed)
    for (ReadBuffer buf : completedReadList) {
      if (buf.isFirstByteConsumed() && buf.isLastByteConsumed()) {
        nodeToEvict = buf;
        break;
      }
    }
    if (nodeToEvict != null) {
      return manualEviction(nodeToEvict);
    }

    // next, try buffers where any bytes have been consumed (maybe a bad idea? have to experiment and see)
    for (ReadBuffer buf : completedReadList) {
      if (buf.isAnyByteConsumed()) {
        nodeToEvict = buf;
        break;
      }
    }

    if (nodeToEvict != null) {
      return manualEviction(nodeToEvict);
    }

    // next, try any old nodes that have not been consumed
    // Failed read buffers (with buffer index=-1) that are older than
    // thresholdAge should be cleaned up, but at the same time should not
    // report successful eviction.
    // Queue logic expects that a buffer is freed up for read ahead when
    // eviction is successful, whereas a failed ReadBuffer would have released
    // its buffer when its status was set to READ_FAILED.
    long earliestBirthday = Long.MAX_VALUE;
    ArrayList<ReadBuffer> oldFailedBuffers = new ArrayList<>();
    for (ReadBuffer buf : completedReadList) {
      if ((buf.getBufferindex() != -1)
          && (buf.getTimeStamp() < earliestBirthday)) {
        nodeToEvict = buf;
        earliestBirthday = buf.getTimeStamp();
      } else if ((buf.getBufferindex() == -1)
          && (currentTimeInMs - buf.getTimeStamp()) > thresholdAgeMilliseconds) {
        oldFailedBuffers.add(buf);
      }
    }

    for (ReadBuffer buf : oldFailedBuffers) {
      manualEviction(buf);
    }

    if ((currentTimeInMs - earliestBirthday > thresholdAgeMilliseconds) && (nodeToEvict != null)) {
      return manualEviction(nodeToEvict);
    }

    LOGGER.trace("No buffer eligible for eviction");
    // nothing can be evicted
    return false;
  }

  private boolean evict(final ReadBuffer buf) {
    // As failed ReadBuffers (bufferIndx = -1) are saved in completedReadList,
    // avoid adding it to availableBufferList.
    if (buf.getBufferindex() != -1) {
      availableBufferList.push(buf.getBufferindex());
    }
    completedReadList.remove(buf);
    buf.setTracingContext(null);
    if(LOGGER.isTraceEnabled()) {
      LOGGER.trace(
          "Eviction of Buffer Completed for BufferIndex: {}, file: {}, offset: {}, length: {}",
          buf.getBufferindex(), buf.getStream().getPath(), buf.getOffset(),
          buf.getLength());
    }
    return true;
  }

  private void waitForProcess(final AbfsInputStream stream, final long position) {
    ReadBuffer readBuf;
    synchronized (this) {
      clearFromReadAheadQueue(stream, position);
      readBuf = getFromList(inProgressList, stream, position);
    }
    if (readBuf != null) {         // if in in-progress queue, then block for it
      try {
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("Got a relevant read buffer for file with eTag {}, offset {}, buffer idx {}",
              stream, readBuf.getOffset(), readBuf.getBufferindex());
        }
        readBuf.getLatch().await();  // blocking wait on the caller stream's thread
        // Note on correctness: readBuf gets out of inProgressList only in 1 place: after worker thread
        // is done processing it (in doneReading). There, the latch is set after removing the buffer from
        // inProgressList. So this latch is safe to be outside the synchronized block.
        // Putting it in synchronized would result in a deadlock, since this thread would be holding the lock
        // while waiting, so no one will be able to  change any state. If this becomes more complex in the future,
        // then the latch cane be removed and replaced with wait/notify whenever inProgressList is touched.
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("latch done for file with eTag {} buffer idx {} length {}",
            stream, readBuf.getBufferindex(), readBuf.getLength());
      }
    }
  }

  private void clearFromReadAheadQueue(final AbfsInputStream stream, final long requestedOffset) {
    ReadBuffer buffer = getFromList(readAheadQueue, stream, requestedOffset);
    if (buffer != null) {
      readAheadQueue.remove(buffer);
      notifyAll();   // lock is held in calling method
      availableBufferList.push(buffer.getBufferindex());
    }
  }

  private int getBlockFromCompletedQueue(final AbfsInputStream stream, final long position,
      final int length, final byte[] buffer) throws IOException {
    ReadBuffer buf = getBufferFromCompletedQueue(stream, position);

    if (buf == null) {
      return 0;
    }

    if (buf.getStatus() == ReadBufferStatus.READ_FAILED) {
      // To prevent new read requests to fail due to old read-ahead attempts,
      // return exception only from buffers that failed within last thresholdAgeMilliseconds
      if ((currentTimeMillis() - (buf.getTimeStamp()) < thresholdAgeMilliseconds)) {
        throw buf.getErrException();
      } else {
        return 0;
      }
    }

    if ((buf.getStatus() != ReadBufferStatus.AVAILABLE)
        || (position >= buf.getOffset() + buf.getLength())) {
      return 0;
    }

    int cursor = (int) (position - buf.getOffset());
    int availableLengthInBuffer = buf.getLength() - cursor;
    int lengthToCopy = Math.min(length, availableLengthInBuffer);
    System.arraycopy(buf.getBuffer(), cursor, buffer, 0, lengthToCopy);
    if (cursor == 0) {
      buf.setFirstByteConsumed(true);
    }
    if (cursor + lengthToCopy == buf.getLength()) {
      buf.setLastByteConsumed(true);
    }
    buf.setAnyByteConsumed(true);
    return lengthToCopy;
  }

  private ReadBuffer getBufferFromCompletedQueue(final AbfsInputStream stream, final long requestedOffset) {
    for (ReadBuffer buffer : completedReadList) {
      // Buffer is returned if the requestedOffset is at or above buffer's
      // offset but less than buffer's length or the actual requestedLength
      if (buffer.getStream() == stream
          && (requestedOffset >= buffer.getOffset())
          && ((requestedOffset < buffer.getOffset() + buffer.getLength())
          || (requestedOffset < buffer.getOffset() + buffer.getRequestedLength()))) {
        return buffer;
      }
    }
    return null;
  }

  private boolean tryMemoryUpscale() {
    if (!isDynamicScalingEnabled) {
      return false; // Dynamic scaling is disabled, so no upscaling.
    }
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
        LOGGER.debug("Scheduled Eviction of Buffer Triggered for BufferIndex: {}, file: {}, offset: {}, length: {}",
            buf.getBufferindex(), buf.getStream().getPath(), buf.getOffset(), buf.getLength());
        evict(buf);
      }
    }
  }

  private boolean manualEviction(final ReadBuffer buf) {
    LOGGER.debug("Manual Eviction of Buffer Triggered for BufferIndex: {}, file: {}, offset: {}, length: {}",
        buf.getBufferindex(), buf.getStream().getPath(), buf.getOffset(), buf.getLength());
    return evict(buf);
  }

  private void adjustThreadPool() {
    // To be Added
  }

  /**
   * Similar to System.currentTimeMillis, except implemented with System.nanoTime().
   * System.currentTimeMillis can go backwards when system clock is changed (e.g., with NTP time synchronization),
   * making it unsuitable for measuring time intervals. nanotime is strictly monotonically increasing per CPU core.
   * Note: it is not monotonic across Sockets, and even within a CPU, its only the
   * more recent parts which share a clock across all cores.
   *
   * @return current time in milliseconds
   */
  private long currentTimeMillis() {
    return System.nanoTime() / 1000 / 1000;
  }

  private void purgeList(AbfsInputStream stream, LinkedList<ReadBuffer> list) {
    for (Iterator<ReadBuffer> it = list.iterator(); it.hasNext();) {
      ReadBuffer readBuffer = it.next();
      if (readBuffer.getStream() == stream) {
        it.remove();
        // As failed ReadBuffers (bufferIndex = -1) are already pushed to free
        // list in doneReading method, we will skip adding those here again.
        if (readBuffer.getBufferindex() != -1) {
          availableBufferList.push(readBuffer.getBufferindex());
        }
      }
    }
  }

  /**
   * Test method that can clean up the current state of readAhead buffers and
   * the lists. Will also trigger a fresh init.
   */
  @VisibleForTesting
  @Override
  public void testResetReadBufferManager() {
    synchronized (this) {
      ArrayList<ReadBuffer> completedBuffers = new ArrayList<>();
      for (ReadBuffer buf : completedReadList) {
        if (buf != null) {
          completedBuffers.add(buf);
        }
      }

      for (ReadBuffer buf : completedBuffers) {
        evict(buf);
      }

      readAheadQueue.clear();
      inProgressList.clear();
      completedReadList.clear();
      availableBufferList.clear();
      for (int i = 0; i < maxBufferPoolSize; i++) {
        bufferPool[i] = null;
      }
      bufferPool = null;
      resetBufferManager();
    }
  }

  @VisibleForTesting
  @Override
  public void testResetReadBufferManager(int readAheadBlockSize, int thresholdAgeMilliseconds) {
    setBlockSize(readAheadBlockSize);
    setThresholdAgeMilliseconds(thresholdAgeMilliseconds);
    testResetReadBufferManager();
  }

  @VisibleForTesting
  static void resetBufferManager() {
    bufferManager = null;
  }

  @VisibleForTesting
  static void setBlockSize(int readAheadBlockSize) {
    blockSize = readAheadBlockSize;
  }

  @VisibleForTesting
  public void setThresholdAgeMilliseconds(int thresholdAgeMs) {
    thresholdAgeMilliseconds = thresholdAgeMs;
  }

  @VisibleForTesting
  public int getThresholdAgeMilliseconds() {
    return thresholdAgeMilliseconds;
  }

  @VisibleForTesting
  public int getCompletedReadListSize() {
    return completedReadList.size();
  }

  @VisibleForTesting
  public void callTryEvict() {
    tryEvict();
  }

  @VisibleForTesting
  public void testMimicFullUseAndAddFailedBuffer(ReadBuffer buf) {
    availableBufferList.clear();
    completedReadList.add(buf);
  }

  @VisibleForTesting
  public int getNumBuffers() {
    return numberOfActiveBuffers;
  }

  @VisibleForTesting
  public synchronized List<ReadBuffer> getInProgressCopiedList() {
    return new ArrayList<>(inProgressList);
  }

  @VisibleForTesting
  public synchronized List<ReadBuffer> getCompletedReadListCopy() {
    return new ArrayList<>(completedReadList);
  }

  @VisibleForTesting
  public synchronized List<Integer> getFreeListCopy() {
    return new ArrayList<>(availableBufferList);
  }

  @VisibleForTesting
  public synchronized List<ReadBuffer> getReadAheadQueueCopy() {
    return new ArrayList<>(readAheadQueue);
  }

  @VisibleForTesting
  public int getReadAheadBlockSize() {
    return blockSize;
  }
}
