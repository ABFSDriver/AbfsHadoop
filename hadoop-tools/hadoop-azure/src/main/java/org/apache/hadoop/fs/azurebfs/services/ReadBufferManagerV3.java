package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadBufferStatus;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.lang.System.currentTimeMillis;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COLON;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HUNDRED_D;
import static org.apache.hadoop.fs.azurebfs.constants.InternalConstants.READ_AHEAD_MEMORY_MONITOR_THREAD_NAME;

public class ReadBufferManagerV3 extends ReadBufferManager {

  // Internal constants
  private static final ReentrantLock LOCK = new ReentrantLock();

  // Buffer Pool Configurations
  private static int minBufferPoolSize;
  private static int maxBufferPoolSize;
  private static int memoryMonitoringIntervalInMilliSec;
  private static double memoryThreshold;
  private static boolean isDynamicMemoryMonitoringEnabled;

  private final AtomicInteger numberOfActiveBuffers = new AtomicInteger(0);
  private byte[][] bufferPool;
  private final Stack<Integer> removedBufferList = new Stack<>();
  private ScheduledExecutorService memoryMonitorThread;

  // Buffer Manager Structures
  private static ReadBufferManagerV3 bufferManager;
  private static AtomicBoolean isConfigured = new AtomicBoolean(false);
  
  private static AbfsSharedThreadPoolManager threadPoolManager;
  private static final ConcurrentHashMap<String, ReadBuffer> bufferMap = new ConcurrentHashMap<>();
  private final ConcurrentSkipListSet<Integer> freeList = new ConcurrentSkipListSet<>();
  /**
   * Private constructor to prevent instantiation as this needs to be singleton.
   */
  private ReadBufferManagerV3() {
    printTraceLog("Creating Read Buffer Manager V4 with HADOOP-18546 patch");
  }

  /**
   * Set the ReadBufferManagerV3 configurations based on the provided before singleton initialization.
   * @param readAheadBlockSize the read-ahead block size to set for the ReadBufferManagerV3.
   * @param abfsConfiguration the configuration to set for the ReadBufferManagerV3.
   */
  public static void setReadBufferManagerConfigs(final int readAheadBlockSize,
      final AbfsConfiguration abfsConfiguration) {
    // Set Configs only before initializations.
    if (bufferManager == null && !isConfigured.get()) {
      LOCK.lock();
      try {
        if (bufferManager == null && !isConfigured.get()) {
          minBufferPoolSize = abfsConfiguration.getMinReadAheadV2BufferPoolSize();
          maxBufferPoolSize = abfsConfiguration.getMaxReadAheadV2BufferPoolSize();
          memoryMonitoringIntervalInMilliSec
              = abfsConfiguration.getReadAheadV2MemoryMonitoringIntervalMillis();
          memoryThreshold = abfsConfiguration.getReadAheadV2MemoryUsageThresholdPercent();
          isDynamicMemoryMonitoringEnabled = abfsConfiguration.isReadAheadV2DynamicScalingEnabled();
          threadPoolManager = AbfsSharedThreadPoolManager.getInstance(abfsConfiguration);
          setThresholdAgeMilliseconds(abfsConfiguration.getReadAheadV2CachedBufferTTLMillis());
          setReadAheadBlockSize(readAheadBlockSize);
          setIsConfigured(true);
        }
      } finally {
        LOCK.unlock();
      }
    }
  }

  static ReadBufferManagerV3 getBufferManager() {
    if (!isConfigured.get()) {
      throw new IllegalStateException("ReadBufferManagerV3 is not configured. "
          + "Please call setReadBufferManagerConfigs() before calling getBufferManager()");
    }
    if (bufferManager == null) {
      LOCK.lock();
      try {
        if (bufferManager == null) {
          bufferManager = new ReadBufferManagerV3();
          bufferManager.init();
          LOGGER.trace("ReadBufferManagerV3 singleton initialized");
        }
      } finally {
        LOCK.unlock();
      }
    }
    return bufferManager;
  }

  /**
   * Initialize the singleton ReadBufferManagerV3.
   */
  @Override
  void init() {
    // Initialize Buffer Pool. Size can never be more than max pool size
    bufferPool = new byte[maxBufferPoolSize][];
    for (int i = 0; i < minBufferPoolSize; i++) {
      // Start with just minimum number of buffers.
      bufferPool[i] = new byte[getReadAheadBlockSize()];
      freeList.add(i);
      numberOfActiveBuffers.getAndIncrement();
    }

    if (isDynamicMemoryMonitoringEnabled) {
      printTraceLog("Starting ReadAhead Memory Monitor Thread with interval: {} ms",
          memoryMonitoringIntervalInMilliSec);
      memoryMonitorThread = Executors.newSingleThreadScheduledExecutor(
          runnable -> {
            Thread t = new Thread(runnable, READ_AHEAD_MEMORY_MONITOR_THREAD_NAME);
            t.setDaemon(true);
            return t;
          });
      memoryMonitorThread.scheduleAtFixedRate(this::scheduledEviction,
          memoryMonitoringIntervalInMilliSec,
          memoryMonitoringIntervalInMilliSec, TimeUnit.MILLISECONDS);
    }

    printTraceLog(
        "ReadBufferManagerV3 initialized with {} buffers",
        numberOfActiveBuffers.get());
  }

  @Override
  public void queueReadAhead(final AbfsInputStream stream,
      final long requestedOffset,
      final int requestedLength,
      TracingContext tracingContext) {
    printTraceLog("Start Queueing ReadAhead for file: {}, with eTag: {}, "
            + "offset: {}, length: {}, triggered by stream: {}",
        stream.getPath(), stream.getETag(), requestedOffset, requestedLength,
        stream.getStreamID());
    ReadBuffer buffer;
    synchronized (this) {
      if (isAlreadyQueued(stream.getETag(), requestedOffset)) {
        // Already queued for this offset, so skip queuing.
        printTraceLog("Skip Queuing ReadAhead for file: {}, with eTag: {}, "
                + "offset: {}, length: {}, triggered by stream: {}, as it is already queued",
            stream.getPath(), stream.getETag(), requestedOffset,
            requestedLength,
            stream.getStreamID());
        return;
      }
      if (freeList.isEmpty() && !tryMemoryUpscale() && !tryEvict()) {
        // No buffers are available and more buffers cannot be created. Skip queuing.
        printTraceLog("Skip Queuing ReadAhead for file: {}, with eTag: {}, "
                + "offset: {}, length: {}, triggered by stream: {} as no buffers are available",
            stream.getPath(), stream.getETag(), requestedOffset,
            requestedLength,
            stream.getStreamID());
        return;
      }

      // Create a new ReadBuffer to keep the prefetched data and queue.
      buffer = new ReadBuffer();
      buffer.setStream(stream);
      buffer.setETag(stream.getETag());
      buffer.setPath(stream.getPath());
      buffer.setOffset(requestedOffset);
      buffer.setLength(0);
      buffer.setRequestedLength(requestedLength);
      buffer.setStatus(ReadBufferStatus.NOT_AVAILABLE);
      buffer.setLatch(new CountDownLatch(1));
      buffer.setTracingContext(tracingContext);
      int bufferIndex = getFreeIndex();
      if (bufferIndex == -1) {
        // This should never happen as we have already checked for free buffers.
        printTraceLog("Skip Queuing ReadAhead for file: {}, with eTag: {}, "
                + "offset: {}, length: {}, triggered by stream: {} as no valid buffer index found",
            stream.getPath(), stream.getETag(), requestedOffset,
            requestedLength,
            stream.getStreamID());
        return;
      }

      buffer.setBuffer(bufferPool[bufferIndex]);
      buffer.setBufferindex(bufferIndex);
      bufferMap.put(generateReadTaskKey(buffer), buffer);
      printTraceLog("Done Queuing ReadAhead for file: {}, with eTag: {}, "
              + "offset: {}, length: {}, triggered by stream: {} with buffer index: {}",
          stream.getPath(), stream.getETag(), requestedOffset, requestedLength,
          stream.getStreamID(), bufferIndex);
    }
    Callable<Void> readAheadTask = () -> readBufferAsync(buffer);
    threadPoolManager.submitReadTask(generateReadTaskKey(buffer), readAheadTask);
  }

  @Override
  public int getBlock(final AbfsInputStream stream, final long offset, final int length, final byte[] buffer)
      throws IOException {
    printTraceLog("Get Block Requested for file: {} with eTag: {}, "
            + "offset: {}, length: {}, by stream: {}",
        stream.getPath(), stream.getETag(), offset, length,
        stream.getStreamID());

    // Wait for In-Progress Read Ahead if any.
    waitForProcess(stream.getETag(), offset, stream.isFirstRead());

    int bytesRead = 0;
    synchronized (this) {
      bytesRead = getCompletedBlock(stream.getETag(), offset, length, buffer);
    }
    if (bytesRead > 0) {
      printTraceLog(
          "Done Reading from Cache for file: {} with eTag: {}, "
              + "offset: {}, length: {}, by stream: {}, bytesRead: {}",
          stream.getPath(), stream.getETag(), offset, length,
          stream.getStreamID(), bytesRead);
      return bytesRead;
    }

    return 0;
  }

  private int getCompletedBlock(final String eTag, final long offset,
      final int length, final byte[] buffer) throws IOException {
    ReadBuffer buf = getFromList(eTag, offset);

    if (buf == null) {
      printTraceLog("No Buffer Found for requested eTag: {} and offset: {}",
          eTag, offset);
      return 0;
    }
    printTraceLog("Buffer Found for requested eTag: {} and offset: {}",
        eTag, offset);

    buf.startReading(); // atomic increment of refCount.

    if (buf.getStatus() == ReadBufferStatus.READ_FAILED) {
      printTraceLog("Found Buffer for file: {} with eTag: {}, offset: {} in READ_FAILED state",
          buf.getPath(), eTag, offset);
      // To prevent new read requests to fail due to old read-ahead attempts,
      // return exception only from buffers that failed within last getThresholdAgeMilliseconds()
      if ((currentTimeMillis() - (buf.getTimeStamp())
          < getThresholdAgeMilliseconds())) {
        throw buf.getErrException();
      } else {
        return 0;
      }
    }

    if ((buf.getStatus() != ReadBufferStatus.AVAILABLE)
        || (offset >= buf.getOffset() + buf.getLength())) {
      printTraceLog("Found Buffer for file: {} with eTag: {}, offset: {} in invalid state",
          buf.getPath(), eTag, offset);
      return 0;
    }

    printTraceLog("Buffer Found for requested eTag: {} and offset: {} is AVAILABLE and valid",
        eTag, offset);

    int cursor = (int) (offset - buf.getOffset());
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

    buf.endReading(); // atomic decrement of refCount
    return lengthToCopy;
  }

  private void waitForProcess(final String eTag, final long offset, boolean isFirstRead) {
    ReadBuffer readBuf;
    synchronized (this) {
      readBuf = getFromList(eTag, offset);
      if (readBuf == null) {
        printTraceLog("No ReadAhead Queued for file with eTag: {}, offset: {}",
            eTag, offset);
        return;
      }

      if (readBuf.getStatus() == ReadBufferStatus.AVAILABLE) {
        printTraceLog(
            "ReadAhead is already in AVAILABLE state for file: {} with eTag: {}, offset: {} ",
            readBuf.getPath(), eTag, offset);
        return;
      }

      if (readBuf.getStatus() == ReadBufferStatus.NOT_AVAILABLE) {
        printTraceLog(
            "ReadAhead is in NOT_AVAILABLE state for file: {} with eTag: {}, offset: {}. Attempt to Cancel the task",
            readBuf.getPath(), eTag, offset);
        boolean isCancelled = threadPoolManager.tryCancelReadTask(generateReadTaskKey(eTag, offset));
        if (isCancelled) {
          printTraceLog(
              "ReadAhead task cancelled successfully for file: {} with eTag: {}, offset: {}. "
                  + "Evicting the buffer",
              readBuf.getPath(), eTag, offset);
          // If the read task is cancelled successfully, evict the buffer.
          bufferMap.remove(generateReadTaskKey(eTag, offset));
          freeList.add(readBuf.getBufferindex());
          threadPoolManager.evictReadTask(generateReadTaskKey(eTag, offset));
          return;
        } else {
          printTraceLog(
              "ReadAhead task could not be cancelled for file: {} with eTag: {}, offset: {}. "
                  + "Waiting for the read to complete",
              readBuf.getPath(), eTag, offset);
        }
      }
    }
    waitForLatchIfNeeded(readBuf);
  }

  private void waitForLatchIfNeeded(ReadBuffer readBuffer) {
    if (readBuffer == null) {
      return;
    }
    try {
      printTraceLog("Waiting for Latch to complete for file: {} with eTag: {}, offset: {} ",
          readBuffer.getPath(), readBuffer.getETag(), readBuffer.getOffset());
      readBuffer.getLatch().await();
      printTraceLog("Wait Complete for Latch to complete for file: {} with eTag: {}, offset: {} ",
          readBuffer.getPath(), readBuffer.getETag(), readBuffer.getOffset());
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  public Void readBufferAsync(ReadBuffer buffer) {
    printTraceLog("Async Prefetch Started for file: {} with eTag: {}, "
            + "offset: {}, length: {}, triggered by stream: {}",
        buffer.getPath(), buffer.getETag(), buffer.getOffset(), buffer.getLength(),
        buffer.getStream().getStreamID());
    buffer.setStatus(ReadBufferStatus.READING_IN_PROGRESS);
    try {
      // do the actual read, from the file.
      int bytesRead = buffer.getStream().readRemote(
          buffer.getOffset(),
          buffer.getBuffer(),
          0,
          // If AbfsInputStream was created with bigger buffer size than
          // read-ahead buffer size, make sure a valid length is passed
          // for remote read
          Math.min(buffer.getRequestedLength(), buffer.getBuffer().length),
          buffer.getTracingContext());
      doneReading(buffer, ReadBufferStatus.AVAILABLE, bytesRead);
    } catch (IOException ex) {
      buffer.setErrException(ex);
      doneReading(buffer, ReadBufferStatus.READ_FAILED, 0);
    } catch (Exception ex) {
      buffer.setErrException(
          new PathIOException(buffer.getStream().getPath(), ex));
      doneReading(buffer, ReadBufferStatus.READ_FAILED, 0);
    }
    printTraceLog("Async Prefetch Finished for file: {} with eTag: {}, "
            + "offset: {}, length: {}, triggered by stream: {}",
        buffer.getPath(), buffer.getETag(), buffer.getOffset(), buffer.getLength(),
        buffer.getStream().getStreamID());
    return null;
  }

  @Override
  public void doneReading(final ReadBuffer buffer,
      final ReadBufferStatus result,
      final int bytesActuallyRead) {
    printTraceLog("Done Reading for file: {} with eTag: {}, "
            + "offset: {}, length: {}, triggered by stream: {} with result: {}, bytes read: {}",
        buffer.getPath(), buffer.getETag(), buffer.getOffset(), buffer.getLength(),
        buffer.getStream().getStreamID(), result, bytesActuallyRead);
    if (result == ReadBufferStatus.AVAILABLE && bytesActuallyRead > 0) {
      buffer.setLength(bytesActuallyRead);
    } else {
      freeList.add(buffer.getBufferindex());
    }
    // completed list also contains FAILED read buffers
    // for sending exception message to clients.
    buffer.setTimeStamp(currentTimeMillis());
    buffer.setStatus(result);
    buffer.getLatch().countDown(); // wake up waiting threads (if any)
    printTraceLog("Latch Counted Down for file: {} with eTag: {}, "
            + "offset: {}, length: {}, triggered by stream: {}",
        buffer.getPath(), buffer.getETag(), buffer.getOffset(), buffer.getLength(),
        buffer.getStream().getStreamID());
  }

  private int getFreeIndex() {
    if (freeList.isEmpty()) {
      return -1;
    }
    Integer bufferIndex = freeList.pollFirst();
    if (bufferIndex == null || bufferIndex > bufferPool.length) {
      return -1;
    }
    return bufferIndex;
  }

  /**
   * Checks if the requested offset is already queued in any of the lists:
   * @param eTag of the file associated with the read request
   * @param requestedOffset the offset in the stream to check
   * @return true if the requested offset is already queued in any of the lists,
   */
  private boolean isAlreadyQueued(final String eTag, final long requestedOffset) {
    // returns true if any part of the buffer is already queued
    return isInList(eTag, requestedOffset);
  }

  /**
   * Check if any buffer in the list contains the requested offset.
   * @param eTag the eTag of the file
   * @param requestedOffset the requested offset
   * @return whether any buffer in the list contains the requested offset
   */
  private boolean isInList(final String eTag,
      final long requestedOffset) {
    return (getFromList(eTag, requestedOffset) != null);
  }

  /**
   * Get the buffer from the list that contains the requested offset.
   * @param eTag the eTag of the file
   * @param requestedOffset the requested offset
   * @return the buffer if found, null otherwise
   */
  private ReadBuffer getFromList(final String eTag,
      final long requestedOffset) {
    for (Map.Entry<String, ReadBuffer> entry: bufferMap.entrySet()) {
      ReadBuffer buffer = bufferMap.get(entry.getKey());
      if (buffer != null && eTag.equals(buffer.getETag())) {
        if (buffer.getStatus() == ReadBufferStatus.AVAILABLE
            && requestedOffset >= buffer.getOffset()
            && requestedOffset < buffer.getOffset() + buffer.getLength()) {
          return buffer;
        } else if (requestedOffset >= buffer.getOffset()
            && requestedOffset < buffer.getOffset() + buffer.getRequestedLength()) {
          return buffer;
        }
      }
    }
    return null;
  }



  private String generateReadTaskKey(final ReadBuffer buffer) {
    return generateReadTaskKey(buffer.getETag(), buffer.getOffset());
  }

  private String generateReadTaskKey(final String eTag, final long requestedOffset) {
    return eTag + COLON + requestedOffset;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void purgeBuffersForStream(AbfsInputStream stream) {
    LOGGER.debug("Purging stale buffers for AbfsInputStream {} ", stream);
    for (Map.Entry<String, ReadBuffer> entry : bufferMap.entrySet()) {
      ReadBuffer readBuffer = entry.getValue();
      if (readBuffer.getStream() == stream) {
        bufferMap.remove(entry.getKey());
        threadPoolManager.evictReadTask(entry.getKey());
        // As failed ReadBuffers (bufferIndex = -1) are already pushed to free
        // list in doneReading method, we will skip adding those here again.
        if (readBuffer.getBufferindex() != -1) {
          freeList.add(readBuffer.getBufferindex());
        }
      }
    }
  }

  @Override
  int getNumBuffers() {
    return numberOfActiveBuffers.get();
  }

  @Override
  void callTryEvict() {

  }

  @Override
  void testResetReadBufferManager() {

  }

  @Override
  void testResetReadBufferManager(final int readAheadBlockSize,
      final int thresholdAgeMilliseconds) {

  }

  @Override
  void resetBufferManager() {

  }

  @Override
  public ReadBuffer getNextBlockToRead() throws InterruptedException {
    // This method is not used in V4 as read is directly submitted to thread pool.
    return null;
  }



  /**
   * Try to upscale memory by adding more buffers to the pool if memory usage is below threshold.
   * @return whether the upscale succeeded
   */
  private synchronized boolean tryMemoryUpscale() {
    if (!isDynamicMemoryMonitoringEnabled) {
      printTraceLog("Dynamic scaling is disabled, skipping memory upscale");
      return false; // Dynamic scaling is disabled, so no upscaling.
    }
    double memoryLoad = getMemoryLoad() * HUNDRED_D;
    printTraceLog("Current Memory Load: {}. Threshold: {}. Current Buffers: {}. Max Buffers: {}",
        memoryLoad, memoryThreshold, getNumBuffers(), maxBufferPoolSize);
    if (memoryLoad < memoryThreshold && getNumBuffers() < maxBufferPoolSize) {
      // Create and Add more buffers in getFreeList().
      int nextIndex = getNumBuffers();
      if (nextIndex >= bufferPool.length) {
        printTraceLog("Buffer Pool is already at max capacity: {} buffers",
            bufferPool.length);
        return false;
      }
      bufferPool[nextIndex] = new byte[getReadAheadBlockSize()];
      freeList.add(nextIndex);
      numberOfActiveBuffers.getAndIncrement();
      printTraceLog(
          "Current Memory Load: {}. Incrementing buffer pool size to {}",
          memoryLoad, getNumBuffers());
      return true;
    }
    printTraceLog("Could not Upscale memory. Total buffers: {} Memory Load: {}",
        getNumBuffers(), memoryLoad);
    return false;
  }

  /**
   * Get the current memory load of the JVM.
   * @return the memory load as a double value between 0.0 and 1.0
   */
  public static double getMemoryLoad() {
    MemoryMXBean osBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memoryUsage = osBean.getHeapMemoryUsage();
    return (double) memoryUsage.getUsed() / memoryUsage.getMax();
  }

  private void scheduledEviction() {
    for (ReadBuffer buf : bufferMap.values()) {
      if (isCompletedBuffer(buf)
          && currentTimeMillis() - buf.getTimeStamp() > getThresholdAgeMilliseconds()) {
        // If the buffer is older than thresholdAge, evict it.
        printTraceLog(
            "Scheduled Eviction of Buffer Triggered for BufferIndex: {}, "
                + "file: {}, with eTag: {}, offset: {}, length: {}, queued by stream: {}",
            buf.getBufferindex(), buf.getPath(), buf.getETag(), buf.getOffset(),
            buf.getLength(), buf.getStream().hashCode());
        evict(buf);
      }
    }

    // TODO: Dynamic Memory downscale logic can be implemented here.
  }
  
  private synchronized boolean tryEvict() {
    ReadBuffer nodeToEvict = null;
    if (bufferMap.isEmpty()) {
      printTraceLog("No buffers to evict");
      return false;  // there are no evict-able buffers
    }

    long currentTimeInMs = currentTimeMillis();

    // first, try buffers where all bytes have been consumed (approximated as first and last bytes consumed)
    for (ReadBuffer buf : bufferMap.values()) {
      if (isCompletedBuffer(buf) && buf.isFullyConsumed()) {
        nodeToEvict = buf;
        break;
      }
    }
    if (nodeToEvict != null) {
      printTraceLog("Evicting fully consumed buffer with buffer index: {}, file: {}, with eTag: {}, offset: {}, triggered by stream: {}",
          nodeToEvict.getBufferindex(), nodeToEvict.getPath(), nodeToEvict.getETag(),
          nodeToEvict.getOffset(), nodeToEvict.getStream().getStreamID());
      return manualEviction(nodeToEvict);
    }

    // next, try buffers where any bytes have been consumed (maybe a bad idea? have to experiment and see)
    for (ReadBuffer buf : bufferMap.values()) {
      if (isCompletedBuffer(buf) && buf.isAnyByteConsumed()) {
        nodeToEvict = buf;
        break;
      }
    }

    if (nodeToEvict != null) {
      printTraceLog(
          "Evicting partially consumed buffer with buffer index: {}, file: {}, with eTag: {}, offset: {}, triggered by stream: {}",
          nodeToEvict.getBufferindex(), nodeToEvict.getPath(),
          nodeToEvict.getETag(),
          nodeToEvict.getOffset(), nodeToEvict.getStream().getStreamID());
      return manualEviction(nodeToEvict);
    }
    
    long earliestBirthday = Long.MAX_VALUE;
    for (ReadBuffer buf : bufferMap.values()) {
      if (isCompletedBuffer(buf) && (buf.getBufferindex() != -1) && (buf.getTimeStamp() < earliestBirthday)) {
        nodeToEvict = buf;
        earliestBirthday = buf.getTimeStamp();
      }
    }

    if ((currentTimeInMs - earliestBirthday > getThresholdAgeMilliseconds())
        && (nodeToEvict != null)) {
      printTraceLog(
          "Evicting buffer based on age with buffer index: {}, file: {}, with eTag: {}, offset: {}, triggered by stream: {}",
          nodeToEvict.getBufferindex(), nodeToEvict.getPath(),
          nodeToEvict.getETag(),
          nodeToEvict.getOffset(), nodeToEvict.getStream().getStreamID());
      return manualEviction(nodeToEvict);
    }

    printTraceLog("No buffer eligible for manual eviction");
    // nothing can be evicted
    return false;
  }

  private boolean isCompletedBuffer(final ReadBuffer buf) {
    return (buf.getStatus() == ReadBufferStatus.AVAILABLE
        || buf.getStatus() == ReadBufferStatus.READ_FAILED);
  }

  private boolean manualEviction(final ReadBuffer buf) {
    printTraceLog(
        "Manual Eviction of Buffer Triggered for BufferIndex: {}, file: {}, with eTag: {}, offset: {}, triggered by stream: {}",
        buf.getBufferindex(), buf.getPath(), buf.getETag(), buf.getOffset(),
        buf.getStream().getStreamID());
    return evict(buf);
  }

  private boolean evict(final ReadBuffer buf) {
    if (buf.getRefCount() > 0) {
      // If the buffer is still being read, then we cannot evict it.
      printTraceLog(
          "Cannot evict buffer with index: {}, file: {}, with eTag: {}, offset: {} as it is still being read by some input stream",
          buf.getBufferindex(), buf.getPath(), buf.getETag(), buf.getOffset());
      return false;
    }
    // As failed ReadBuffers (bufferIndx = -1) are saved in bufferMap,
    // avoid adding it to availableBufferList.
    if (buf.getBufferindex() != -1) {
      freeList.add(buf.getBufferindex());
    }
    bufferMap.remove(generateReadTaskKey(buf));
    buf.setTracingContext(null);
    threadPoolManager.evictReadTask(generateReadTaskKey(buf));
    printTraceLog(
        "Eviction of Buffer Completed for BufferIndex: {}, file: {}, with eTag: {}, offset: {}, is fully consumed: {}, is partially consumed: {}",
        buf.getBufferindex(), buf.getPath(), buf.getETag(), buf.getOffset(),
        buf.isFullyConsumed(), buf.isAnyByteConsumed());
    return true;
  }
  
  private void printTraceLog(String message, Object... args) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(message, args);
    }
  }

  private static void setIsConfigured(boolean configured) {
    isConfigured.set(configured);
  }
}
