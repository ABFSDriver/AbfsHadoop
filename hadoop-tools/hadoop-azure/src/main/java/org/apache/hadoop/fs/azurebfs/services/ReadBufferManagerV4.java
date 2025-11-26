package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbfsSharedThreadPoolManager;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadBufferStatus;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.lang.System.currentTimeMillis;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COLON;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HUNDRED_D;
import static org.apache.hadoop.fs.azurebfs.constants.InternalConstants.READ_AHEAD_MEMORY_MONITOR_THREAD_NAME;

public class ReadBufferManagerV4 extends ReadBufferManager {

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
  private static ReadBufferManagerV4 bufferManager;
  private static AtomicBoolean isConfigured = new AtomicBoolean(false);
  
  private static AbfsSharedThreadPoolManager threadPoolManager;
  private static ConcurrentHashMap<String, ReadBuffer> bufferMap = new ConcurrentHashMap<>();
  private final Deque<Integer> freeList = new ConcurrentLinkedDeque<>();
  /**
   * Private constructor to prevent instantiation as this needs to be singleton.
   */
  private ReadBufferManagerV4() {
    printTraceLog("Creating Read Buffer Manager V4 with HADOOP-18546 patch");
  }

  /**
   * Set the ReadBufferManagerV4 configurations based on the provided before singleton initialization.
   * @param readAheadBlockSize the read-ahead block size to set for the ReadBufferManagerV4.
   * @param abfsConfiguration the configuration to set for the ReadBufferManagerV4.
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
          memoryThreshold =
              abfsConfiguration.getReadAheadV2MemoryUsageThresholdPercent()
                  / HUNDRED_D;
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

  static ReadBufferManagerV4 getBufferManager() {
    if (!isConfigured.get()) {
      throw new IllegalStateException("ReadBufferManagerV4 is not configured. "
          + "Please call setReadBufferManagerConfigs() before calling getBufferManager()");
    }
    if (bufferManager == null) {
      LOCK.lock();
      try {
        if (bufferManager == null) {
          bufferManager = new ReadBufferManagerV4();
          bufferManager.init();
          LOGGER.trace("ReadBufferManagerV4 singleton initialized");
        }
      } finally {
        LOCK.unlock();
      }
    }
    return bufferManager;
  }

  /**
   * Initialize the singleton ReadBufferManagerV4.
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
        "ReadBufferManagerV4 initialized with {} buffers",
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
            stream.getPath(), stream.getETag(), requestedOffset, requestedLength,
            stream.getStreamID());
        return;
      }
      if (freeList.isEmpty() && !tryMemoryUpscale() && !tryEvict()) {
        // No buffers are available and more buffers cannot be created. Skip queuing.
        printTraceLog("Skip Queuing ReadAhead for file: {}, with eTag: {}, "
            + "offset: {}, length: {}, triggered by stream: {} as no buffers are available",
            stream.getPath(), stream.getETag(), requestedOffset,
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
                + "offset: {}, length: {}, triggered by stream: {} as no buffers are available",
            stream.getPath(), stream.getETag(), requestedOffset,
            stream.getStreamID());
        return;
      }

      buffer.setBuffer(bufferPool[bufferIndex]);
      buffer.setBufferindex(bufferIndex);

      Callable<Void> readAheadTask = () -> readBufferAsync(buffer);
      threadPoolManager.submitReadTask(generateReadTaskKey(buffer), readAheadTask);
      bufferMap.put(generateReadTaskKey(buffer), buffer);
      printTraceLog("Done Queuing ReadAhead for file: {}, with eTag: {}, "
          + "offset: {}, length: {}, triggered by stream: {} with buffer index: {}",
          stream.getPath(), stream.getETag(), requestedOffset, requestedLength,
          stream.getStreamID(), bufferIndex);
    }
  }

  @Override
  public int getBlock(final AbfsInputStream stream, final long offset, final int length, final byte[] buffer)
      throws IOException {
    printTraceLog("Get Block Requested for file: {} with eTag: {}, "
            + "offset: {}, length: {}, triggered by stream: {}",
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
          "Done Reading from Cache for file: {} with eTag: {}, offset: {}, length: {}, requested by stream: {}",
          stream.getETag(), offset, bytesRead, stream.getStreamID());
      return bytesRead;
    }

    return 0;
  }

  private int getCompletedBlock(final String eTag, final long offset,
      final int length, final byte[] buffer) throws IOException {
    ReadBuffer buf = bufferMap.get(generateReadTaskKey(eTag, offset));

    if (buf == null) {
      return 0;
    }

    buf.startReading(); // atomic increment of refCount.

    if (buf.getStatus() == ReadBufferStatus.READ_FAILED) {
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
      return 0;
    }

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
    ReadBuffer readBuf = null;
    synchronized (this) {
      String key = generateReadTaskKey(eTag, offset);

      if (!isFirstRead && threadPoolManager.removeReadTask(key)) {
        bufferMap.remove(key);
        return;
      }
      readBuf = bufferMap.get(key);
    }
    if (readBuf != null) {
      try {
        printTraceLog(
            "A relevant read buffer for file: {}, with eTag: {}, offset: {}, "
                + "queued by stream: {}, having buffer idx: {} is being prefetched, waiting for latch",
            readBuf.getPath(), readBuf.getETag(), readBuf.getOffset(),
            readBuf.getStream().hashCode(), readBuf.getBufferindex());
        readBuf.getLatch().await();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
      printTraceLog("Latch done for file: {}, with eTag: {}, for offset: {}, "
              + "buffer index: {} queued by stream: {}", readBuf.getPath(),
          readBuf.getETag(),
          readBuf.getOffset(), readBuf.getBufferindex(),
          readBuf.getStream().hashCode());
    }
  }

  public Void readBufferAsync(ReadBuffer buffer) {
    printTraceLog("Async Prefetch Started for file: {} with eTag: {}, "
            + "offset: {}, length: {}, triggered by stream: {}",
        buffer.getPath(), buffer.getETag(), buffer.getOffset(), buffer.getLength(),
        buffer.getStream().getStreamID());
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
    printTraceLog("Async Prefetch Call Done for file: {} with eTag: {}, "
            + "offset: {}, length: {}, triggered by stream: {} with result: {}, bytes read: {}",
        buffer.getPath(), buffer.getETag(), buffer.getOffset(), buffer.getLength(),
        buffer.getStream().getStreamID(), result, bytesActuallyRead);
    if (result == ReadBufferStatus.AVAILABLE && bytesActuallyRead > 0) {
      buffer.setStatus(ReadBufferStatus.AVAILABLE);
      buffer.setLength(bytesActuallyRead);
    } else {
      freeList.push(buffer.getBufferindex());
    }
    // completed list also contains FAILED read buffers
    // for sending exception message to clients.
    buffer.setStatus(result);
    buffer.setTimeStamp(currentTimeMillis());
    buffer.getLatch().countDown();
  }

  private int getFreeIndex() {
    if (freeList.isEmpty()) {
      return -1;
    }
    Integer bufferIndex = freeList.pop();
    if (bufferIndex > bufferPool.length) {
      return -1;
    }
    return bufferIndex;
  }

  private boolean isAlreadyQueued(final String eTag, final long requestedOffset) {
    String readTaskKey = generateReadTaskKey(eTag, requestedOffset);
    ReadBuffer buffer = bufferMap.get(readTaskKey);
    return buffer != null;
  }

  private String generateReadTaskKey(final ReadBuffer buffer) {
    return generateReadTaskKey(buffer.getETag(), buffer.getOffset());
  }

  private String generateReadTaskKey(final String eTag, final long requestedOffset) {
    return eTag + COLON + requestedOffset;
  }

  @Override
  void purgeBuffersForStream(final AbfsInputStream stream) {

  }

  @Override
  int getNumBuffers() {
    return 0;
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

  private synchronized boolean tryMemoryUpscale() {
    // TODO: Implement memory upscale logic.
    return false;
  }

  private void scheduledEviction() {
    for (ReadBuffer buf : bufferMap.values()) {
      if (currentTimeMillis() - buf.getTimeStamp() > getThresholdAgeMilliseconds()) {
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
    if (bufferMap.size() <= 0) {
      return false;  // there are no evict-able buffers
    }

    long currentTimeInMs = currentTimeMillis();

    // first, try buffers where all bytes have been consumed (approximated as first and last bytes consumed)
    for (ReadBuffer buf : bufferMap.values()) {
      if (buf.isFullyConsumed()) {
        nodeToEvict = buf;
        break;
      }
    }
    if (nodeToEvict != null) {
      printTraceLog("Evicting fully consumed buffer with buffer index: {}, file: {}, with eTag: {}, offset: {}, queued by stream: {}",
          nodeToEvict.getBufferindex(), nodeToEvict.getPath(), nodeToEvict.getETag(),
          nodeToEvict.getOffset(), nodeToEvict.getStream().getStreamID());
      return manualEviction(nodeToEvict);
    }

    // next, try buffers where any bytes have been consumed (maybe a bad idea? have to experiment and see)
    for (ReadBuffer buf : bufferMap.values()) {
      if (buf.isAnyByteConsumed()) {
        nodeToEvict = buf;
        break;
      }
    }

    if (nodeToEvict != null) {
      printTraceLog(
          "Evicting partially consumed buffer with buffer index: {}, file: {}, with eTag: {}, offset: {}, queued by stream: {}",
          nodeToEvict.getBufferindex(), nodeToEvict.getPath(),
          nodeToEvict.getETag(),
          nodeToEvict.getOffset(), nodeToEvict.getStream().getStreamID());
      return manualEviction(nodeToEvict);
    }
    
    long earliestBirthday = Long.MAX_VALUE;
    for (ReadBuffer buf : bufferMap.values()) {
      if ((buf.getBufferindex() != -1) && (buf.getTimeStamp() < earliestBirthday)) {
        nodeToEvict = buf;
        earliestBirthday = buf.getTimeStamp();
      }
    }

    if ((currentTimeInMs - earliestBirthday > getThresholdAgeMilliseconds())
        && (nodeToEvict != null)) {
      printTraceLog(
          "Evicting buffer based on age with buffer index: {}, file: {}, with eTag: {}, offset: {}, queued by stream: {}",
          nodeToEvict.getBufferindex(), nodeToEvict.getPath(),
          nodeToEvict.getETag(),
          nodeToEvict.getOffset(), nodeToEvict.getStream().getStreamID());
      return manualEviction(nodeToEvict);
    }

    printTraceLog("No buffer eligible for manual eviction");
    // nothing can be evicted
    return false;
  }

  private boolean manualEviction(final ReadBuffer buf) {
    printTraceLog(
        "Manual Eviction of Buffer Triggered for BufferIndex: {}, file: {}, with eTag: {}, offset: {}, queued by stream: {}",
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
    // As failed ReadBuffers (bufferIndx = -1) are saved in getCompletedReadList(),
    // avoid adding it to availableBufferList.
    if (buf.getBufferindex() != -1) {
      freeList.push(buf.getBufferindex());
    }
    bufferMap.remove(generateReadTaskKey(buf));
    buf.setTracingContext(null);
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
