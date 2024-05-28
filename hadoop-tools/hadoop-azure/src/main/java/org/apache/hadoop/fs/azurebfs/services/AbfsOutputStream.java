/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Future;
import java.util.UUID;

import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidIngressServiceException;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.impl.BackReference;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.utils.CachedSASToken;
import org.apache.hadoop.fs.azurebfs.utils.Listener;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.store.DataBlocks;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.Syncable;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.STREAM_ID_LEN;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_WRITE_WITHOUT_LEASE;
import static org.apache.hadoop.fs.impl.StoreImplementationUtils.isProbeForSyncable;
import static org.apache.hadoop.io.IOUtils.wrapException;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters.Mode.APPEND_MODE;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters.Mode.FLUSH_CLOSE_MODE;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters.Mode.FLUSH_MODE;
import static org.apache.hadoop.util.Preconditions.checkState;

/**
 * The BlobFsOutputStream for Rest AbfsClient.
 */
public class AbfsOutputStream extends OutputStream implements Syncable,
    StreamCapabilities, IOStatisticsSource {

  private final AbfsClient client;
  private final String path;
  /** The position in the file being uploaded, where the next block would be
   * uploaded.
   * This is used in constructing the AbfsClient requests to ensure that,
   * even if blocks are uploaded out of order, they are reassembled in
   * correct order.
   * */
  private long position;
  private boolean closed;
  private boolean supportFlush;
  private boolean disableOutputStreamFlush;
  private boolean enableSmallWriteOptimization;
  private boolean isAppendBlob;
  private boolean isExpectHeaderEnabled;
  private volatile IOException lastError;

  private long lastFlushOffset;
  private long lastTotalAppendOffset = 0;

  private final int bufferSize;
  private byte[] buffer;
  private int bufferIndex;
  private int numOfAppendsToServerSinceLastFlush;
  private final int maxConcurrentRequestCount;
  private final int maxRequestsThatCanBeQueued;

  private ConcurrentLinkedDeque<WriteOperation> writeOperations;
  private final ContextEncryptionAdapter contextEncryptionAdapter;

  // SAS tokens can be re-used until they expire
  private CachedSASToken cachedSasToken;
  private final String outputStreamId;
  private final TracingContext tracingContext;
  private Listener listener;

  private AbfsLease lease;
  private String leaseId;

  private final Statistics statistics;
  private final AbfsOutputStreamStatistics outputStreamStatistics;
  private IOStatistics ioStatistics;

  private static final Logger LOG =
      LoggerFactory.getLogger(AbfsOutputStream.class);

  /** Factory for blocks. */
  private final DataBlocks.BlockFactory blockFactory;

  /** Executor service to carry out the parallel upload requests. */
  private final ListeningExecutorService executorService;

  /** The etag of the blob. */
  private String eTag;

  /** ABFS instance to be held by the output stream to avoid GC close. */
  private final BackReference fsBackRef;
  private final AbfsServiceType serviceTypeAtInit;
  private final boolean isDFSToBlobFallbackEnabled;
  private AbfsServiceType currentExecutingServiceType;
  private volatile AzureIngressHandler ingressHandler;

  public AbfsOutputStream(AbfsOutputStreamContext abfsOutputStreamContext)
      throws IOException {
    this.client = abfsOutputStreamContext.getClient();
    this.statistics = abfsOutputStreamContext.getStatistics();
    this.path = abfsOutputStreamContext.getPath();
    this.position = abfsOutputStreamContext.getPosition();
    this.closed = false;
    this.supportFlush = abfsOutputStreamContext.isEnableFlush();
    this.isExpectHeaderEnabled = abfsOutputStreamContext.isExpectHeaderEnabled();
    this.disableOutputStreamFlush = abfsOutputStreamContext
            .isDisableOutputStreamFlush();
    this.enableSmallWriteOptimization
        = abfsOutputStreamContext.isEnableSmallWriteOptimization();
    this.isAppendBlob = abfsOutputStreamContext.isAppendBlob();
    this.lastError = null;
    this.lastFlushOffset = 0;
    this.bufferSize = abfsOutputStreamContext.getWriteBufferSize();
    this.bufferIndex = 0;
    this.numOfAppendsToServerSinceLastFlush = 0;
    this.writeOperations = new ConcurrentLinkedDeque<>();
    this.outputStreamStatistics = abfsOutputStreamContext.getStreamStatistics();
    this.fsBackRef = abfsOutputStreamContext.getFsBackRef();
    this.contextEncryptionAdapter = abfsOutputStreamContext.getEncryptionAdapter();
    this.eTag = abfsOutputStreamContext.getETag();

    if (this.isAppendBlob) {
      this.maxConcurrentRequestCount = 1;
    } else {
      this.maxConcurrentRequestCount = abfsOutputStreamContext
          .getWriteMaxConcurrentRequestCount();
    }
    this.maxRequestsThatCanBeQueued = abfsOutputStreamContext
        .getMaxWriteRequestsToQueue();

    this.lease = abfsOutputStreamContext.getLease();
    this.leaseId = abfsOutputStreamContext.getLeaseId();
    this.executorService =
        MoreExecutors.listeningDecorator(abfsOutputStreamContext.getExecutorService());
    this.cachedSasToken = new CachedSASToken(
        abfsOutputStreamContext.getSasTokenRenewPeriodForStreamsInSeconds());
    this.outputStreamId = createOutputStreamId();
    this.tracingContext = new TracingContext(abfsOutputStreamContext.getTracingContext());
    this.tracingContext.setStreamID(outputStreamId);
    this.tracingContext.setOperation(FSOperationType.WRITE);
    this.ioStatistics = outputStreamStatistics.getIOStatistics();
    this.blockFactory = abfsOutputStreamContext.getBlockFactory();
    this.isDFSToBlobFallbackEnabled = abfsOutputStreamContext.isDFSToBlobFallbackEnabled();
    // create that first block. This guarantees that an open + close sequence
    // writes a 0-byte entry.
    this.serviceTypeAtInit = this.currentExecutingServiceType =
            abfsOutputStreamContext.getIngressServiceType();
    createIngressHandler(serviceTypeAtInit, abfsOutputStreamContext.getBlockFactory(), bufferSize);
    createBlockIfNeeded(position);
  }

  private AzureIngressHandler getIngressHandler() {
    return ingressHandler;
  }

  private synchronized AzureIngressHandler createIngressHandler(AbfsServiceType serviceType,
      DataBlocks.BlockFactory blockFactory, int bufferSize) throws IOException {
    if (ingressHandler != null) {
      return ingressHandler;
    }
    if (serviceType == AbfsServiceType.BLOB) {
      ingressHandler = new AzureBlobIngressHandler(this, blockFactory,
          bufferSize, eTag);
    } else if (isDFSToBlobFallbackEnabled) {
      ingressHandler = new AzureBlobIngressFallbackHandler(this, blockFactory,
          bufferSize, eTag);
    } else {
      ingressHandler = new AzureDFSIngressHandler(this, blockFactory,
          bufferSize);
    }
    return ingressHandler;
  }

  private synchronized void switchHandler() throws IOException {
    // this could be the nth thread reporting switch,
    // switch is done already, return
    if (serviceTypeAtInit != currentExecutingServiceType) {
      return;
    }
    // switch current executing service type
    if (serviceTypeAtInit == AbfsServiceType.BLOB) {
      currentExecutingServiceType = AbfsServiceType.DFS;
    }
    else {
      currentExecutingServiceType = AbfsServiceType.BLOB;
    }
    // switch current ingress handler.
    ingressHandler = createIngressHandler(currentExecutingServiceType, blockFactory, bufferSize);
  }

  private int bufferData(AbfsBlock block, final byte[] data, final int off,
      final int length)
      throws IOException {
    return getIngressHandler().bufferData(block, data, off, length);
  }

  private AbfsRestOperation remoteWrite(AbfsBlock blockToUpload,
      DataBlocks.BlockUploadData uploadData,
      AppendRequestParameters reqParams,
      TracingContext tracingContext)
      throws IOException {
    return getIngressHandler().remoteWrite(blockToUpload, uploadData, reqParams,
        tracingContext);
  }

  private AbfsRestOperation remoteFlush(final long offset,
      final boolean retainUncommitedData,
      final boolean isClose,
      final String leaseId,
      TracingContext tracingContext)
      throws IOException {
    return getIngressHandler().remoteFlush(offset, retainUncommitedData,
        isClose, leaseId, tracingContext);
  }

  private String createOutputStreamId() {
    return StringUtils.right(UUID.randomUUID().toString(), STREAM_ID_LEN);
  }

  /**
   * Query the stream for a specific capability.
   *
   * @param capability string to query the stream support for.
   * @return true for hsync and hflush.
   */
  @Override
  public boolean hasCapability(String capability) {
    return supportFlush && isProbeForSyncable(capability);
  }

  /**
   * Writes the specified byte to this output stream. The general contract for
   * write is that one byte is written to the output stream. The byte to be
   * written is the eight low-order bits of the argument b. The 24 high-order
   * bits of b are ignored.
   *
   * @param byteVal the byteValue to write.
   * @throws IOException if an I/O error occurs. In particular, an IOException may be
   *                     thrown if the output stream has been closed.
   */
  @Override
  public void write(final int byteVal) throws IOException {
    write(new byte[]{(byte) (byteVal & 0xFF)});
  }

  /**
   * Writes length bytes from the specified byte array starting at off to
   * this output stream.
   *
   * @param data   the byte array to write.
   * @param off the start off in the data.
   * @param length the number of bytes to write.
   * @throws IOException if an I/O error occurs. In particular, an IOException may be
   *                     thrown if the output stream has been closed.
   */
  @Override
  public synchronized void write(final byte[] data, final int off, final int length)
      throws IOException {
    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }

    // validate if data is not null and index out of bounds.
    DataBlocks.validateWriteArgs(data, off, length);
    maybeThrowLastError();

    if (off < 0 || length < 0 || length > data.length - off) {
      throw new IndexOutOfBoundsException();
    }

    if (hasLease() && isLeaseFreed()) {
      throw new PathIOException(path, ERR_WRITE_WITHOUT_LEASE);
    }

    AbfsBlock block = createBlockIfNeeded(position);
    int written = bufferData(block, data, off, length);
    int remainingCapacity = block.remainingCapacity();

    if (written < length) {
      // Number of bytes to write is more than the data block capacity,
      // trigger an upload and then write on the next block.
      LOG.debug("writing more data than block capacity -triggering upload");
      uploadCurrentBlock();
      // tail recursion is mildly expensive, but given buffer sizes must be MB.
      // it's unlikely to recurse very deeply.
      this.write(data, off + written, length - written);
    } else {
      if (remainingCapacity == 0) {
        // the whole buffer is done, trigger an upload
        uploadCurrentBlock();
      }
    }
    incrementWriteOps();
  }

  /**
   * Demand create a destination block.
   *
   * @return the active block; null if there isn't one.
   * @throws IOException on any failure to create
   */
  private AbfsBlock createBlockIfNeeded(long position)
      throws IOException {
    return getIngressHandler().getBlockManager()
        .createBlock(getIngressHandler(), position);
  }

  /**
   * Start an asynchronous upload of the current block.
   *
   * @throws IOException Problems opening the destination for upload,
   *                     initializing the upload, or if a previous operation has failed.
   */
  private synchronized void uploadCurrentBlock() throws IOException {
    checkState(getIngressHandler().getBlockManager().hasActiveBlock(),
        "No active block");
    LOG.debug("Writing block # {}",
        getIngressHandler().getBlockManager().getBlockCount());
    try {
      uploadBlockAsync(getIngressHandler().getBlockManager().getActiveBlock(),
          false, false);
    } finally {
      // set the block to null, so the next write will create a new block.
      getIngressHandler().getBlockManager().clearActiveBlock();
    }
  }

  /**
   * Upload a block of data.
   * This will take the block.
   *
   * @param blockToUpload    block to upload.
   * @throws IOException     upload failure
   */
  private void uploadBlockAsync(AbfsBlock blockToUpload,
      boolean isFlush, boolean isClose)
      throws IOException {
    // todo: sneha - if appendblob, only DFS outputstream should be created at layer above.
    // can lease continue to work ?
    // ignore smallwriteoptm when in blob
    if (this.isAppendBlob) {
      ingressHandler.writeAppendBlobCurrentBufferToService();
      return;
    }
    if (!blockToUpload.hasData()) {
      return;
    }
    numOfAppendsToServerSinceLastFlush++;

    final int bytesLength = blockToUpload.dataSize();
    final long offset = position;
    position += bytesLength;
    outputStreamStatistics.bytesToUpload(bytesLength);
    outputStreamStatistics.writeCurrentBuffer();
    DataBlocks.BlockUploadData blockUploadData = blockToUpload.startUpload();
    final Future<Void> job =
        executorService.submit(() -> {
          AbfsPerfTracker tracker =
              client.getAbfsPerfTracker();
          try (AbfsPerfInfo perfInfo = new AbfsPerfInfo(tracker,
              "writeCurrentBufferToService", "append")) {
            AppendRequestParameters.Mode
                mode = APPEND_MODE;
            if (isFlush & isClose) {
              mode = FLUSH_CLOSE_MODE;
            } else if (isFlush) {
              mode = FLUSH_MODE;
            }
            /*
             * Parameters Required for an APPEND call.
             * offset(here) - refers to the position in the file.
             * bytesLength - Data to be uploaded from the block.
             * mode - If it's append, flush or flush_close.
             * leaseId - The AbfsLeaseId for this request.
             */
            AppendRequestParameters reqParams = new AppendRequestParameters(
                offset, 0, bytesLength, mode, false, leaseId, isExpectHeaderEnabled);
            AbfsRestOperation op;
            try {
              op = remoteWrite(blockToUpload, blockUploadData, reqParams,
                  tracingContext);
            } catch (InvalidIngressServiceException ex) {
              switchHandler();
              // retry the operation with switched handler.
              op = remoteWrite(blockToUpload, blockUploadData,
                  reqParams, tracingContext);
            }

            cachedSasToken.update(op.getSasToken());
            perfInfo.registerResult(op.getResult());
            perfInfo.registerSuccess(true);
            outputStreamStatistics.uploadSuccessful(bytesLength);
            return null;
          } finally{
            IOUtils.close(blockUploadData);
          }
        });
    writeOperations.add(new WriteOperation(job, offset, bytesLength));

    // Try to shrink the queue
    shrinkWriteOperationQueue();
  }

  /**
   * A method to set the lastError if an exception is caught.
   * @param ex Exception caught.
   * @throws IOException Throws the lastError.
   */
  void failureWhileSubmit(Exception ex) throws IOException {
    if (ex instanceof AbfsRestOperationException) {
      if (((AbfsRestOperationException) ex).getStatusCode()
          == HttpURLConnection.HTTP_NOT_FOUND) {
        throw new FileNotFoundException(ex.getMessage());
      }
    }
    if (ex instanceof IOException) {
      lastError = (IOException) ex;
    } else {
      lastError = new IOException(ex);
    }
    throw lastError;
  }

  /**
   * Is there an active block and is there any data in it to upload?
   *
   * @return true if there is some data to upload in an active block else false.
   */
  boolean hasActiveBlockDataToUpload() {
    AzureBlockManager blockManager = getIngressHandler().getBlockManager();
    AbfsBlock activeBlock = blockManager.getActiveBlock();
    return blockManager.hasActiveBlock() && activeBlock.hasData();
  }

  /**
   * Increment Write Operations.
   */
  private void incrementWriteOps() {
    if (statistics != null) {
      statistics.incrementWriteOps(1);
    }
  }

  /**
   * Throw the last error recorded if not null.
   * After the stream is closed, this is always set to
   * an exception, so acts as a guard against method invocation once
   * closed.
   * @throws IOException if lastError is set
   */
  private void maybeThrowLastError() throws IOException {
    if (lastError != null) {
      throw lastError;
    }
  }

  /**
   * Flushes this output stream and forces any buffered output bytes to be
   * written out. If any data remains in the payload it is committed to the
   * service. Data is queued for writing and forced out to the service
   * before the call returns.
   */
  @Override
  public void flush() throws IOException {
    if (!disableOutputStreamFlush) {
      flushInternalAsync();
    }
  }

  /** Similar to posix fsync, flush out the data in client's user buffer
   * all the way to the disk device (but the disk may have it in its cache).
   * @throws IOException if error occurs
   */
  @Override
  public void hsync() throws IOException {
    if (supportFlush) {
      flushInternal(false);
    }
  }

  /** Flush out the data in client's user buffer. After the return of
   * this call, new readers will see the data.
   * @throws IOException if any error occurs
   */
  @Override
  public void hflush() throws IOException {
    if (supportFlush) {
      flushInternal(false);
    }
  }

  public String getStreamID() {
    return outputStreamId;
  }

  public void registerListener(Listener listener1) {
    listener = listener1;
    tracingContext.setListener(listener);
  }

  /**
   * Force all data in the output stream to be written to Azure storage.
   * Wait to return until this is complete. Close the access to the stream and
   * shutdown the upload thread pool.
   * If the blob was created, its lease will be released.
   * Any error encountered caught in threads and stored will be rethrown here
   * after cleanup.
   */
  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }

    try {
      // Check if Executor Service got shutdown before the writes could be
      // completed.
      if (hasActiveBlockDataToUpload() && executorService.isShutdown()) {
        throw new PathIOException(path, "Executor Service closed before "
            + "writes could be completed.");
      }
      flushInternal(true);
    } catch (IOException e) {
      // Problems surface in try-with-resources clauses if
      // the exception thrown in a close == the one already thrown
      // -so we wrap any exception with a new one.
      // See HADOOP-16785
      throw wrapException(path, e.getMessage(), e);
    } finally {
      if (contextEncryptionAdapter != null) {
        contextEncryptionAdapter.destroy();
      }
      if (hasLease()) {
        lease.free();
        lease = null;
      }
      lastError = new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
      buffer = null;
      bufferIndex = 0;
      closed = true;
      writeOperations.clear();
      getIngressHandler().getBlockManager().clearActiveBlock();
    }
    LOG.debug("Closing AbfsOutputStream : {}", this);
  }

  private synchronized void flushInternal(boolean isClose) throws IOException {
    maybeThrowLastError();

    // if its a flush post write < buffersize, send flush parameter in append
    if (!isAppendBlob
        && enableSmallWriteOptimization
        && (numOfAppendsToServerSinceLastFlush == 0) // there are no ongoing store writes
        && (writeOperations.size() == 0) // double checking no appends in progress
        && hasActiveBlockDataToUpload()) { // there is
      // some data that is pending to be written
      smallWriteOptimizedflushInternal(isClose);
      return;
    }

    if (hasActiveBlockDataToUpload()) {
      uploadCurrentBlock();
    }
    flushWrittenBytesToService(isClose);
    numOfAppendsToServerSinceLastFlush = 0;
  }

  private synchronized void smallWriteOptimizedflushInternal(boolean isClose) throws IOException {
    // writeCurrentBufferToService will increment numOfAppendsToServerSinceLastFlush
    uploadBlockAsync(getIngressHandler().getBlockManager().getActiveBlock(), true, isClose);
    waitForAppendsToComplete();
    shrinkWriteOperationQueue();
    maybeThrowLastError();
    numOfAppendsToServerSinceLastFlush = 0;
  }

  private synchronized void flushInternalAsync() throws IOException {
    maybeThrowLastError();
    if (hasActiveBlockDataToUpload()) {
      uploadCurrentBlock();
    }
    waitForAppendsToComplete();
    flushWrittenBytesToServiceAsync();
  }

  private synchronized void waitForAppendsToComplete() throws IOException {
    for (WriteOperation writeOperation : writeOperations) {
      try {
        writeOperation.task.get();
      } catch (Exception ex) {
        outputStreamStatistics.uploadFailed(writeOperation.length);
        if (ex.getCause() instanceof AbfsRestOperationException) {
          if (((AbfsRestOperationException) ex.getCause()).getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
            throw new FileNotFoundException(ex.getMessage());
          }
        }

        if (ex.getCause() instanceof AzureBlobFileSystemException) {
          ex = (AzureBlobFileSystemException) ex.getCause();
        }
        lastError = new IOException(ex);
        throw lastError;
      }
    }
  }

  private synchronized void flushWrittenBytesToService(boolean isClose) throws IOException {
    waitForAppendsToComplete();
    flushWrittenBytesToServiceInternal(position, false, isClose);
  }

  private synchronized void flushWrittenBytesToServiceAsync() throws IOException {
    shrinkWriteOperationQueue();

    if (this.lastTotalAppendOffset > this.lastFlushOffset) {
      this.flushWrittenBytesToServiceInternal(this.lastTotalAppendOffset, true,
          false/*Async flush on close not permitted*/);
    }
  }

  private synchronized void flushWrittenBytesToServiceInternal(final long offset,
      final boolean retainUncommitedData, final boolean isClose) throws IOException {
    // flush is called for appendblob only on close
    if (this.isAppendBlob && !isClose) {
      return;
    }
    AbfsPerfTracker tracker = client.getAbfsPerfTracker();
    AbfsRestOperation op;
    try (AbfsPerfInfo perfInfo = new AbfsPerfInfo(tracker,
        "flushWrittenBytesToServiceInternal", "flush")) {
      op = remoteFlush(offset, retainUncommitedData,
          isClose, leaseId, tracingContext);
      if (op != null) {
        cachedSasToken.update(op.getSasToken());
        perfInfo.registerResult(op.getResult()).registerSuccess(true);
      }
    } catch (InvalidIngressServiceException ex) {
      // todo: sneha - new exception type
      switchHandler();
      // retry the operation with switched handler.
      op = remoteFlush(offset, retainUncommitedData,
          isClose, leaseId, tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      if (ex instanceof AbfsRestOperationException) {
        if (((AbfsRestOperationException) ex).getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
          throw new FileNotFoundException(ex.getMessage());
        }
      }
      lastError = new IOException(ex);
      throw lastError;
    }
    this.lastFlushOffset = offset;
  }

  /**
   * Try to remove the completed write operations from the beginning of write
   * operation FIFO queue.
   */
  private synchronized void shrinkWriteOperationQueue() throws IOException {
    try {
      WriteOperation peek = writeOperations.peek();
      while (peek != null && peek.task.isDone()) {
        peek.task.get();
        lastTotalAppendOffset += peek.length;
        writeOperations.remove();
        peek = writeOperations.peek();
        // Incrementing statistics to indicate queue has been shrunk.
        outputStreamStatistics.queueShrunk();
      }
    } catch (Exception e) {
      if (e.getCause() instanceof AzureBlobFileSystemException) {
        lastError = (AzureBlobFileSystemException) e.getCause();
      } else {
        lastError = new IOException(e);
      }
      throw lastError;
    }
  }

  protected static class WriteOperation {
    protected final Future<Void> task;
    protected final long startOffset;
    protected final long length;

    WriteOperation(final Future<Void> task, final long startOffset, final long length) {
      Preconditions.checkNotNull(task, "task");
      Preconditions.checkArgument(startOffset >= 0, "startOffset");
      Preconditions.checkArgument(length >= 0, "length");

      this.task = task;
      this.startOffset = startOffset;
      this.length = length;
    }
  }

  @VisibleForTesting
  public synchronized void waitForPendingUploads() throws IOException {
    waitForAppendsToComplete();
  }

  /**
   * Getter method for AbfsOutputStream statistics.
   *
   * @return statistics for AbfsOutputStream.
   */
  @VisibleForTesting
  public AbfsOutputStreamStatistics getOutputStreamStatistics() {
    return outputStreamStatistics;
  }

  /**
   * Getter to get the size of the task queue.
   *
   * @return the number of writeOperations in AbfsOutputStream.
   */
  @VisibleForTesting
  public int getWriteOperationsSize() {
    return writeOperations.size();
  }

  @VisibleForTesting
  int getMaxConcurrentRequestCount() {
    return this.maxConcurrentRequestCount;
  }

  @VisibleForTesting
  int getMaxRequestsThatCanBeQueued() {
    return maxRequestsThatCanBeQueued;
  }

  @VisibleForTesting
  Boolean isAppendBlobStream() {
    return isAppendBlob;
  }

  @Override
  public IOStatistics getIOStatistics() {
    return ioStatistics;
  }

  @VisibleForTesting
  public boolean isLeaseFreed() {
    if (lease == null) {
      return true;
    }
    return lease.isFreed();
  }

  @VisibleForTesting
  public boolean hasLease() {
    return lease != null;
  }

  /**
   * Appending AbfsOutputStream statistics to base toString().
   *
   * @return String with AbfsOutputStream statistics.
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(super.toString());
    sb.append("AbfsOutputStream@").append(this.hashCode());
    sb.append("){");
    sb.append(outputStreamStatistics.toString());
    sb.append("}");
    return sb.toString();
  }

  @VisibleForTesting
  BackReference getFsBackRef() {
    return fsBackRef;
  }

  @VisibleForTesting
  ListeningExecutorService getExecutorService() {
    return executorService;
  }

  @VisibleForTesting
  AbfsClient getClient() {
    return client;
  }

  public String getPath() {
    return this.path;
  }

  public long getPosition() {
    return position;
  }

  public String getCachedSasTokenString() {
    return cachedSasToken.get();
  }

  public ContextEncryptionAdapter getContextEncryptionAdapter() {
    return contextEncryptionAdapter;
  }

  public AzureBlockManager getBlockManager() {
    return getIngressHandler().getBlockManager();
  }

  public TracingContext getTracingContext() {
    return tracingContext;
  }

  public boolean isDFSToBlobFallbackEnabled() {
    return isDFSToBlobFallbackEnabled;
  }
}
