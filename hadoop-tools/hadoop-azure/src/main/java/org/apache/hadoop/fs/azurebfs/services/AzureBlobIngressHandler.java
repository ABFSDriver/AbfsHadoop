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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.store.DataBlocks;
import org.apache.hadoop.io.IOUtils;

/**
 * The BlobFsOutputStream for Rest AbfsClient.
 */
public class AzureBlobIngressHandler extends AzureIngressHandler {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsOutputStream.class);

  private String eTag;

  private final Lock lock = new ReentrantLock();

  private final AzureBlobBlockManager blobBlockManager;

  private final AbfsBlobClient blobClient;

  private final AbfsClientHandler clientHandler;

  /**
   * Constructs an AzureBlobIngressHandler.
   *
   * @param abfsOutputStream the AbfsOutputStream.
   * @param blockFactory     the block factory.
   * @param bufferSize       the buffer size.
   * @param eTag             the eTag.
   * @throws AzureBlobFileSystemException if an error occurs.
   */
  public AzureBlobIngressHandler(AbfsOutputStream abfsOutputStream,
      DataBlocks.BlockFactory blockFactory,
      int bufferSize, String eTag, AbfsClientHandler clientHandler)
      throws AzureBlobFileSystemException {
    super(abfsOutputStream);
    this.eTag = eTag;
    this.blobBlockManager = new AzureBlobBlockManager(this.abfsOutputStream,
        blockFactory, bufferSize);
    this.clientHandler = clientHandler;
    this.blobClient = clientHandler.getBlobClient();
    LOG.trace("Created a new BlobIngress Handler for AbfsOutputStream instance {} for path {}",
        abfsOutputStream.getStreamID(), abfsOutputStream.getPath());
  }

  /**
   * Buffers data into the specified block.
   *
   * @param block the block to buffer data into.
   * @param data  the data to be buffered.
   * @param off   the start offset in the data.
   * @param length the number of bytes to buffer.
   * @return the number of bytes buffered.
   * @throws IOException if an I/O error occurs.
   */
  @Override
  protected synchronized int bufferData(AbfsBlock block,
      final byte[] data,
      final int off,
      final int length)
      throws IOException {
    AbfsBlobBlock blobBlock = (AbfsBlobBlock) block;
    blobBlockManager.trackBlockWithData(blobBlock);
    LOG.trace("Buffering data of length {} to block at offset {}", length, off);
    return blobBlock.write(data, off, length);
  }

  /**
   * Performs a remote write operation.
   *
   * @param blockToUpload the block to upload.
   * @param uploadData    the data to upload.
   * @param reqParams     the request parameters.
   * @param tracingContext the tracing context.
   * @return the resulting AbfsRestOperation.
   * @throws IOException if an I/O error occurs.
   */
  @Override
  protected AbfsRestOperation remoteWrite(AbfsBlock blockToUpload,
      DataBlocks.BlockUploadData uploadData,
      AppendRequestParameters reqParams,
      TracingContext tracingContext)
      throws IOException {
    AbfsBlobBlock blobBlockToUpload = (AbfsBlobBlock) blockToUpload;
    reqParams.setBlockId(blobBlockToUpload.getBlockId());
    reqParams.setEtag(getETag());
    AbfsRestOperation op;
    TracingContext tracingContextAppend = new TracingContext(tracingContext);
    tracingContextAppend.setIngressHandler("BAppend");
    tracingContextAppend.setPosition(String.valueOf(blobBlockToUpload.getOffset()));
    try {
      LOG.trace("Starting remote write for block with ID {} and offset {}",
          blobBlockToUpload.getBlockId(), blobBlockToUpload.getOffset());
      op = getClient().append(abfsOutputStream.getPath(), uploadData.toByteArray(),
              reqParams,
              abfsOutputStream.getCachedSasTokenString(),
              abfsOutputStream.getContextEncryptionAdapter(),
              tracingContextAppend);
      blobBlockManager.updateBlockStatus(blobBlockToUpload,
          AbfsBlockStatus.SUCCESS);
    } catch (AbfsRestOperationException ex) {
      LOG.error("Error in remote write requiring handler switch for path {}", abfsOutputStream.getPath(), ex);
      if (shouldIngressHandlerBeSwitched(ex)) {
        throw getIngressHandlerSwitchException(ex);
      }
      LOG.error("Error in remote write for path {} and offset {}", abfsOutputStream.getPath(),
          blobBlockToUpload.getOffset(), ex);
      throw ex;
    }
    return op;
  }

  /**
   * Flushes data to the remote store.
   *
   * @param offset               the offset to flush.
   * @param retainUncommitedData whether to retain uncommitted data.
   * @param isClose              whether this is a close operation.
   * @param leaseId              the lease ID.
   * @param tracingContext       the tracing context.
   * @return the resulting AbfsRestOperation.
   * @throws IOException if an I/O error occurs.
   */
  @Override
  protected synchronized AbfsRestOperation remoteFlush(final long offset,
      final boolean retainUncommitedData,
      final boolean isClose,
      final String leaseId,
      TracingContext tracingContext)
      throws IOException {
    AbfsRestOperation op;
    if (abfsOutputStream.isAppendBlob()) {
      return null;
    }
    if (blobBlockManager.prepareListToCommit(offset) == 0) {
      return null;
    }
    try {
      // Generate the xml with the list of blockId's to generate putBlockList call.
      String blockListXml = generateBlockListXml(
          blobBlockManager.getBlockIdList());
      TracingContext tracingContextFlush = new TracingContext(tracingContext);
      tracingContextFlush.setIngressHandler("BFlush");
      tracingContextFlush.setPosition(String.valueOf(offset));
      LOG.trace("Flushing data at offset {} for path {}", offset, abfsOutputStream.getPath());
      op = getClient().flush(blockListXml.getBytes(StandardCharsets.UTF_8),
              abfsOutputStream.getPath(),
              isClose, abfsOutputStream.getCachedSasTokenString(), leaseId,
              getETag(), abfsOutputStream.getContextEncryptionAdapter(), tracingContextFlush);
      setETag(op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG));
      blobBlockManager.postCommitCleanup();
    } catch (AbfsRestOperationException ex) {
      LOG.error("Error in remote flush requiring handler switch for path {}", abfsOutputStream.getPath(), ex);
      if (shouldIngressHandlerBeSwitched(ex)) {
        throw getIngressHandlerSwitchException(ex);
      }
      LOG.error("Error in remote flush for path {} and offset {}", abfsOutputStream.getPath(), offset, ex);
      throw ex;
    }
    return op;
  }

  /**
   * Sets the eTag of the blob.
   *
   * @param eTag the eTag to set.
   */
  void setETag(String eTag) {
    lock.lock();
    try {
      this.eTag = eTag;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Gets the eTag value of the blob.
   *
   * @return the eTag.
   */
  @VisibleForTesting
  @Override
  public String getETag() {
    lock.lock();
    try {
      return eTag;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Writes the current buffer to the service. .
   *
   */
  @Override
  protected void writeAppendBlobCurrentBufferToService() throws IOException {
    AbfsBlock activeBlock = blobBlockManager.getActiveBlock();

    // No data, return immediately.
    if (!abfsOutputStream.hasActiveBlockDataToUpload()) {
      return;
    }

    // Prepare data for upload.
    final int bytesLength = activeBlock.dataSize();
    DataBlocks.BlockUploadData uploadData = activeBlock.startUpload();

    // Clear active block and update statistics.
    blobBlockManager.clearActiveBlock();
    abfsOutputStream.getOutputStreamStatistics().writeCurrentBuffer();
    abfsOutputStream.getOutputStreamStatistics().bytesToUpload(bytesLength);

    // Update the stream position.
    final long offset = abfsOutputStream.getPosition();
    abfsOutputStream.setPosition(offset + bytesLength);

    // Perform the upload within a performance tracking context.
    try (AbfsPerfInfo perfInfo = new AbfsPerfInfo(
        blobClient.getAbfsPerfTracker(),
        "writeCurrentBufferToService", "append")) {
      LOG.trace("Writing current buffer to service at offset {} and path {}", offset, abfsOutputStream.getPath());
      AppendRequestParameters reqParams = new AppendRequestParameters(
          offset, 0, bytesLength, AppendRequestParameters.Mode.APPEND_MODE,
          true, abfsOutputStream.getLeaseId(), abfsOutputStream.isExpectHeaderEnabled());

      // Perform the remote write operation.
      AbfsRestOperation op = blobClient.appendBlock(abfsOutputStream.getPath(),
          reqParams, uploadData.toByteArray(), new TracingContext(abfsOutputStream.getTracingContext()));

      // Update the SAS token and log the successful upload.
      abfsOutputStream.getCachedSasToken().update(op.getSasToken());
      abfsOutputStream.getOutputStreamStatistics().uploadSuccessful(bytesLength);

      // Register performance information.
      perfInfo.registerResult(op.getResult());
      perfInfo.registerSuccess(true);
    } catch (Exception ex) {
      LOG.error("Failed to upload current buffer of length {} and path {}", bytesLength, abfsOutputStream.getPath(), ex);
      abfsOutputStream.getOutputStreamStatistics().uploadFailed(bytesLength);
      abfsOutputStream.failureWhileSubmit(ex);
    } finally {
      // Ensure the upload data stream is closed.
      IOUtils.closeStreams(uploadData, activeBlock);
    }

  }

  /**
   * Gets the block manager.
   *
   * @return the block manager.
   */
  @Override
  public AzureBlockManager getBlockManager() {
    return blobBlockManager;
  }

  /**
   * Gets the blob client.
   *
   * @return the blob client.
   */
  @Override
  public AbfsBlobClient getClient() {
    return blobClient;
  }

  @VisibleForTesting
  public AbfsClientHandler getClientHandler() {
    return clientHandler;
  }
}
