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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.store.DataBlocks;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;

/**
 * The BlobFsOutputStream for Rest AbfsClient.
 */
public class AzureDFSIngressHandler extends AzureIngressHandler {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsOutputStream.class);

  private AzureBlockManager blockManager;

  private AbfsDfsClient dfsClient;

  /**
   * Constructs an AzureDFSIngressHandler.
   *
   * @param abfsOutputStream the AbfsOutputStream.
   */
  public AzureDFSIngressHandler(AbfsOutputStream abfsOutputStream) {
    super(abfsOutputStream);
    blockManager = null;
    LOG.trace(
        "Created a new DFSIngress Handler for AbfsOutputStream instance {} for path {}",
        abfsOutputStream.getStreamID(), abfsOutputStream.getPath());
  }

  /**
   * Constructs an AzureDFSIngressHandler with specified parameters.
   *
   * @param abfsOutputStream the AbfsOutputStream.
   * @param blockFactory     the block factory.
   * @param bufferSize       the buffer size.
   */
  public AzureDFSIngressHandler(AbfsOutputStream abfsOutputStream,
      DataBlocks.BlockFactory blockFactory,
      int bufferSize, AbfsClientHandler clientHandler) {
    this(abfsOutputStream);
    this.blockManager = new AzureDFSBlockManager(this.abfsOutputStream,
        blockFactory, bufferSize);
    this.dfsClient = clientHandler.getDfsClient();
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
  public synchronized int bufferData(AbfsBlock block,
      final byte[] data,
      final int off,
      final int length)
      throws IOException {
    LOG.trace("Buffering data of length {} to block at offset {}", length, off);
    return block.write(data, off, length);
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
      TracingContext tracingContext) throws IOException {
    TracingContext tracingContextAppend = new TracingContext(tracingContext);
    if (tracingContextAppend.getIngressHandler().equals(EMPTY_STRING)) {
      tracingContextAppend.setIngressHandler("DAppend");
      tracingContextAppend.setPosition(
          String.valueOf(blockToUpload.getOffset()));
    }
    LOG.trace("Starting remote write for block with offset {} and path {}", blockToUpload.getOffset(), abfsOutputStream.getPath());
    return dfsClient.append(abfsOutputStream.getPath(),
        uploadData.toByteArray(), reqParams,
        abfsOutputStream.getCachedSasTokenString(),
        abfsOutputStream.getContextEncryptionAdapter(),
        tracingContextAppend);
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
    TracingContext tracingContextFlush = new TracingContext(tracingContext);
    if (tracingContextFlush.getIngressHandler().equals(EMPTY_STRING)) {
      tracingContextFlush.setIngressHandler("DFlush");
      tracingContextFlush.setPosition(String.valueOf(offset));
    }
    LOG.trace("Flushing data at offset {} and path {}", offset, abfsOutputStream.getPath());
    return dfsClient
        .flush(abfsOutputStream.getPath(), offset, retainUncommitedData,
            isClose,
            abfsOutputStream.getCachedSasTokenString(), leaseId,
            abfsOutputStream.getContextEncryptionAdapter(),
            tracingContextFlush);
  }

  /**
   * Appending the current active data block to the service. Clearing the active
   * data block and releasing all buffered data.
   *
   * @throws IOException if there is any failure while starting an upload for
   *                     the data block or while closing the BlockUploadData.
   */
  @Override
  protected void writeAppendBlobCurrentBufferToService() throws IOException {
    AbfsBlock activeBlock = blockManager.getActiveBlock();

    // No data, return immediately.
    if (!abfsOutputStream.hasActiveBlockDataToUpload()) {
      return;
    }

    // Prepare data for upload.
    final int bytesLength = activeBlock.dataSize();
    DataBlocks.BlockUploadData uploadData = activeBlock.startUpload();

    // Clear active block and update statistics.
    blockManager.clearActiveBlock();
    abfsOutputStream.getOutputStreamStatistics().writeCurrentBuffer();
    abfsOutputStream.getOutputStreamStatistics().bytesToUpload(bytesLength);

    // Update the stream position.
    final long offset = abfsOutputStream.getPosition();
    abfsOutputStream.setPosition(offset + bytesLength);

    // Perform the upload within a performance tracking context.
    try (AbfsPerfInfo perfInfo = new AbfsPerfInfo(
        dfsClient.getAbfsPerfTracker(),
        "writeCurrentBufferToService", "append")) {
      LOG.trace("Writing current buffer to service at offset {} and path {}", offset, abfsOutputStream.getPath());
      AppendRequestParameters reqParams = new AppendRequestParameters(
          offset, 0, bytesLength, AppendRequestParameters.Mode.APPEND_MODE,
          true, abfsOutputStream.getLeaseId(), abfsOutputStream.isExpectHeaderEnabled());

      // Perform the remote write operation.
      AbfsRestOperation op = remoteWrite(activeBlock, uploadData, reqParams,
          new TracingContext(abfsOutputStream.getTracingContext()));

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
    return blockManager;
  }

  /**
   * Gets the dfs client.
   *
   * @return the dfs client.
   */
  @Override
  public AbfsDfsClient getClient() {
    return dfsClient;
  }
}
