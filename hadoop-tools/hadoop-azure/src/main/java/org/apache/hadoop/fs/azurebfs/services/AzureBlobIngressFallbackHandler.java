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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.store.DataBlocks;

/**
 * Handles the fallback mechanism for Azure Blob Ingress operations.
 */
public class AzureBlobIngressFallbackHandler extends AzureDFSIngressHandler {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsOutputStream.class);

  private final AzureBlobBlockManager blobBlockManager;

  private String eTag;

  private final Lock lock = new ReentrantLock();

  /**
   * Constructs an AzureBlobIngressFallbackHandler.
   *
   * @param abfsOutputStream the AbfsOutputStream.
   * @param blockFactory     the block factory.
   * @param bufferSize       the buffer size.
   * @param eTag             the eTag.
   * @throws AzureBlobFileSystemException if an error occurs.
   */
  public AzureBlobIngressFallbackHandler(AbfsOutputStream abfsOutputStream,
      DataBlocks.BlockFactory blockFactory,
      int bufferSize, String eTag, AbfsClientHandler clientHandler) throws AzureBlobFileSystemException {
    super(abfsOutputStream, clientHandler);
    this.eTag = eTag;
    this.blobBlockManager = new AzureBlobBlockManager(this.abfsOutputStream,
        blockFactory, bufferSize);
    LOG.trace(
        "Created a new BlobFallbackIngress Handler for AbfsOutputStream instance {} for path {}",
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
  public synchronized int bufferData(AbfsBlock block,
      final byte[] data,
      final int off,
      final int length) throws IOException {
    blobBlockManager.trackBlockWithData(block);
    LOG.trace("Buffering data of length {} to block at offset {}", length, off);
    return super.bufferData(block, data, off, length);
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
    AbfsRestOperation op;
    TracingContext tracingContextAppend = new TracingContext(tracingContext);
    tracingContextAppend.setIngressHandler("FBAppend");
    tracingContextAppend.setPosition(String.valueOf(blockToUpload.getOffset()));
    try {
      op = super.remoteWrite(blockToUpload, uploadData, reqParams,
          tracingContextAppend);
      blobBlockManager.updateBlockStatus(blockToUpload,
          AbfsBlockStatus.SUCCESS);
    } catch (AbfsRestOperationException ex) {
      if (shouldIngressHandlerBeSwitched(ex)) {
        LOG.error("Error in remote write requiring handler switch for path {}", abfsOutputStream.getPath(), ex);
        throw getIngressHandlerSwitchException(ex);
      }
      LOG.error("Error in remote write for path {} and offset {}", abfsOutputStream.getPath(),
          blockToUpload.getOffset(), ex);
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
      TracingContext tracingContext) throws IOException {
    AbfsRestOperation op;
    if (blobBlockManager.prepareListToCommit(offset) == 0) {
      return null;
    }
    try {
      TracingContext tracingContextFlush = new TracingContext(tracingContext);
      tracingContextFlush.setIngressHandler("FBFlush");
      tracingContextFlush.setPosition(String.valueOf(offset));
      op = super.remoteFlush(offset, retainUncommitedData, isClose, leaseId,
          tracingContextFlush);
      blobBlockManager.postCommitCleanup();
    } catch (AbfsRestOperationException ex) {
      if (shouldIngressHandlerBeSwitched(ex)) {
        LOG.error("Error in remote flush requiring handler switch for path {}", abfsOutputStream.getPath(), ex);
        throw getIngressHandlerSwitchException(ex);
      }
      LOG.error("Error in remote flush for path {} and offset {}", abfsOutputStream.getPath(), offset, ex);
      throw ex;
    }
    return op;
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
   * Gets the eTag value of the blob.
   *
   * @return the eTag.
   */
  @VisibleForTesting
  public String getETag() {
    lock.lock();
    try {
      return eTag;
    } finally {
      lock.unlock();
    }
  }
}
