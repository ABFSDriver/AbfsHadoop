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

public class AzureBlobIngressFallbackHandler extends AzureDFSIngressHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AbfsOutputStream.class);

  private final AzureBlobBlockManager blobBlockManager;
  private String eTag;
  private final Lock lock = new ReentrantLock();

  public AzureBlobIngressFallbackHandler(AbfsOutputStream abfsOutputStream,
      DataBlocks.BlockFactory blockFactory,
      int bufferSize, String eTag)
      throws AzureBlobFileSystemException {
    super(abfsOutputStream);
    this.eTag = eTag;
    this.blobBlockManager = new AzureBlobBlockManager(this.abfsOutputStream,
        blockFactory,
        bufferSize);
  }

  @Override
  public synchronized int bufferData(AbfsBlock block,
      final byte[] data,
      final int off,
      final int length) throws IOException {
    AbfsBlobBlock blobBlock = (AbfsBlobBlock) block;
    blobBlockManager.trackBlockWithData(blobBlock);
    return super.bufferData(block, data, off, length);
  }

  @Override
  protected AbfsRestOperation remoteWrite(AbfsBlock blockToUpload,
      DataBlocks.BlockUploadData uploadData,
      AppendRequestParameters reqParams,
      TracingContext tracingContext)
      throws IOException {
    AbfsRestOperation op = null;
    AbfsBlobBlock blobBlockToUpload = (AbfsBlobBlock) blockToUpload;
    reqParams.setBlockId(blobBlockToUpload.getBlockId());
    reqParams.setEtag(getETag());
    try {
      op = super.remoteWrite(blockToUpload, uploadData, reqParams,
          tracingContext);
      blobBlockManager.updateBlockStatus(blobBlockToUpload,
          AbfsBlockStatus.SUCCESS);
    } catch (AbfsRestOperationException ex) {
      if (shouldIngressHandlerBeSwitched(ex)) {
        throw getIngressHandlerSwitchException(ex);
      }

      throw ex;
    }

    return op;
  }

  @Override
  protected synchronized AbfsRestOperation remoteFlush(final long offset,
      final boolean retainUncommitedData,
      final boolean isClose,
      final String leaseId,
      TracingContext tracingContext)
      throws IOException {
    AbfsRestOperation op;

    if (blobBlockManager.prepareListToCommit(offset) == 0) {
      return null;
    }
    try {
      String blockListXml = generateBlockListXml(blobBlockManager.getBlockIdList());
      op = abfsOutputStream.getClient()
          .flush(blockListXml.getBytes(), abfsOutputStream.getPath(),
              isClose, abfsOutputStream.getCachedSasTokenString(), leaseId,
              getETag(), new TracingContext(tracingContext));
      setETag(op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG));
      blobBlockManager.postCommitCleanup();
    } catch (AbfsRestOperationException ex) { // todo: sneha - for flush
      // without appends but with lease - close
      if (shouldIngressHandlerBeSwitched(ex)) {
        throw getIngressHandlerSwitchException(ex);
      }

      throw ex;
    }
    return op;
  }

  @Override
  public AzureBlockManager getBlockManager() {
    return blobBlockManager;
  }

  /**
   * Set the eTag of the blob.
   *
   * @param eTag eTag.
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
   * Get eTag value of blob.
   *
   * @return eTag.
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
