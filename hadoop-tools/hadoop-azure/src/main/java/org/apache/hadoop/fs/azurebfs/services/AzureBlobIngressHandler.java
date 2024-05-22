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

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.store.DataBlocks;
import org.apache.hadoop.classification.VisibleForTesting;

import java.io.IOException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The BlobFsOutputStream for Rest AbfsClient.
 */
public class AzureBlobIngressHandler extends AzureIngressHandler  {
    // todo: sneha - just pass down log instance from parent given same class?
    private static final Logger LOG =
            LoggerFactory.getLogger(AbfsOutputStream.class);

    /** The etag of the blob. */
    // TODO: sneha - when final blob code lands this needs to be plumbed through outputstreamContext
    private String eTag = UUID.randomUUID().toString();
    private AzureBlobBlockManager blobBlockManager;

    public AzureBlobIngressHandler(AbfsOutputStream abfsOutputStream,
                                   DataBlocks.BlockFactory blockFactory,
                                   int bufferSize)
            throws AzureBlobFileSystemException {
        super(abfsOutputStream);
        this.blobBlockManager = new AzureBlobBlockManager(this.abfsOutputStream,
                blockFactory,
                bufferSize);
    }

    @Override
    public synchronized int bufferData(AbfsBlock block,final byte[] data, final int off, final int length)
            throws IOException {
        AbfsBlobBlock blobBlock = (AbfsBlobBlock) block;
        blobBlockManager.trackBlockWithData(blobBlock);
        return blobBlock.write(data, off, length);
    }

    @Override
    protected AbfsRestOperation remoteWrite(AbfsBlock blockToUpload,
                                            DataBlocks.BlockUploadData uploadData,
                                            AppendRequestParameters reqParams,
                                            TracingContext tracingContext)
            throws IOException {
        AbfsBlobBlock blobBlockToUpload = (AbfsBlobBlock) blockToUpload;
        AbfsRestOperation op = null;
        try {
            // todo: sneha abfsclient update for blob apis, below is dummy dfs call
            op = abfsOutputStream.getClient()
                    .append(abfsOutputStream.getPath(),
                            uploadData.toByteArray(), reqParams,
                            abfsOutputStream.getCachedSasTokenString(),
                            abfsOutputStream.getContextEncryptionAdapter(),
                            new TracingContext(tracingContext));
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
        AbfsRestOperation op = null;

        if (blobBlockManager.prepareListToCommit() == 0) {
            return null;
        }

        try {
            // Generate the xml with the list of blockId's to generate putBlockList call.
            String blockListXml = generateBlockListXml(
                    blobBlockManager.getBlockIdList());
            // todo: sneha - pending blob api support. dummy dfs call below
            op = abfsOutputStream.getClient()
                    .flush(abfsOutputStream.getPath(), offset,
                            retainUncommitedData, isClose,
                            abfsOutputStream.getCachedSasTokenString(),
                            leaseId,
                            abfsOutputStream.getContextEncryptionAdapter(),
                            new TracingContext(tracingContext));
            blobBlockManager.postCommitCleanup();
        } catch (AbfsRestOperationException ex) { // will be needed if flush came when lease is effective
            if (shouldIngressHandlerBeSwitched(ex)) {
                throw getIngressHandlerSwitchException(ex);
            }

            throw ex;
        }
        return op;
    }


    /**
     * Set the eTag of the blob.
     *
     * @param eTag eTag.
     */
    void setETag(String eTag) {
        // todo: sneha - is there an lock for etag ? purpose ?
        this.eTag = eTag;
    }

    /**
     * Get eTag value of blob.
     *
     * @return eTag.
     */
    @VisibleForTesting
    public String getETag() {
        return eTag;
    }


    @Override
    protected void writeAppendBlobCurrentBufferToService() throws IOException {
        // Should never hit this
        throw new InvalidConfigurationValueException("AppendBlob support with BlobEndpoint not " +
                "enabled in ABFS driver yet.");
    }

    private static String generateBlockListXml(Set<String> blockIds) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(XML_VERSION);
        stringBuilder.append(BLOCK_LIST_START_TAG);
        for (String blockId : blockIds) {
            stringBuilder.append(String.format(LATEST_BLOCK_FORMAT, blockId));
        }
        stringBuilder.append(BLOCK_LIST_END_TAG);
        return stringBuilder.toString();
    }

    @Override
    public AzureBlockManager getBlockManager() {
        return blobBlockManager;
    }

}