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

/**
 * The BlobFsOutputStream for Rest AbfsClient.
 */
public class AzureDFSIngressHandler extends AzureIngressHandler {

    private static final Logger LOG =
            LoggerFactory.getLogger(AbfsOutputStream.class);

    private AzureBlockManager blockManager;

    public AzureDFSIngressHandler(AbfsOutputStream abfsOutputStream) {
        super(abfsOutputStream);
        blockManager = null;
    }

    public AzureDFSIngressHandler(AbfsOutputStream abfsOutputStream,
                                  DataBlocks.BlockFactory blockFactory,
                                  int bufferSize) {
        this(abfsOutputStream);
        this.blockManager = new AzureDFSBlockManager(this.abfsOutputStream,
                blockFactory, bufferSize);
    }

    @Override
    public synchronized int bufferData(AbfsBlock block, final byte[] data, final int off, final int length)
            throws IOException {
        return block.write(data, off, length);
    }

    @Override
    protected AbfsRestOperation remoteWrite(AbfsBlock blockToUpload,
                                            DataBlocks.BlockUploadData uploadData,
                                            AppendRequestParameters reqParams,
                                            TracingContext tracingContext) throws IOException {
        return abfsOutputStream.getClient().append(abfsOutputStream.getPath(),
                uploadData.toByteArray(), reqParams, abfsOutputStream.getCachedSasTokenString(),
                abfsOutputStream.getContextEncryptionAdapter(), new TracingContext(tracingContext));
    }

    @Override
    protected synchronized AbfsRestOperation remoteFlush(final long offset,
                                                         final boolean retainUncommitedData,
                                                         final boolean isClose,
                                                         final String leaseId,
                                                         TracingContext tracingContext)
            throws IOException {
        return abfsOutputStream.getClient()
                .flush(abfsOutputStream.getPath(), offset, retainUncommitedData, isClose,
                        abfsOutputStream.getCachedSasTokenString(), leaseId,
                        abfsOutputStream.getContextEncryptionAdapter(),
                        new TracingContext(tracingContext));
    }

    /**
     * Appending the current active data block to service. Clearing the active
     * data block and releasing all buffered data.
     * @throws IOException if there is any failure while starting an upload for
     *                     the dataBlock or while closing the BlockUploadData.
     */
    // todo: sneha - is just a direct call to append data
    // maybe not hard to include append blob support over blobendpoint too
    // right away ?
    protected void writeAppendBlobCurrentBufferToService() throws IOException {
//        AbfsBlock activeBlock = getActiveBlock();
//        // No data, return.
//        if (!hasActiveBlockDataToUpload()) {
//            return;
//        }
//
//        final int bytesLength = activeBlock.dataSize();
//        DataBlocks.BlockUploadData uploadData = activeBlock.startUpload();
//        clearActiveBlock();
//        outputStreamStatistics.writeCurrentBuffer();
//        outputStreamStatistics.bytesToUpload(bytesLength);
//        final long offset = position;
//        position += bytesLength;
//        AbfsPerfTracker tracker = client.getAbfsPerfTracker();
//        try (AbfsPerfInfo perfInfo = new AbfsPerfInfo(tracker,
//                "writeCurrentBufferToService", "append")) {
//            AppendRequestParameters reqParams = new AppendRequestParameters(offset, 0,
//                    bytesLength, APPEND_MODE, true, leaseId, isExpectHeaderEnabled);
//            AbfsRestOperation op = client.append(path, uploadData.toByteArray(),
//                    reqParams, cachedSasToken.get(), contextEncryptionAdapter,
//                    new TracingContext(tracingContext));
//            cachedSasToken.update(op.getSasToken());
//            outputStreamStatistics.uploadSuccessful(bytesLength);
//
//            perfInfo.registerResult(op.getResult());
//            perfInfo.registerSuccess(true);
//            return;
//        } catch (Exception ex) {
//            outputStreamStatistics.uploadFailed(bytesLength);
//            failureWhileSubmit(ex);
//        } finally {
//            IOUtils.close(uploadData);
//        }
    }

    @Override
    public AzureBlockManager getBlockManager() {
        return blockManager;
    }

}
