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

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidIngressServiceException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.store.DataBlocks;

import java.io.IOException;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.SOURCE_PATH_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.BLOB_OPERATION_NOT_SUPPORTED;

public abstract class AzureIngressHandler {

    protected AbfsOutputStream abfsOutputStream;

    protected AzureIngressHandler(AbfsOutputStream abfsOutputStream) {
        // TODO: sneha: blob handler needs trcing context
        //  add a trace log here with this instance
        this.abfsOutputStream = abfsOutputStream;
    }

    protected abstract int bufferData(AbfsBlock block, final byte[] data, final int off, final int length)
            throws IOException;

    protected abstract AbfsRestOperation remoteWrite(AbfsBlock blockToUpload,
                                                     DataBlocks.BlockUploadData uploadData,
                                                     AppendRequestParameters reqParams,
                                                     TracingContext tracingContext) throws IOException;

    protected abstract AbfsRestOperation remoteFlush(final long offset,
                                                     final boolean retainUncommitedData,
                                                     final boolean isClose,
                                                     final String leaseId,
                                                     TracingContext tracingContext) throws IOException;

    protected abstract void writeAppendBlobCurrentBufferToService() throws IOException;

    protected boolean shouldIngressHandlerBeSwitched(AbfsRestOperationException ex) {
        if (ex.getStatusCode() == HTTP_CONFLICT && ex.getMessage().contains(BLOB_OPERATION_NOT_SUPPORTED)) {
            return true;
        }

        return false;
    }

    // todo: sneha - create a custom RestOpException to identify switch
    // that has current handler class name in exception message
    protected InvalidIngressServiceException getIngressHandlerSwitchException(AbfsRestOperationException e) {
        return new InvalidIngressServiceException(e.getStatusCode(), SOURCE_PATH_NOT_FOUND.getErrorCode(),
                e.getMessage(), e);
    }

    public abstract AzureBlockManager getBlockManager();

}
