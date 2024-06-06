/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidIngressServiceException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.store.DataBlocks;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOCK_LIST_END_TAG;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOCK_LIST_START_TAG;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.LATEST_BLOCK_FORMAT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.XML_VERSION;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.INVALID_INGRESS_SERVICE;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.SOURCE_PATH_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.BLOB_OPERATION_NOT_SUPPORTED;

/**
 * Abstract base class for handling ingress operations for Azure Data Lake Storage (ADLS).
 */
public abstract class AzureIngressHandler {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsOutputStream.class);

  /** The output stream associated with this handler */
  protected AbfsOutputStream abfsOutputStream;

  /**
   * Constructs an AzureIngressHandler.
   *
   * @param abfsOutputStream the output stream associated with this handler
   */
  protected AzureIngressHandler(AbfsOutputStream abfsOutputStream) {
    // TODO: sneha: blob handler needs tracing context
    this.abfsOutputStream = abfsOutputStream;
  }

  /**
   * Buffers data into the specified block.
   *
   * @param block the block to buffer data into
   * @param data the data to buffer
   * @param off the start offset in the data
   * @param length the number of bytes to buffer
   * @return the number of bytes buffered
   * @throws IOException if an I/O error occurs
   */
  protected abstract int bufferData(AbfsBlock block,
      final byte[] data,
      final int off,
      final int length) throws IOException;

  /**
   * Performs a remote write operation to upload a block.
   *
   * @param blockToUpload the block to upload
   * @param uploadData the data to upload
   * @param reqParams the request parameters for the append operation
   * @param tracingContext the tracing context
   * @return the result of the REST operation
   * @throws IOException if an I/O error occurs
   */
  protected abstract AbfsRestOperation remoteWrite(AbfsBlock blockToUpload,
      DataBlocks.BlockUploadData uploadData,
      AppendRequestParameters reqParams,
      TracingContext tracingContext) throws IOException;

  /**
   * Performs a remote flush operation.
   *
   * @param offset the offset to flush to
   * @param retainUncommittedData whether to retain uncommitted data
   * @param isClose whether this is a close operation
   * @param leaseId the lease ID
   * @param tracingContext the tracing context
   * @return the result of the REST operation
   * @throws IOException if an I/O error occurs
   */
  protected abstract AbfsRestOperation remoteFlush(final long offset,
      final boolean retainUncommittedData,
      final boolean isClose,
      final String leaseId,
      TracingContext tracingContext) throws IOException;

  /**
   * Writes the current buffer to the service for an append blob.
   *
   * @throws IOException if an I/O error occurs
   */
  protected abstract void writeAppendBlobCurrentBufferToService()
      throws IOException;

  /**
   * Determines if the ingress handler should be switched based on the given exception.
   *
   * @param ex the exception that occurred
   * @return true if the ingress handler should be switched, false otherwise
   */
  protected boolean shouldIngressHandlerBeSwitched(AbfsRestOperationException ex) {
    return ex.getStatusCode() == HTTP_CONFLICT && ex.getMessage()
        .contains(BLOB_OPERATION_NOT_SUPPORTED);
  }

  /**
   * Constructs an InvalidIngressServiceException that includes the current handler class name in the exception message.
   *
   * @param e the original AbfsRestOperationException that triggered this exception.
   * @return an InvalidIngressServiceException with the status code, error code, original message, and handler class name.
   */
  protected InvalidIngressServiceException getIngressHandlerSwitchException(AbfsRestOperationException e) {
    return new InvalidIngressServiceException(e.getStatusCode(),
        INVALID_INGRESS_SERVICE.getErrorCode(),
        e.getMessage() + " " + getClass().getName(), e);
  }

  /**
   * Gets the block manager associated with this handler.
   *
   * @return the block manager
   */
  protected abstract AzureBlockManager getBlockManager();

  /**
   * Gets the client associated with this handler.
   *
   * @return the block manager
   */
  public abstract AbfsClient getClient();

  /**
   * Generates an XML string representing the block list.
   *
   * @param blockIds the set of block IDs
   * @return the generated XML string
   */
  protected static String generateBlockListXml(Set<String> blockIds) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(XML_VERSION);
    stringBuilder.append(BLOCK_LIST_START_TAG);
    for (String blockId : blockIds) {
      stringBuilder.append(String.format(LATEST_BLOCK_FORMAT, blockId));
    }
    stringBuilder.append(BLOCK_LIST_END_TAG);
    return stringBuilder.toString();
  }
}
