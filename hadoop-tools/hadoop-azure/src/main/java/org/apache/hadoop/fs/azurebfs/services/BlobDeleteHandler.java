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
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.PATH_NOT_FOUND;

/**
 * Orchestrator for delete over Blob endpoint. Blob endpoint for flat-namespace
 * account does not support director delete. This class is responsible for
 * deleting the blobs and creating the parent directory marker file if needed.
 */
public class BlobDeleteHandler extends ListActionTaker {

  private static final Logger LOG = LoggerFactory.getLogger(
      AzureBlobFileSystemStore.class);

  private final Path path;

  private final boolean recursive;

  private boolean nonRecursiveDeleteDirectoryFailed = false;

  private final TracingContext tracingContext;

  private final AtomicInteger deleteCount = new AtomicInteger(0);


  public BlobDeleteHandler(final Path path,
      final boolean recursive,
      final AbfsBlobClient abfsBlobClient,
      final TracingContext tracingContext) {
    super(path, abfsBlobClient, abfsBlobClient.getAbfsConfiguration()
        .getBlobDeleteDirConsumptionParallelism(), tracingContext);
    this.path = path;
    this.recursive = recursive;
    this.tracingContext = tracingContext;
  }

  private boolean deleteInternal(final Path path)
      throws AzureBlobFileSystemException {
    abfsClient.deleteBlobPath(path, null, tracingContext);
    deleteCount.incrementAndGet();
    return true;
  }

  /**
   * Orchestrate the delete operation.
   *
   * @return true if the delete operation is successful.
   * @throws IOException if deletion fails due to server error or path doesn't exist.
   */
  public boolean execute() throws IOException {
    /*
     * ABFS is not aware if it's a file or directory. So, we need to list the
     * path and delete the listed objects. The listing returns the children of
     * the path and not the path itself.
     */
    listRecursiveAndTakeAction();
    if (nonRecursiveDeleteDirectoryFailed) {
      throw new IOException("Non-recursive delete of non-empty directory");
    }
    tracingContext.setOperatedBlobCount(deleteCount.get() + 1);
    /*
     * If path is actually deleted.
     */
    boolean deleted;
    try {
      /*
       * Delete the required path.
       * Directory needs to be safely delete the path, as the path can be implicit.
       */
      deleted = recursive ? safeDelete(path) : deleteInternal(path);
    } finally {
      tracingContext.setOperatedBlobCount(null);
    }
    if (deleteCount.get() == 0) {
      /*
       * DeleteCount can be zero only if the path does not exist.
       */
      throw new AbfsRestOperationException(HTTP_NOT_FOUND,
          PATH_NOT_FOUND.getErrorCode(), PATH_NOT_FOUND.getErrorMessage(),
          new PathIOException(path.toString(), "Path not found"));
    }

    /*
     * Ensure that parent directory of the deleted path is marked as a folder. This
     * is required if the parent is an implicit directory (path with no marker blob),
     * and the given path is the only child of the parent, the parent would become
     * non-existing.
     */
    if (deleted) {
      ensurePathParentExist();
    }
    return deleted;
  }

  private void ensurePathParentExist()
      throws AzureBlobFileSystemException {
    if (!path.isRoot() && !path.getParent().isRoot()) {
      createDir(path.getParent());
//      try {
//        abfsClient.create
//        abfsClient.getCreateCallback()
//            .createDirectory(path.getParent(), tracingContext);
//      } catch (AbfsRestOperationException ex) {
//        if (ex.getStatusCode() != HTTP_CONFLICT) {
//          throw ex;
//        }
//      }
    }
  }

  @VisibleForTesting
  void createDir(final Path parent) {

  }

  /**{@inheritDoc}*/
  @Override
  boolean takeAction(final Path path) throws IOException {
    if (!recursive) {
      /*
       * If the delete operation is non-recursive, then the path can not be a directory.
       */
      nonRecursiveDeleteDirectoryFailed = true;
      return false;
    }
    return safeDelete(path);
  }

  /**
   * Delete the path if it exists. Gracefully handles the case where the path does not exist.
   *
   * @param path path to delete.
   * @return true if the path is deleted or is not found.
   * @throws AzureBlobFileSystemException server error.
   */
  private boolean safeDelete(final Path path)
      throws AzureBlobFileSystemException {
    try {
      return deleteInternal(path);
    } catch (AbfsRestOperationException ex) {
      if (ex.getStatusCode() == HTTP_NOT_FOUND) {
        return true;
      }
      throw ex;
    }
  }
}
