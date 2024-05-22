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
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.enums.BlobCopyProgress;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COPY_STATUS_ABORTED;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COPY_STATUS_FAILED;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COPY_STATUS_SUCCESS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_COPY_ID;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_COPY_SOURCE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_COPY_STATUS;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_COPY_STATUS_DESCRIPTION;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.COPY_BLOB_ABORTED;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.COPY_BLOB_FAILED;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.RENAME_DESTINATION_PARENT_PATH_NOT_FOUND;

public class BlobRenameHandler extends ListActionTaker {

  public static final Logger LOG = LoggerFactory.getLogger(AbfsClient.class);

  private final String source;

  private final String destination;

  private final String srcEtag;

  private final Path src, dst;

  private final AbfsBlobClient abfsBlobClient;

  private final boolean isAtomicRename, isAtomicRenameRecovery;
  private final TracingContext tracingContext;
  private AbfsLease srcAbfsLease;
  private String srcLeaseId;
  private final List<AbfsLease> leases = new ArrayList<>();
  private final AtomicInteger operatedBlobCount = new AtomicInteger(0);

  public BlobRenameHandler(final String src,
      final String dst,
      final AbfsClient abfsClient,
      final String srcEtag,
      final boolean isAtomicRename,
      final boolean isAtomicRenameRecovery,
      final TracingContext tracingContext) {
    super(new Path(src), abfsClient, tracingContext);
    this.source = src;
    this.destination = dst;
    this.abfsBlobClient = (AbfsBlobClient) abfsClient;
    this.srcEtag = srcEtag;
    this.tracingContext = tracingContext;
    this.src = new Path(src);
    this.dst = new Path(dst);
    this.isAtomicRename = isAtomicRename;
    this.isAtomicRenameRecovery = isAtomicRenameRecovery;
  }

  public AbfsClientRenameResult execute() throws IOException {
    PathInformation pathInformation = new PathInformation();
    final boolean result;
    if (preCheck(src, dst, pathInformation)) {

      RenameAtomicity renameAtomicity = null;
      if (isAtomicRename) {
        srcAbfsLease = takeLease(src, srcEtag);
        srcLeaseId = srcAbfsLease.getLeaseID();
        if (!isAtomicRenameRecovery) {
          renameAtomicity = getRenameAtomicity(pathInformation);
          renameAtomicity.preRename();
        }
      }
      if (pathInformation.getIsDirectory()) {
        result = listRecursiveAndTakeAction() && finalSrcRename();
      } else {
        result = renameInternal(src, dst);
      }
      if (result && renameAtomicity != null) {
        renameAtomicity.postRename();
      }
      return new AbfsClientRenameResult(null, result, false);
    } else {
      return new AbfsClientRenameResult(null, false, false);
    }
  }

  private boolean finalSrcRename() throws IOException {
    tracingContext.setOperatedBlobCount(operatedBlobCount.get() + 1);
    try {
      return renameInternal(src, dst);
    } finally {
      tracingContext.setOperatedBlobCount(null);
    }
  }

  @VisibleForTesting
  public RenameAtomicity getRenameAtomicity(final PathInformation pathInformation)
      throws IOException {
    return new RenameAtomicity(src, dst,
        new Path(src.getParent(), src.getName() + RenameAtomicity.SUFFIX),
        abfsBlobClient.getCreateCallback(),
        abfsBlobClient.getReadCallback(), tracingContext, false,
        pathInformation.getETag(),
        abfsBlobClient);
  }

  private AbfsLease takeLease(final Path src, final String eTag)
      throws AzureBlobFileSystemException {
    AbfsLease lease = new AbfsLease(abfsBlobClient, src.toUri().getPath(),
        abfsBlobClient.getAbfsConfiguration().getAtomicRenameLeaseDuration(),
        eTag,
        tracingContext);
    leases.add(lease);
    return lease;
  }

  private boolean containsColon(Path p) {
    return p.toUri().getPath().contains(":");
  }

  private boolean preCheck(final Path src, final Path dst,
      final PathInformation pathInformation) throws IOException {
    if (containsColon(dst)) {
      throw new IOException("Cannot rename to file " + dst
          + " that has colons in the name through blob endpoint");
    }
    Path nestedDstParent = dst.getParent();
    LOG.debug("Check if the destination is subDirectory");
    if (nestedDstParent != null && nestedDstParent.toUri()
        .getPath()
        .indexOf(src.toUri().getPath()) == 0) {
      LOG.info("Rename src: {} dst: {} failed as dst is subDir of src",
          src, dst);
      return false;
    }

    pathInformation.copy(abfsBlobClient.getPathInformation(src,
        tracingContext));
    final boolean result;
    if(!pathInformation.getPathExists()) {
      throw new AbfsRestOperationException(
          HttpURLConnection.HTTP_NOT_FOUND,
          AzureServiceErrorCode.SOURCE_PATH_NOT_FOUND.getErrorCode(), null,
          new Exception(AzureServiceErrorCode.SOURCE_PATH_NOT_FOUND.getErrorCode()));
    }
    if(srcEtag != null && !srcEtag.equals(pathInformation.getETag())) {
      throw new AbfsRestOperationException(
          HttpURLConnection.HTTP_CONFLICT,
          AzureServiceErrorCode.PATH_ALREADY_EXISTS.getErrorCode(), null,
          new Exception(AzureServiceErrorCode.PATH_ALREADY_EXISTS.getErrorCode()));
    }

    /*
     * Destination path name can be same to that of source path name only in the
     * case of a directory rename.
     *
     * In case the directory is being renamed to some other name, the destination
     * check would happen on the AzureBlobFileSystem#rename method.
     */
    if (pathInformation.getIsDirectory() && dst.getName().equals(src.getName())) {
      PathInformation dstPathInformation = abfsBlobClient.getPathInformation(dst,
          tracingContext);
      if (dstPathInformation.getPathExists()) {
        LOG.info(
            "Rename src: {} dst: {} failed as qualifiedDst already exists",
            src, dst);
        throw new AbfsRestOperationException(
            HttpURLConnection.HTTP_CONFLICT,
            AzureServiceErrorCode.PATH_ALREADY_EXISTS.getErrorCode(), null,
            null);
      }
    }

    //TODO: pranav: check if getPathInformation(dst) is required? or will copyBlob on existing dst will fail and would fail the rename?

    if (!dst.isRoot() && !nestedDstParent.isRoot() && (
        !pathInformation.getIsDirectory() || !dst.getName()
            .equals(src.getName()))) {
      PathInformation nestedDstInfo = abfsBlobClient.getPathInformation(
          nestedDstParent,
          tracingContext);
      if (!nestedDstInfo.getPathExists() || !nestedDstInfo.getIsDirectory()) {
        throw new AbfsRestOperationException(
            HttpURLConnection.HTTP_NOT_FOUND,
            RENAME_DESTINATION_PARENT_PATH_NOT_FOUND.getErrorCode(), null,
            new Exception(
                RENAME_DESTINATION_PARENT_PATH_NOT_FOUND.getErrorCode()));
      }
    }

    return true;
  }

  @Override
  boolean takeAction(final Path path) throws IOException {
    return renameInternal(path,
        createDestinationPathForBlobPartOfRenameSrcDir(dst, path, src));
  }

  private boolean renameInternal(final Path path,
      final Path destinationPathForBlobPartOfRenameSrcDir) throws IOException {
    final String leaseId;
    AbfsLease abfsLease = null;
    if (isAtomicRename) {
      if (path.equals(src)) {
        abfsLease = srcAbfsLease;
        leaseId = srcLeaseId;
      } else {
        abfsLease = takeLease(path, null);
        leaseId = abfsLease.getLeaseID();
      }
    } else {
      leaseId = null;
    }
    copyPath(path, destinationPathForBlobPartOfRenameSrcDir, leaseId);
    abfsClient.deleteBlobPath(path, leaseId, tracingContext);
    if (abfsLease != null) {
      abfsLease.cancelTimer();
    }
    operatedBlobCount.incrementAndGet();
    return true;
  }

  private void copyPath(final Path src, final Path dst, final String leaseId)
      throws IOException {
    String copyId;
    try {
      AbfsRestOperation copyPathOp = abfsClient.copyBlob(src, dst, leaseId,
          tracingContext);
      final String progress = copyPathOp.getResult()
          .getResponseHeader(X_MS_COPY_STATUS);
      if (COPY_STATUS_SUCCESS.equalsIgnoreCase(progress)) {
        return;
      }
      copyId = copyPathOp.getResult()
          .getResponseHeader(X_MS_COPY_ID);
    } catch (AbfsRestOperationException ex) {
      if(ex.getStatusCode() == HttpURLConnection.HTTP_CONFLICT) {
        AbfsRestOperation dstPathStatus = abfsClient.getPathStatus(dst.toUri().getPath(),
            false, tracingContext, null);
        if (dstPathStatus.getResult() != null && ((ROOT_PATH
            + abfsClient.getFileSystem() + src.toUri()
            .getPath()).equals(
            getDstSource(dstPathStatus)))) {
          return;
        }
      }
      throw ex;
    }
    final long pollWait = abfsClient.getAbfsConfiguration()
        .getBlobCopyProgressPollWaitMillis();
    while (handleCopyInProgress(dst, tracingContext, copyId)
        == BlobCopyProgress.PENDING) {
      try {
        Thread.sleep(pollWait);
      } catch (Exception e) {

      }
    }
  }

  private String getDstSource(final AbfsRestOperation dstPathStatus) {
    try {
      String responseHeader = dstPathStatus.getResult()
          .getResponseHeader(X_MS_COPY_SOURCE);
      if (responseHeader == null) {
        return null;
      }
      return new URL(responseHeader).toURI().getPath();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Verifies if the blob copy is success or a failure or still in progress.
   *
   * @param dstPath path of the destination for the copying
   * @param tracingContext object of tracingContext used for the tracing of the
   * server calls.
   * @param copyId id returned by server on the copy server-call. This id gets
   * attached to blob and is returned by GetBlobProperties API on the destination.
   *
   * @return true if copying is success, false if it is still in progress.
   *
   * @throws AzureBlobFileSystemException exception returned in making server call
   * for GetBlobProperties on the path. It can be thrown if the copyStatus is failure
   * or is aborted.
   */
  @VisibleForTesting
  public BlobCopyProgress handleCopyInProgress(final Path dstPath,
      final TracingContext tracingContext,
      final String copyId) throws AzureBlobFileSystemException {
    AbfsRestOperation op = abfsClient.getPathStatus(dstPath.toUri().getPath(),
        false, tracingContext, null);

    if (op.getResult() != null && copyId.equals(
        op.getResult().getResponseHeader(X_MS_COPY_ID))) {
      final String copyStatus = op.getResult()
          .getResponseHeader(X_MS_COPY_STATUS);
      if (COPY_STATUS_SUCCESS.equalsIgnoreCase(copyStatus)) {
        return BlobCopyProgress.SUCCESS;
      }
      if (COPY_STATUS_FAILED.equalsIgnoreCase(copyStatus)) {
        throw new AbfsRestOperationException(
            COPY_BLOB_FAILED.getStatusCode(), COPY_BLOB_FAILED.getErrorCode(),
            String.format("copy to path %s failed due to: %s",
                dstPath.toUri().getPath(),
                op.getResult().getResponseHeader(X_MS_COPY_STATUS_DESCRIPTION)),
            new Exception(COPY_BLOB_FAILED.getErrorCode()));
      }
      if (COPY_STATUS_ABORTED.equalsIgnoreCase(copyStatus)) {
        throw new AbfsRestOperationException(
            COPY_BLOB_ABORTED.getStatusCode(), COPY_BLOB_ABORTED.getErrorCode(),
            String.format("copy to path %s aborted", dstPath.toUri().getPath()),
            new Exception(COPY_BLOB_ABORTED.getErrorCode()));
      }
    }
    return BlobCopyProgress.PENDING;
  }

  /**
   * Translates the destination path for a blob part of a source directory getting
   * renamed.
   *
   * @param destinationDir destination directory for the rename operation
   * @param blobPath path of blob inside sourceDir being renamed.
   * @param sourceDir source directory for the rename operation
   *
   * @return translated path for the blob
   */
  private Path createDestinationPathForBlobPartOfRenameSrcDir(final Path destinationDir,
      final Path blobPath, final Path sourceDir) {
    String destinationPathStr = destinationDir.toUri().getPath();
    String sourcePathStr = sourceDir.toUri().getPath();
    String srcBlobPropertyPathStr = blobPath.toUri().getPath();
    if (sourcePathStr.equals(srcBlobPropertyPathStr)) {
      return destinationDir;
    }
    return new Path(
        destinationPathStr + ROOT_PATH + srcBlobPropertyPathStr.substring(
            sourcePathStr.length()));
  }

  @VisibleForTesting
  public List<AbfsLease> getLeases() {
    return leases;
  }
}
