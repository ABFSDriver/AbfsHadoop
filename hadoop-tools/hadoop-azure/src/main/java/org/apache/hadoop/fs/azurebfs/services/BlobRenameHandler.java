package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.HttpURLConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.enums.BlobCopyProgress;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COPY_STATUS_ABORTED;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COPY_STATUS_FAILED;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COPY_STATUS_SUCCESS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_COPY_ID;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_COPY_STATUS;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_COPY_STATUS_DESCRIPTION;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.COPY_BLOB_ABORTED;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.COPY_BLOB_FAILED;

public class BlobRenameHandler extends RenameHandler {

  private static final Logger LOG = LoggerFactory.getLogger(
      AzureBlobFileSystemStore.class);

  public BlobRenameHandler(final Path src,
      final Path dst,
      final AbfsClient abfsClient,
      final boolean isAtomicRenameKey,
      final String srcEtag,
      final AbfsCounters abfsCounters,
      final boolean isNamespaceEnabled,
      final AzureBlobFileSystemStore.GetFileStatusImpl getFileStatusCallback,
      final AzureBlobFileSystem.GetRenameAtomicityCreateCallback renameAtomicityCreateCallback,
      final AzureBlobFileSystem.GetRenameAtomicityReadCallback renameAtomicityReadCallback,
      final TracingContext tracingContext) {
    super(src, dst, abfsClient, isAtomicRenameKey, srcEtag, isNamespaceEnabled,
        abfsCounters,
        getFileStatusCallback,
        tracingContext);
  }

  @Override
  protected PathInformation getPathInformation(final Path path)
      throws IOException {
    try {
      AbfsRestOperation op = getFileStatusImpl.getFileStatus(path, null,
          isNamespaceEnabled, tracingContext);
      return new PathInformation(true,
          AbfsHttpConstants.DIRECTORY.equalsIgnoreCase(op.getResult()
              .getResponseHeader(HttpHeaderConfigurations.X_MS_RESOURCE_TYPE)));
    } catch (AbfsRestOperationException ex) {
      if (ex.getStatusCode() == HTTP_NOT_FOUND) {
        //TODO: call getListBlob if dev has switched on for implicit case.
        return new PathInformation(false, false);
      }
      throw ex;
    }
  }

  @Override
  boolean preChecks(final Path src, final Path adjustedQualifiedDst)
      throws IOException {
    Path nestedDstParent = adjustedQualifiedDst.getParent();
    LOG.debug("Check if the destination is subDirectory");
    if (nestedDstParent != null && nestedDstParent.toUri()
        .getPath()
        .indexOf(src.toUri().getPath()) == 0) {
      LOG.info("Rename src: {} dst: {} failed as dst is subDir of src",
          src, adjustedQualifiedDst);
      return false;
    }

    if (adjustedQualifiedDst.equals(new Path(dst, src.getName()))) {
      PathInformation pathInformation = getPathInformation(
          adjustedQualifiedDst);
      if (pathInformation.getPathExists()) {
        LOG.info(
            "Rename src: {} dst: {} failed as qualifiedDst already exists",
            src, adjustedQualifiedDst);
        throw new AbfsRestOperationException(
            HttpURLConnection.HTTP_CONFLICT,
            AzureServiceErrorCode.PATH_ALREADY_EXISTS.getErrorCode(), null,
            null);
      }
    }
    return true;
  }

  @Override
  boolean rename(final Path src, final Path dst) throws IOException {
    PathInformation pathInformation = getPathInformation(src);
    if (pathInformation.getIsDirectory()) {
      return listRecursiveAndTakeAction();
    }
    return renameInternal(src, dst);
  }

  private boolean renameInternal(final Path src, final Path dst)
      throws IOException {
    copyPath(src, dst, null);
    abfsClient.deletePath(src.toUri().getPath(), false, null, tracingContext, isNamespaceEnabled);
    return true;
  }

  private void copyPath(final Path src, final Path dst, final String leaseId) throws IOException {
    AbfsRestOperation copyPathOp = abfsClient.copyBlob(src.toUri().getPath(), dst.toUri().getPath(), leaseId, tracingContext);
    final String progress = copyPathOp.getResult()
        .getResponseHeader(X_MS_COPY_STATUS);
    if (COPY_STATUS_SUCCESS.equalsIgnoreCase(progress)) {
      return;
    }
    final String copyId = copyPathOp.getResult().getResponseHeader(X_MS_COPY_ID);
    final long pollWait = abfsClient.getAbfsConfiguration().getBlobCopyProgressPollWaitMillis();
    while (handleCopyInProgress(dst, tracingContext, copyId)
        == BlobCopyProgress.PENDING) {
      try {
        Thread.sleep(pollWait);
      } catch (Exception e) {

      }
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
  BlobCopyProgress handleCopyInProgress(final Path dstPath,
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


  @Override
  boolean takeAction(final Path path) throws IOException {
    return renameInternal(path,
        createDestinationPathForBlobPartOfRenameSrcDir(dst, path, src));
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
}
