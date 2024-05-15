package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.HttpURLConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_COPY_STATUS;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_COPY_STATUS_DESCRIPTION;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.COPY_BLOB_ABORTED;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.COPY_BLOB_FAILED;

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
    if (preCheck(src, dst)) {
      PathInformation pathInformation = abfsBlobClient.getPathInformation(src,
          tracingContext);
      RenameAtomicity renameAtomicity = null;
      if (isAtomicRename) {
        srcAbfsLease = takeLease(src);
        srcLeaseId = srcAbfsLease.getLeaseID();
        if (!isAtomicRenameRecovery) {
          renameAtomicity = new RenameAtomicity(
              new Path(src.getParent(), src.getName() + RenameAtomicity.SUFFIX),
              abfsBlobClient.getCreateCallback(),
              abfsBlobClient.getReadCallback(), tracingContext, false,
              abfsBlobClient);
          renameAtomicity.preRename();
        }
      }
      final boolean result;
      if (pathInformation.getIsDirectory()) {
        result = listRecursiveAndTakeAction() && renameInternal(src, dst);
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

  private AbfsLease takeLease(final Path src)
      throws AzureBlobFileSystemException {
    return new AbfsLease(abfsBlobClient, src.toUri().getPath(),
        abfsBlobClient.getAbfsConfiguration().getAtomicRenameLeaseDuration(),
        tracingContext);
  }

  private boolean preCheck(final Path src, final Path dst) throws IOException {
    Path nestedDstParent = dst.getParent();
    LOG.debug("Check if the destination is subDirectory");
    if (nestedDstParent != null && nestedDstParent.toUri()
        .getPath()
        .indexOf(src.toUri().getPath()) == 0) {
      LOG.info("Rename src: {} dst: {} failed as dst is subDir of src",
          src, dst);
      return false;
    }

    if (dst.equals(new Path(dst, src.getName()))) {
      PathInformation pathInformation = abfsBlobClient.getPathInformation(dst,
          tracingContext);
      if (pathInformation.getPathExists()) {
        LOG.info(
            "Rename src: {} dst: {} failed as qualifiedDst already exists",
            src, dst);
        throw new AbfsRestOperationException(
            HttpURLConnection.HTTP_CONFLICT,
            AzureServiceErrorCode.PATH_ALREADY_EXISTS.getErrorCode(), null,
            null);
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
        abfsLease = takeLease(path);
        leaseId = abfsLease.getLeaseID();
      }
    } else {
      leaseId = null;
    }
    copyPath(path, destinationPathForBlobPartOfRenameSrcDir, leaseId);
    abfsClient.deleteBlobPath(path, leaseId, tracingContext);
    if (abfsLease != null) {
      abfsLease.free();
    }
    return true;
  }

  private void copyPath(final Path src, final Path dst, final String leaseId)
      throws IOException {
    AbfsRestOperation copyPathOp = abfsClient.copyBlob(src, dst, leaseId,
        tracingContext);
    final String progress = copyPathOp.getResult()
        .getResponseHeader(X_MS_COPY_STATUS);
    if (COPY_STATUS_SUCCESS.equalsIgnoreCase(progress)) {
      return;
    }
    final String copyId = copyPathOp.getResult()
        .getResponseHeader(X_MS_COPY_ID);
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
