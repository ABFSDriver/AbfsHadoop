package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.HttpURLConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;

public class BlobRenameHandler extends RenameHandler {

  private static final Logger LOG = LoggerFactory.getLogger(
      AzureBlobFileSystemStore.class);

  public BlobRenameHandler(final Path src, final Path dst, final AbfsClient abfsClient,
      final boolean isAtomicRenameKey,
      final String srcEtag,
      final AbfsCounters abfsCounters,
      final boolean isNamespaceEnabled,
      final AzureBlobFileSystemStore.GetFileStatusCallback getFileStatusCallback,
      final TracingContext tracingContext) {
    super(src, dst, abfsClient, isAtomicRenameKey, srcEtag, isNamespaceEnabled, abfsCounters,
        getFileStatusCallback,
        tracingContext);
  }

  @Override
  protected PathInformation getPathInformation(final Path path)
      throws IOException {
    try {
      AbfsRestOperation op = getFileStatusCallback.getFileStatus(path, null, isNamespaceEnabled, tracingContext);
      return new PathInformation(true,
          AbfsHttpConstants.DIRECTORY.equalsIgnoreCase(op.getResult()
              .getResponseHeader(HttpHeaderConfigurations.X_MS_RESOURCE_TYPE)));
    } catch (AbfsRestOperationException ex) {
      if(ex.getStatusCode() == HTTP_NOT_FOUND) {
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

    if(adjustedQualifiedDst.equals(new Path(dst, src.getName()))) {
      PathInformation pathInformation = getPathInformation(adjustedQualifiedDst);
      if(pathInformation.getPathExists()) {
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
    return false;
  }

  @Override
  boolean takeAction(final Path path) throws IOException {
    return rename(path, createDestinationPathForBlobPartOfRenameSrcDir(dst, path, src));
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
    return new Path(destinationPathStr + ROOT_PATH + srcBlobPropertyPathStr.substring(
        sourcePathStr.length()));
  }
}
