package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.HttpURLConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;

public class BlobRenameHandler extends ListActionTaker {

  public static final Logger LOG = LoggerFactory.getLogger(AbfsClient.class);

  private final String source;

  private final String destination;
  private final String srcEtag;

  private final Path src, dst;

  private final AbfsBlobClient abfsBlobClient;

  private final TracingContext tracingContext;

  public BlobRenameHandler(final String source,
      final String destination,
      final AbfsBlobClient abfsBlobClient,
      final TracingContext tracingContext) {
    super(new Path(source), abfsBlobClient, tracingContext);
    this.source = source;
    src = new Path(source);
    dst = new Path(destination);
    this.destination = destination;
    this.abfsBlobClient = abfsBlobClient;
    this.tracingContext = tracingContext;
    this.srcEtag = null;
  }

  public BlobRenameHandler(final String src,
      final String dst,
      final AbfsClient abfsClient,
      final String srcEtag,
      final TracingContext tracingContext) {
    super(new Path(src), abfsClient, tracingContext);
    this.source = src;
    this.destination = dst;
    this.abfsBlobClient = (AbfsBlobClient) abfsClient;
    this.srcEtag = srcEtag;
    this.tracingContext = tracingContext;
    this.src = new Path(src);
    this.dst = new Path(dst);

  }

  public AbfsClientRenameResult execute() throws IOException {
    if (preCheck(src, dst)) {
      PathInformation pathInformation = getPathInformation(src);
      if (pathInformation.getIsDirectory()) {
        return new AbfsClientRenameResult(null, listRecursiveAndTakeAction(),
            false);
      }
      return new AbfsClientRenameResult(null, renameInternal(src, dst), false);
    } else {
      return new AbfsClientRenameResult(null, false, false);
    }
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
      PathInformation pathInformation = getPathInformation(
          dst);
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

  private PathInformation getPathInformation(final Path src)
      throws IOException {
    //implement it.
    return null;
  }

  @Override
  boolean takeAction(final Path path) throws IOException {
    return renameInternal(path,
        createDestinationPathForBlobPartOfRenameSrcDir(dst, path, src));
  }

  private boolean renameInternal(final Path path,
      final Path destinationPathForBlobPartOfRenameSrcDir) {
    return false;
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
