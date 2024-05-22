package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.permission.FsPermission;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.TRUE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_META_HDI_ISFOLDER;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.PATH_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.utils.PathUtils.getRelativePath;

public class BlobDeleteHandler extends ListActionTaker {

  private static final Logger LOG = LoggerFactory.getLogger(
      AzureBlobFileSystemStore.class);

  private final Path path;

  private final boolean recursive;

  private boolean nonRecursiveDeleteFailed = false;

  private final AbfsBlobClient abfsBlobClient;

  private final TracingContext tracingContext;

  private final AtomicInteger deleteCount = new AtomicInteger(0);


  public BlobDeleteHandler(final Path path,
      final boolean recursive,
      final AbfsBlobClient abfsBlobClient,
      final TracingContext tracingContext) {
    super(path, abfsBlobClient, tracingContext);
    this.path = path;
    this.recursive = recursive;
    this.abfsBlobClient = abfsBlobClient;
    this.tracingContext = tracingContext;
  }

  protected boolean deleteInternal(final Path path)
      throws AzureBlobFileSystemException {
    abfsClient.deleteBlobPath(path, null, tracingContext);
    deleteCount.incrementAndGet();
    return true;
  }

  public boolean execute() throws IOException {
    listRecursiveAndTakeAction();
    if (nonRecursiveDeleteFailed) {
      throw new IOException("Non-recursive delete of non-empty directory");
    }
    tracingContext.setOperatedBlobCount(deleteCount.get() + 1);
    boolean deleted;
    try {
      deleted = recursive ? safeDelete(path) : deleteInternal(path);
    } finally {
      tracingContext.setOperatedBlobCount(null);
    }
    if (deleteCount.get() == 0) {
      throw new AbfsRestOperationException(HTTP_NOT_FOUND,
          PATH_NOT_FOUND.getErrorCode(), PATH_NOT_FOUND.getErrorMessage(),
          new PathIOException(path.toString(), "Path not found"));
    }
    if (deleted && !path.isRoot() && !path.getParent().isRoot()) {
      HashMap<String, String> metadata = new HashMap<>();
      metadata.put(X_MS_META_HDI_ISFOLDER, TRUE);
      try {
        abfsClient.createPath(path.getParent().toUri().getPath(), false,
            false,
            metadata, null,
            tracingContext);
      } catch (AbfsRestOperationException ex) {
        if (ex.getStatusCode() != HTTP_CONFLICT) {
          throw ex;
        }
      }
    }
    return deleted;

  }

  private void checkParent() throws IOException {
    if (path.isRoot()) {
      return;
    }
    Path parentPath = path.getParent();
    if (parentPath.isRoot()) {
      return;
    }

    String srcPathStr = path.toUri().getPath();
    String srcParentPathSrc = parentPath.toUri().getPath();
    LOG.debug(String.format(
        "Creating Parent of Path %s : %s", srcPathStr, srcParentPathSrc));
    abfsClient.createPath(srcParentPathSrc, false, true,
        new AzureBlobFileSystemStore.Permissions(false,
            FsPermission.getDirDefault(),
            FsPermission.getUMask(
                abfsClient.getAbfsConfiguration().getRawConfiguration())),
        false, null, null,
        tracingContext);
    LOG.debug(String.format("Directory for parent of Path %s : %s created",
        srcPathStr, srcParentPathSrc));
  }

  @Override
  boolean takeAction(final Path path) throws IOException {
    if (!recursive) {
      nonRecursiveDeleteFailed = true;
      return false;
    }
    return safeDelete(path);
  }

  private boolean safeDelete(final Path path)
      throws AzureBlobFileSystemException {
    try {
      return deleteInternal(path);
    } catch (AbfsRestOperationException ex) {
      if (ex.getStatusCode() == HTTP_NOT_FOUND) {
        LOG.debug("Path {} not found", path);
        return true;
      }
      throw ex;
    }
  }
}
