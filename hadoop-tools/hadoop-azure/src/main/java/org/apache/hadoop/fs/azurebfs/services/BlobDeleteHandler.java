package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.permission.FsPermission;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.utils.PathUtils.getRelativePath;

public class BlobDeleteHandler extends ListActionTaker {

  private static final Logger LOG = LoggerFactory.getLogger(
      AzureBlobFileSystemStore.class);

  private final Path path;
  private final boolean recursive;
  private final AbfsBlobClient abfsBlobClient;
  private final TracingContext tracingContext;


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
    String relativePath = getRelativePath(path);
    try {
      abfsClient.deletePath(relativePath, recursive,
          null, tracingContext, false);
      return true;
    } catch (AbfsRestOperationException ex) {
      if (ex.getStatusCode() == HTTP_NOT_FOUND) {
        return true;
      }
      throw ex;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean execute() throws IOException {
    PathInformation pathInformation = abfsBlobClient.getPathInformation(path, tracingContext);
    if (pathInformation.getIsDirectory()) {
      boolean result = listRecursiveAndTakeAction();
      checkParent();
      return result;
    }
    boolean result = deleteInternal(path);
    checkParent();
    return result;
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
            FsPermission.getUMask(abfsClient.getAbfsConfiguration().getRawConfiguration())),
        false, null, null,
        tracingContext);
    LOG.debug(String.format("Directory for parent of Path %s : %s created",
        srcPathStr, srcParentPathSrc));
  }

  @Override
  boolean takeAction(final Path path) throws IOException {
    return deleteInternal(path);
  }
}
