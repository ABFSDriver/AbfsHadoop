package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.permission.FsPermission;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;
import static org.apache.hadoop.fs.azurebfs.utils.PathUtils.getRelativePath;

public class BlobDeleteHandler extends DeleteHandler {

  private static final Logger LOG = LoggerFactory.getLogger(
      AzureBlobFileSystemStore.class);

  private final AbfsConfiguration abfsConfiguration;

  public BlobDeleteHandler(final Path path,
      final boolean recursive,
      final boolean isNamespaceEnabled,
      final AbfsClient abfsClient,
      final AbfsPerfTracker abfsPerfTracker,
      final AzureBlobFileSystemStore.GetFileStatusImpl getFileStatusImpl,
      final AbfsConfiguration abfsConfiguration,
      final TracingContext tracingContext) {
    super(path, recursive, isNamespaceEnabled, abfsClient, abfsPerfTracker,
        getFileStatusImpl,
        tracingContext);
    this.abfsConfiguration = abfsConfiguration;
  }

  @Override
  protected boolean deleteInternal(final Path path)
      throws AzureBlobFileSystemException {
    String relativePath = getRelativePath(path);
    try {
      abfsClient.deletePath(relativePath, recursive,
          null, tracingContext, isNamespaceEnabled);
      return true;
    } catch (AbfsRestOperationException ex) {
      if (ex.getStatusCode() == HTTP_NOT_FOUND) {
        return true;
      }
      throw ex;
    }
  }

  @Override
  protected boolean delete(final Path path)
      throws IOException {
    PathInformation pathInformation = getPathInformation(path);
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
        new AzureBlobFileSystemStore.Permissions(isNamespaceEnabled,
            FsPermission.getDirDefault(),
            FsPermission.getUMask(abfsConfiguration.getRawConfiguration())),
        false, null, null,
        tracingContext);
    LOG.debug(String.format("Directory for parent of Path %s : %s created",
        srcPathStr, srcParentPathSrc));
  }

  private PathInformation getPathInformation(Path path) throws IOException {
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
}
