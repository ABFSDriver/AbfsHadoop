package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;

public class BlobDeleteHandler extends DeleteHandler {

  public BlobDeleteHandler(final Path path,
      final boolean recursive,
      final boolean isNamespaceEnabled,
      final AbfsClient abfsClient,
      final AbfsPerfTracker abfsPerfTracker,
      final TracingContext tracingContext) {
    super(path, recursive, isNamespaceEnabled, abfsClient, abfsPerfTracker,
        tracingContext);
  }

  @Override
  protected boolean deleteInternal(final Path path)
      throws AzureBlobFileSystemException {
    return false;
  }

  @Override
  protected boolean deleteRoot() throws AzureBlobFileSystemException {
    return deleteInternal(new Path(ROOT_PATH));
  }
}
