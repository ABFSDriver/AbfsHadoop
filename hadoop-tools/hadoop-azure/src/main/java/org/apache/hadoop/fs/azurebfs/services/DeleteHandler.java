package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public abstract class DeleteHandler {
  final Path path;
  final boolean recursive;
  final boolean isNamespaceEnabled;
  final AbfsClient abfsClient;
  final AbfsPerfTracker abfsPerfTracker;
  final TracingContext tracingContext;


  public DeleteHandler(final Path path, final boolean recursive, final boolean isNamespaceEnabled,
      final AbfsClient abfsClient, final AbfsPerfTracker abfsPerfTracker,
      final TracingContext tracingContext) {
    this.path = path;
    this.recursive = recursive;
    this.isNamespaceEnabled = isNamespaceEnabled;
    this.abfsClient = abfsClient;
    this.abfsPerfTracker = abfsPerfTracker;
    this.tracingContext = tracingContext;
  }


  public final boolean execute() throws AzureBlobFileSystemException {
    if(path.isRoot()) {
      if(!recursive) {
        return false;
      }
      return deleteRoot();
    }
    return deleteInternal(path);
  }

  protected abstract boolean deleteInternal(final Path path) throws AzureBlobFileSystemException;

  protected abstract boolean deleteRoot() throws AzureBlobFileSystemException;
}
