package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public abstract class DeleteHandler extends ListActionTaker{
  final Path path;
  final boolean recursive;
  final boolean isNamespaceEnabled;
  final AbfsClient abfsClient;
  final AbfsPerfTracker abfsPerfTracker;
  final TracingContext tracingContext;


  public DeleteHandler(final Path path, final boolean recursive, final boolean isNamespaceEnabled,
      final AbfsClient abfsClient, final AbfsPerfTracker abfsPerfTracker,
      final TracingContext tracingContext) {
    super(path, abfsClient, tracingContext);
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

  @Override
  boolean takeAction(final Path path) throws IOException {
    return deleteInternal(path);
  }
}
