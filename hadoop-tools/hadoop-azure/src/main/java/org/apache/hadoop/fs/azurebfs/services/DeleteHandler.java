package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public abstract class DeleteHandler extends ListActionTaker {

  final Path path;

  final boolean recursive;

  final boolean isNamespaceEnabled;

  final AbfsClient abfsClient;

  final AbfsPerfTracker abfsPerfTracker;

  final TracingContext tracingContext;

  final AzureBlobFileSystemStore.GetFileStatusImpl getFileStatusImpl;

  public DeleteHandler(final Path path,
      final boolean recursive,
      final boolean isNamespaceEnabled,
      final AbfsClient abfsClient,
      final AbfsPerfTracker abfsPerfTracker,
      final AzureBlobFileSystemStore.GetFileStatusImpl getFileStatusImpl,
      final TracingContext tracingContext) {
    super(path, abfsClient, tracingContext);
    this.path = path;
    this.recursive = recursive;
    this.isNamespaceEnabled = isNamespaceEnabled;
    this.abfsClient = abfsClient;
    this.abfsPerfTracker = abfsPerfTracker;
    this.tracingContext = tracingContext;
    this.getFileStatusImpl = getFileStatusImpl;
  }


  public final boolean execute() throws IOException {
    return delete(path);
  }

  protected abstract boolean delete(final Path path) throws IOException;

  protected abstract boolean deleteInternal(final Path path)
      throws AzureBlobFileSystemException;

  @Override
  boolean takeAction(final Path path) throws IOException {
    return deleteInternal(path);
  }
}
