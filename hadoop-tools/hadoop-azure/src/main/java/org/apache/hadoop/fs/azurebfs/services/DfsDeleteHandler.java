package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.utils.PathUtils.getRelativePath;

public class DfsDeleteHandler extends DeleteHandler {

  private static final Logger LOG = LoggerFactory.getLogger(
      AzureBlobFileSystemStore.class);

  public DfsDeleteHandler(final Path path,
      final boolean recursive,
      final boolean isNamespaceEnabled,
      final AbfsClient abfsClient,
      final AbfsPerfTracker abfsPerfTracker,
      final AzureBlobFileSystemStore.GetFileStatusImpl getFileStatusImpl,
      final TracingContext tracingContext) {
    super(path, recursive, isNamespaceEnabled, abfsClient, abfsPerfTracker, getFileStatusImpl, tracingContext);
  }

  @Override
  protected boolean deleteInternal(final Path path) throws
      AzureBlobFileSystemException {
    final Instant startAggregate = abfsPerfTracker.getLatencyInstant();
    long countAggregate = 0;
    boolean shouldContinue = true;

    LOG.debug("delete filesystem: {} path: {} recursive: {}",
        abfsClient.getFileSystem(),
        path,
        String.valueOf(recursive));

    String continuation = null;

    String relativePath = getRelativePath(path);

    do {
      try (AbfsPerfInfo perfInfo = new AbfsPerfInfo(abfsPerfTracker, "delete", "deletePath")) {
        AbfsRestOperation op = abfsClient.deletePath(relativePath, recursive,
            continuation, tracingContext, isNamespaceEnabled);
        perfInfo.registerResult(op.getResult());
        continuation = op.getResult().getResponseHeader(
            HttpHeaderConfigurations.X_MS_CONTINUATION);
        perfInfo.registerSuccess(true);
        countAggregate++;
        shouldContinue = continuation != null && !continuation.isEmpty();

        if (!shouldContinue) {
          perfInfo.registerAggregates(startAggregate, countAggregate);
        }
      }
    } while (shouldContinue);

    return true;
  }

  @Override
  protected boolean delete(final Path path)
      throws AzureBlobFileSystemException {
    return deleteInternal(path);
  }
}
