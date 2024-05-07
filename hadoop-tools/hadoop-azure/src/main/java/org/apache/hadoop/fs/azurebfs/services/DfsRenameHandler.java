package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.METADATA_INCOMPLETE_RENAME_FAILURES;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.RENAME_RECOVERY;
import static org.apache.hadoop.fs.azurebfs.utils.PathUtils.getRelativePath;

public class DfsRenameHandler extends RenameHandler {
  private static final Logger LOG = LoggerFactory.getLogger(
      AzureBlobFileSystemStore.class);

  private final AbfsPerfTracker abfsPerfTracker;
  private boolean isNamespaceEnabled;

  public DfsRenameHandler(final Path src, final Path dst,
      final AbfsClient abfsClient, final boolean isAtomicRenameKey,
      final String srcEtag,
      final TracingContext tracingContext, final AbfsPerfTracker abfsPerfTracker,
      final AbfsCounters abfsCounters,
      final AzureBlobFileSystemStore.GetFileStatusCallback getFileStatusCallback,
      final boolean isNamespaceEnabled) {
    super(src, dst, abfsClient, isAtomicRenameKey, srcEtag, isNamespaceEnabled, abfsCounters,
        getFileStatusCallback,
        tracingContext);
    this.abfsPerfTracker = abfsPerfTracker;
    this.isNamespaceEnabled = isNamespaceEnabled;
  }

  /**
   * No additional checks for DFS endpoint, server on the dfs endpoint would do
   * all the relevant checks.
   */
  @Override
  boolean preChecks(final Path src, final Path adjustedQualifiedDst)
      throws IOException {
    return true;
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
        return new PathInformation(false, false);
      }
      throw ex;
    }
  }

  @Override
  boolean rename(final Path src, final Path dst) throws IOException {
    final Instant startAggregate = abfsPerfTracker.getLatencyInstant();
    long countAggregate = 0;
    boolean shouldContinue;

    if(isAtomicRenameKey) {
      LOG.warn("The atomic rename feature is not supported by the ABFS scheme; however rename,"
          +" create and delete operations are atomic if Namespace is enabled for your Azure Storage account.");
    }

    LOG.debug("renameAsync filesystem: {} source: {} destination: {}",
        abfsClient.getFileSystem(),
        src,
        dst);

    String continuation = null;

    String sourceRelativePath = getRelativePath(src);
    String destinationRelativePath = getRelativePath(dst);
    // was any operation recovered from?
    boolean recovered = false;

    do {
      try (AbfsPerfInfo perfInfo = startTracking("rename", "renamePath")) {
        final AbfsClientRenameResult abfsClientRenameResult =
            abfsClient.renamePath(sourceRelativePath, destinationRelativePath,
                continuation, tracingContext, srcEtag, false,
                isNamespaceEnabled);

        AbfsRestOperation op = abfsClientRenameResult.getOp();
        perfInfo.registerResult(op.getResult());
        continuation = op.getResult().getResponseHeader(
            HttpHeaderConfigurations.X_MS_CONTINUATION);
        perfInfo.registerSuccess(true);
        countAggregate++;
        shouldContinue = continuation != null && !continuation.isEmpty();
        // update the recovery flag.
        recovered |= abfsClientRenameResult.isRenameRecovered();
        populateRenameRecoveryStatistics(abfsClientRenameResult);
        if (!shouldContinue) {
          perfInfo.registerAggregates(startAggregate, countAggregate);
        }
      }
    } while (shouldContinue);
    return recovered;
  }

  private AbfsPerfInfo startTracking(String callerName, String calleeName) {
    return new AbfsPerfInfo(abfsPerfTracker, callerName, calleeName);
  }

  /**
   * Increment rename recovery based counters in IOStatistics.
   *
   * @param abfsClientRenameResult Result of an ABFS rename operation.
   */
  private void populateRenameRecoveryStatistics(
      AbfsClientRenameResult abfsClientRenameResult) {
    if (abfsClientRenameResult.isRenameRecovered()) {
      abfsCounters.incrementCounter(RENAME_RECOVERY, 1);
    }
    if (abfsClientRenameResult.isIncompleteMetadataState()) {
      abfsCounters.incrementCounter(METADATA_INCOMPLETE_RENAME_FAILURES, 1);
    }
  }
}
