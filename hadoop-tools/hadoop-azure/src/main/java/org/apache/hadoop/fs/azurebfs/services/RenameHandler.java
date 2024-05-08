package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public abstract class RenameHandler extends ListActionTaker {

  final Path src, dst;

  final AbfsClient abfsClient;

  final String srcEtag;

  final TracingContext tracingContext;

  final boolean isAtomicRenameKey;

  final AbfsCounters abfsCounters;

  final boolean isNamespaceEnabled;

  final AzureBlobFileSystemStore.GetFileStatusImpl getFileStatusImpl;

  public RenameHandler(final Path src,
      final Path dst,
      final AbfsClient abfsClient,
      final boolean isAtomicRenameKey,
      final String srcEtag,
      final boolean isNamespaceEnabled,
      final AbfsCounters abfsCounters,
      final AzureBlobFileSystemStore.GetFileStatusImpl getFileStatusCallback,
      final TracingContext tracingContext) {
    super(src, abfsClient, tracingContext);
    this.src = src;
    this.dst = dst;
    this.abfsClient = abfsClient;
    this.isAtomicRenameKey = isAtomicRenameKey;
    this.srcEtag = srcEtag;
    this.abfsCounters = abfsCounters;
    this.tracingContext = tracingContext;
    this.isNamespaceEnabled = isNamespaceEnabled;
    this.getFileStatusImpl = getFileStatusCallback;
  }

  public final boolean execute() throws IOException {
    Path srcParent = src.getParent();
    if (srcParent == null) {
      return false;
    }

    //Rename under same folder;
    if (srcParent.equals(dst)) {
      PathInformation srcPathInformation = getPathInformation(src);
      return srcPathInformation.getPathExists();
    }

    if (src.equals(dst)) {
      PathInformation srcPathInformation = getPathInformation(src);
      return srcPathInformation.getPathExists()
          && !srcPathInformation.getIsDirectory();
    }

    Path adjustedQualifiedDst = dst;
    if (!isNamespaceEnabled) {
      PathInformation pathInformation = getPathInformation(dst);
      if (pathInformation.getPathExists()) {
        if (!pathInformation.getIsDirectory()) {
          return src.equals(dst);
        } else {
          adjustedQualifiedDst = new Path(dst, src.getName());
        }
      }
    }

    if (!preChecks(src, adjustedQualifiedDst)) {
      return false;
    }

    return rename(src, adjustedQualifiedDst);
  }

  protected abstract PathInformation getPathInformation(final Path path)
      throws IOException;

  /**
   * Checks for the HDFS rename preconditions.
   *
   * @return true if the preconditions are met.
   * @throws IOException server failures on performing preconditions
   */
  abstract boolean preChecks(final Path src, final Path adjustedQualifiedDst)
      throws IOException;

  /**
   * Orchestrates the rename operation.
   *
   * @param src the source path
   * @param dst the destination path
   * @return true if the rename was successful.
   * @throws IOException server failures on performing rename
   */
  abstract boolean rename(final Path src, final Path dst) throws IOException;
}
