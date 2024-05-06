package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public abstract class RenameHandler {
  final Path src, dst;
  final AbfsClient abfsClient;
  final String srcEtag;
  final TracingContext tracingContext;
  final boolean isAtomicRenameKey;
  final AbfsCounters abfsCounters;
  public RenameHandler(final Path src, final Path dst, final AbfsClient abfsClient,
      final boolean isAtomicRenameKey,
      final String srcEtag,
      final AbfsCounters abfsCounters, final TracingContext tracingContext) {
    this.src = src;
    this.dst = dst;
    this.abfsClient = abfsClient;
    this.isAtomicRenameKey = isAtomicRenameKey;
    this.srcEtag = srcEtag;
    this.abfsCounters = abfsCounters;
    this.tracingContext = tracingContext;
  }

  public boolean execute() throws IOException {
    if(!preChecks()) {
      return false;
    }
    Path adjustedQualifiedDst = getAdjustedQualifiedDst();

    return rename(src, adjustedQualifiedDst);
  }

  /**
   * Checks for the HDFS rename preconditions.
   *
   * @return true if the preconditions are met.
   * @throws IOException server failures on performing preconditions
   */
  abstract boolean preChecks() throws IOException;

  /**
   * Orchestrates the rename operation.
   *
   * @param src the source path
   * @param dst the destination path
   * @return true if the rename was successful.
   * @throws IOException server failures on performing rename
   */
  abstract boolean rename(final Path src, final Path dst) throws IOException;

  /**
   * Creates destination path in accordance to HDFS contract.
   *
   * @return the adjusted qualified destination path.
   */
  abstract Path getAdjustedQualifiedDst();
  
}
