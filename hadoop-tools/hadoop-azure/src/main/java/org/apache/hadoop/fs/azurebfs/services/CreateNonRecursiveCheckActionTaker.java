package org.apache.hadoop.fs.azurebfs.services;

import java.io.Closeable;

import org.apache.hadoop.fs.Path;

/**
 * Maintains resources taken in pre-check for a createNonRecursive to happen, and
 * own the release of the resources after createNonRecursive is done.
 */
public class CreateNonRecursiveCheckActionTaker implements Closeable {
  private final AbfsClient abfsClient;
  private final Path path;
  private final AbfsLease abfsLease;

  public CreateNonRecursiveCheckActionTaker(AbfsClient abfsClient, Path path, AbfsLease abfsLease) {
    this.abfsClient = abfsClient;
    this.path = path;
    this.abfsLease = abfsLease;
  }

  /**
   * Release the resources taken in pre-check for a createNonRecursive to happen.
   */
  @Override
  public void close() {
   if(abfsLease != null) {
      abfsLease.free();
    }
  }
}
