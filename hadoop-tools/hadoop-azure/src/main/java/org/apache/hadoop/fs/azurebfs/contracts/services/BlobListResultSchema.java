package org.apache.hadoop.fs.azurebfs.contracts.services;

import java.util.ArrayList;
import java.util.List;

public class BlobListResultSchema implements ListResultSchema {

  private List<BlobListResultEntrySchema> paths;
  private String nextMarker;

  public BlobListResultSchema() {
    this.paths = new ArrayList<>();
    nextMarker = null;
  }

  @Override
  public List<BlobListResultEntrySchema> paths() {
    return paths;
  }

  @Override
  public ListResultSchema withPaths(final List<? extends ListResultEntrySchema> paths) {
    this.paths = (List<BlobListResultEntrySchema>) paths;
    return this;
  }

  public void addBlobListEntry(final BlobListResultEntrySchema blobListEntry) {
    this.paths.add(blobListEntry);
  }

  public String getNextMarker() {
    return nextMarker;
  }

  public void setNextMarker(String nextMarker) {
    this.nextMarker = nextMarker;
  }
}
