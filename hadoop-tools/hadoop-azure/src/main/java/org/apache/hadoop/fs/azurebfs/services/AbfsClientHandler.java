package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.util.Preconditions;

public class AbfsClientHandler {
  private AbfsServiceType defaultServiceType;
  private AbfsClient dfsAbfsClient;
  private AbfsClient blobAbfsClient;

  public AbfsClientHandler(AbfsServiceType defaultServiceType,
      AbfsClient dfsAbfsClient, AbfsClient blobAbfsClient) {
    this.blobAbfsClient = blobAbfsClient;
    this.dfsAbfsClient = dfsAbfsClient;
    this.defaultServiceType = defaultServiceType;
  }

  public AbfsClient getClient() {
    if (defaultServiceType == AbfsServiceType.DFS) {
      Preconditions.checkNotNull(dfsAbfsClient, "DFS client is not initialized");
      return dfsAbfsClient;
    } else {
      Preconditions.checkNotNull(dfsAbfsClient, "Blob client is not initialized");
      return blobAbfsClient;
    }
  }

  public AbfsClient getClient(AbfsServiceType serviceType) {
    if (serviceType == AbfsServiceType.DFS) {
      Preconditions.checkNotNull(dfsAbfsClient, "DFS client is not initialized");
      return dfsAbfsClient;
    } else {
      Preconditions.checkNotNull(dfsAbfsClient, "Blob client is not initialized");
      return blobAbfsClient;
    }
  }
}
