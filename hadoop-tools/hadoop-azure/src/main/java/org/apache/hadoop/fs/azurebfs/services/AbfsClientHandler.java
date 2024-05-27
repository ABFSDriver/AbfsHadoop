package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.util.Preconditions;

/**
 * AbfsClientHandler is a class that provides a way to get the AbfsClient
 * based on the service type.
 */
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

  public AbfsDfsClient getDfsClient() {
    Preconditions.checkNotNull(dfsAbfsClient, "DFS client is not initialized");
    return (AbfsDfsClient) dfsAbfsClient;
  }

  public AbfsBlobClient getBlobClient() {
    Preconditions.checkNotNull(blobAbfsClient, "Blob client is not initialized");
    return (AbfsBlobClient) blobAbfsClient;
  }
}