package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.util.Preconditions;

/**
 * AbfsClientHandler is a class that provides a way to get the AbfsClient
 * based on the service type.
 */
public class AbfsClientHandler {

  private final AbfsServiceType defaultServiceType;

  private final AbfsDfsClient dfsAbfsClient;

  private final AbfsBlobClient blobAbfsClient;

  public AbfsClientHandler(AbfsServiceType defaultServiceType,
      AbfsDfsClient dfsAbfsClient, AbfsBlobClient blobAbfsClient) {
    Preconditions.checkNotNull(dfsAbfsClient,
        "DFS client is not initialized");
    Preconditions.checkNotNull(blobAbfsClient,
        "Blob client is not initialized");
    this.blobAbfsClient = blobAbfsClient;
    this.dfsAbfsClient = dfsAbfsClient;
    this.defaultServiceType = defaultServiceType;
  }

  public AbfsClient getClient() {
    return getClient(defaultServiceType);
  }

  public AbfsClient getClient(AbfsServiceType serviceType) {
    return serviceType == AbfsServiceType.DFS ? dfsAbfsClient : blobAbfsClient;
  }

  public AbfsDfsClient getDfsClient() {
    return dfsAbfsClient;
  }

  public AbfsBlobClient getBlobClient() {
    return blobAbfsClient;
  }
}
