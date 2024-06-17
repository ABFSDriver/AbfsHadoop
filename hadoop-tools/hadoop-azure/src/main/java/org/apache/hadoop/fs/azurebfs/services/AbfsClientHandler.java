package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azurebfs.extensions.EncryptionContextProvider;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.util.Preconditions;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_BLOB_DOMAIN_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_DFS_DOMAIN_NAME;
import static org.apache.hadoop.fs.azurebfs.utils.UriUtils.changeUrlFromBlobToDfs;
import static org.apache.hadoop.fs.azurebfs.utils.UriUtils.changeUrlFromDfsToBlob;

/**
 * AbfsClientHandler is a class that provides a way to get the AbfsClient
 * based on the service type.
 */
public class AbfsClientHandler {

  private final AbfsServiceType defaultServiceType;

  private final AbfsDfsClient dfsAbfsClient;

  private final AbfsBlobClient blobAbfsClient;

  public AbfsClientHandler(final URL baseUrl,
      final SharedKeyCredentials sharedKeyCredentials,
      final AbfsConfiguration abfsConfiguration,
      final AccessTokenProvider tokenProvider,
      final EncryptionContextProvider encryptionContextProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    this.dfsAbfsClient = new AbfsDfsClient(changeUrlFromBlobToDfs(baseUrl),
        sharedKeyCredentials, abfsConfiguration, tokenProvider,
        encryptionContextProvider, abfsClientContext);
    this.blobAbfsClient = new AbfsBlobClient(changeUrlFromDfsToBlob(baseUrl),
        sharedKeyCredentials, abfsConfiguration, tokenProvider,
        encryptionContextProvider, abfsClientContext);
    this.defaultServiceType = abfsConfiguration.getFsConfiguredServiceType();
  }

  public AbfsClientHandler(final URL baseUrl,
      final SharedKeyCredentials sharedKeyCredentials,
      final AbfsConfiguration abfsConfiguration,
      final SASTokenProvider sasTokenProvider,
      final EncryptionContextProvider encryptionContextProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    this.dfsAbfsClient = new AbfsDfsClient(changeUrlFromBlobToDfs(baseUrl),
        sharedKeyCredentials, abfsConfiguration, sasTokenProvider,
        encryptionContextProvider, abfsClientContext);
    this.blobAbfsClient = new AbfsBlobClient(changeUrlFromDfsToBlob(baseUrl),
        sharedKeyCredentials, abfsConfiguration, sasTokenProvider,
        encryptionContextProvider, abfsClientContext);
    this.defaultServiceType = abfsConfiguration.getFsConfiguredServiceType();
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
