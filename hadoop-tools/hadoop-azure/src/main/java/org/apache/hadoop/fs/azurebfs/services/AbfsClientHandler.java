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

  private AbfsServiceType defaultServiceType;
  private AbfsServiceType ingressServiceType;
  private final AbfsDfsClient dfsAbfsClient;
  private final AbfsBlobClient blobAbfsClient;

  public AbfsClientHandler(final URL baseUrl,
      final SharedKeyCredentials sharedKeyCredentials,
      final AbfsConfiguration abfsConfiguration,
      final AccessTokenProvider tokenProvider,
      final EncryptionContextProvider encryptionContextProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    this.dfsAbfsClient = createDfsClient(baseUrl, sharedKeyCredentials,
        abfsConfiguration, tokenProvider, null, encryptionContextProvider,
        abfsClientContext);
    this.blobAbfsClient = createBlobClient(baseUrl, sharedKeyCredentials,
        abfsConfiguration, tokenProvider, null, encryptionContextProvider,
        abfsClientContext);
    initServiceType(abfsConfiguration);
  }

  public AbfsClientHandler(final URL baseUrl,
      final SharedKeyCredentials sharedKeyCredentials,
      final AbfsConfiguration abfsConfiguration,
      final SASTokenProvider sasTokenProvider,
      final EncryptionContextProvider encryptionContextProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    this.dfsAbfsClient = createDfsClient(baseUrl, sharedKeyCredentials,
        abfsConfiguration, null, sasTokenProvider, encryptionContextProvider,
        abfsClientContext);
    this.blobAbfsClient = createBlobClient(baseUrl, sharedKeyCredentials,
        abfsConfiguration, null, sasTokenProvider, encryptionContextProvider,
        abfsClientContext);
    initServiceType(abfsConfiguration);
  }

  private void initServiceType(final AbfsConfiguration abfsConfiguration) {
    this.defaultServiceType = abfsConfiguration.getFsConfiguredServiceType();
    this.ingressServiceType = abfsConfiguration.getIngressServiceType();
  }

  public AbfsClient getClient() {
    return getClient(defaultServiceType);
  }

  public AbfsClient getIngressClient() {
    return getClient(ingressServiceType);
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

  private AbfsDfsClient createDfsClient(final URL baseUrl,
      final SharedKeyCredentials creds,
      final AbfsConfiguration abfsConfiguration,
      final AccessTokenProvider tokenProvider,
      final SASTokenProvider sasTokenProvider,
      final EncryptionContextProvider encryptionContextProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    URL dfsUrl = changeUrlFromBlobToDfs(baseUrl);
    if (tokenProvider != null) {
      return new AbfsDfsClient(dfsUrl, creds, abfsConfiguration,
          tokenProvider, encryptionContextProvider,
          abfsClientContext);
    } else {
      return new AbfsDfsClient(dfsUrl, creds, abfsConfiguration,
          sasTokenProvider, encryptionContextProvider,
          abfsClientContext);
    }
  }

  private AbfsBlobClient createBlobClient(final URL baseUrl,
      final SharedKeyCredentials creds,
      final AbfsConfiguration abfsConfiguration,
      final AccessTokenProvider tokenProvider,
      final SASTokenProvider sasTokenProvider,
      final EncryptionContextProvider encryptionContextProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    URL blobUrl = changeUrlFromDfsToBlob(baseUrl);
    if (tokenProvider != null) {
      return new AbfsBlobClient(blobUrl, creds, abfsConfiguration,
          tokenProvider, encryptionContextProvider,
          abfsClientContext);
    } else {
      return new AbfsBlobClient(blobUrl, creds, abfsConfiguration,
          sasTokenProvider, encryptionContextProvider,
          abfsClientContext);
    }
  }
}
