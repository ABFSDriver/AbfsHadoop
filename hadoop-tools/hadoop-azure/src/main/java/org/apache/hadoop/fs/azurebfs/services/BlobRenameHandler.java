package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

public class BlobRenameHandler extends RenameHandler {

  public BlobRenameHandler(final Path src, final Path dst, final AbfsClient abfsClient,
      final boolean isAtomicRenameKey,
      final String srcEtag,
      final AbfsCounters abfsCounters,
      final boolean isNamespaceEnabled,
      final AzureBlobFileSystemStore.GetFileStatusCallback getFileStatusCallback,
      final TracingContext tracingContext) {
    super(src, dst, abfsClient, isAtomicRenameKey, srcEtag, isNamespaceEnabled, abfsCounters,
        getFileStatusCallback,
        tracingContext);
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
        //TODO: call getListBlob if dev has switched on for implicit case.
        return new PathInformation(false, false);
      }
      throw ex;
    }
  }

  @Override
  boolean endpointBasedPreChecks() throws IOException {
    return false;
  }

  @Override
  boolean rename(final Path src, final Path dst) throws IOException {
    return false;
  }
}
