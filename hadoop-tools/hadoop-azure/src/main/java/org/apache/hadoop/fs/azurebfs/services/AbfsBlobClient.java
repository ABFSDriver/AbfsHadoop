/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidAbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListResultSchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListXmlParser;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.StorageErrorResponseSchema;
import org.apache.hadoop.fs.azurebfs.extensions.EncryptionContextProvider;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.*;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.*;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.*;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.PATH_EXISTS;

/**
 * AbfsClient interacting with Blob endpoint.
 */
public class AbfsBlobClient extends AbfsClient implements Closeable {

  public AbfsBlobClient(final URL baseUrl,
      final SharedKeyCredentials sharedKeyCredentials,
      final AbfsConfiguration abfsConfiguration,
      final AccessTokenProvider tokenProvider,
      final EncryptionContextProvider encryptionContextProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    super(baseUrl, sharedKeyCredentials, abfsConfiguration, tokenProvider,
        encryptionContextProvider, abfsClientContext);
  }

  public AbfsBlobClient(final URL baseUrl,
      final SharedKeyCredentials sharedKeyCredentials,
      final AbfsConfiguration abfsConfiguration,
      final SASTokenProvider sasTokenProvider,
      final EncryptionContextProvider encryptionContextProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    super(baseUrl, sharedKeyCredentials, abfsConfiguration, sasTokenProvider,
        encryptionContextProvider, abfsClientContext);
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  public List<AbfsHttpHeader> createDefaultHeaders() {
    return this.createDefaultHeaders(this.xMsVersion);
  }

  /**
   * Create request headers for Rest Operation using the specified API version.
   * Blob Endpoint API responses are in JSON/XML format.
   * @param xMsVersion
   * @return default request headers
   */
  @Override
  public List<AbfsHttpHeader> createDefaultHeaders(AbfsHttpConstants.ApiVersion xMsVersion) {
    List<AbfsHttpHeader> requestHeaders = super.createCommonHeaders(xMsVersion);
    requestHeaders.add(new AbfsHttpHeader(ACCEPT, APPLICATION_JSON
        + COMMA + SINGLE_WHITE_SPACE + APPLICATION_OCTET_STREAM
        + COMMA + SINGLE_WHITE_SPACE + APPLICATION_XML));
    return requestHeaders;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/create-container></a>.
   * Creates a storage container as filesystem root.
   * @param tracingContext
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation createFilesystem(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = new AbfsUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESTYPE, CONTAINER);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.CreateContainer,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/set-container-metadata></a>.
   * Sets user-defined properties of the filesystem.
   * @param properties comma separated list of metadata key-value pairs.
   * @param tracingContext
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation setFilesystemProperties(final Hashtable<String, String> properties,
      TracingContext tracingContext) throws AzureBlobFileSystemException  {
    List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    List<AbfsHttpHeader> metadataRequestHeaders = getMetadataHeadersList(properties);
    requestHeaders.addAll(metadataRequestHeaders);

    AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESTYPE, CONTAINER);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, METADATA);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.SetContainerMetadata,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/get-container-properties></a>.
   * Gets all the properties of the filesystem.
   * @param tracingContext
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   * */
  @Override
  public AbfsRestOperation getFilesystemProperties(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESTYPE, CONTAINER);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.GetContainerProperties,
        HTTP_METHOD_HEAD, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/delete-container></a>.
   * Deletes the Container acting as current filesystem.
   * @param tracingContext
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation deleteFilesystem(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESTYPE, CONTAINER);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.DeleteContainer,
        HTTP_METHOD_DELETE, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob></a>.
   * Creates a file or directory(marker file) at specified path.
   * @param path of the directory to be created.
   * @param tracingContext
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation createPath(final String path,
      final boolean isFile,
      final boolean overwrite,
      final AzureBlobFileSystemStore.Permissions permissions,
      final boolean isAppendBlob,
      final String eTag,
      final ContextEncryptionAdapter contextEncryptionAdapter,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    if (isFile) {
      AbfsHttpOperation op1Result = null;
      try {
        op1Result = getPathStatus(path, false, tracingContext,
            null).getResult();
      } catch (AbfsRestOperationException ex) {
        if (ex.getStatusCode() == HTTP_NOT_FOUND) {
          LOG.debug("No explicit directory/path found: {}", path);
        } else {
          throw ex;
        }
      }
      if (op1Result != null && checkIsDir(op1Result)) {
        throw new AbfsRestOperationException(HTTP_CONFLICT,
            AzureServiceErrorCode.PATH_CONFLICT.getErrorCode(),
            PATH_EXISTS,
            null);
      }
    }
    requestHeaders.add(new AbfsHttpHeader(CONTENT_LENGTH, ZERO));
    if (isAppendBlob) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_BLOB_TYPE, APPEND_BLOB_TYPE));
    } else {
      requestHeaders.add(new AbfsHttpHeader(X_MS_BLOB_TYPE, BLOCK_BLOB_TYPE));
    }
    if (!overwrite) {
      requestHeaders.add(new AbfsHttpHeader(IF_NONE_MATCH, AbfsHttpConstants.STAR));
    }
    if (eTag != null && !eTag.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.IF_MATCH, eTag));
    }
    if (!isFile) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_META_HDI_ISFOLDER, TRUE));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    appendSASTokenToQuery(path, SASTokenProvider.CREATE_FILE_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.PutBlob,
        HTTP_METHOD_PUT, url, requestHeaders);
    try {
      op.execute(tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      // If we have no HTTP response, throw the original exception.
      if (!op.hasResult()) {
        throw ex;
      }
      if (!isFile && op.getResult().getStatusCode() == HTTP_CONFLICT) {
        // This ensures that we don't throw ex only for existing directory but if a blob exists we throw exception.
        final AbfsHttpOperation opResult = this.getPathStatus(
            path, true, tracingContext, null).getResult();
        if (checkIsDir(opResult)) {
          return op;
        }
      }
      throw ex;
    }
    return op;
  }

  /**
   *  Creates marker blobs for the parent directories of the specified path.
   *
   * @param path The path for which parent directories need to be created.
   * @param overwrite A flag indicating whether existing directories should be overwritten.
   * @param permissions The permissions to be set for the created directories.
   * @param isAppendBlob A flag indicating whether the created blob should be of type APPEND_BLOB.
   * @param eTag The eTag to be matched for conditional requests.
   * @param contextEncryptionAdapter The encryption adapter for context encryption.
   * @param tracingContext The tracing context for the operation.
   * @throws AzureBlobFileSystemException If the creation of any parent directory fails.
   */
  public void createMarkerBlobs(final Path path,
      final boolean overwrite,
      final AzureBlobFileSystemStore.Permissions permissions,
      final boolean isAppendBlob,
      final String eTag,
      final ContextEncryptionAdapter contextEncryptionAdapter,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    ArrayList<Path> keysToCreateAsFolder = new ArrayList<>();
    checkParentChainForFile(path, tracingContext,
        keysToCreateAsFolder);
    for (Path pathToCreate : keysToCreateAsFolder) {
      createPath(pathToCreate.toUri().getPath(), false, overwrite, permissions,
          isAppendBlob, eTag, contextEncryptionAdapter, tracingContext);
    }
  }

  /**
   * Checks for the entire parent hierarchy and returns if any directory exists and
   * throws an exception if any file exists.
   * @param path path to check the hierarchy for.
   * @param tracingContext the tracingcontext.
   */
  private void checkParentChainForFile(Path path, TracingContext tracingContext,
      List<Path> keysToCreateAsFolder) throws AzureBlobFileSystemException {
    AbfsHttpOperation opResult = null;
    try {
      opResult = getPathStatus(path.toUri().getPath(), true,
          tracingContext, null).getResult();
    } catch (AbfsRestOperationException ex) {
      if (ex.getStatusCode() == HTTP_NOT_FOUND) {
        LOG.debug("No explicit directory/path found: {}", path);
      } else {
        throw ex;
      }
    }
    boolean isDirectory = opResult != null && checkIsDir(opResult);
    if (opResult != null && !isDirectory) {
      throw new AbfsRestOperationException(HTTP_CONFLICT,
          AzureServiceErrorCode.PATH_CONFLICT.getErrorCode(),
          PATH_EXISTS,
          null);
    }
    if (isDirectory) {
      return;
    }
    Path current = path.getParent();
    while (current != null && !current.isRoot()) {
      try {
        opResult = getPathStatus(current.toUri().getPath(), true,
            tracingContext, null).getResult();
      } catch (AbfsRestOperationException ex) {
        if (ex.getStatusCode() == HTTP_NOT_FOUND) {
          LOG.debug("No explicit directory/path found: {}", path);
        } else {
          throw ex;
        }
      }
      isDirectory = opResult != null && checkIsDir(opResult);
      if (opResult != null && !isDirectory) {
        throw new AbfsRestOperationException(HTTP_CONFLICT,
            AzureServiceErrorCode.PATH_CONFLICT.getErrorCode(),
            PATH_EXISTS,
            null);
      }
      if (isDirectory) {
        return;
      }
      keysToCreateAsFolder.add(current);
      current = current.getParent();
    }
  }

  /**
   * Appends a block to an append blob.
   * API reference: <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/append-block"></a>
   *
   * @param path               the path of the append blob.
   * @param data               the data to be appended.
   * @param tracingContext     the tracing context.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation appendBlock(final String path,
      AppendRequestParameters requestParameters,
      final byte[] data,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    requestHeaders.add(new AbfsHttpHeader(CONTENT_LENGTH, String.valueOf(data.length)));
    requestHeaders.add(new AbfsHttpHeader(X_MS_BLOB_TYPE, APPEND_BLOB_TYPE));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, APPEND_BLOCK);
    String sasTokenForReuse = appendSASTokenToQuery(path, SASTokenProvider.APPEND_BLOCK_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.AppendBlock,
        HTTP_METHOD_PUT,
        url,
        requestHeaders,
        data,
        requestParameters.getoffset(),
        requestParameters.getLength(),
        sasTokenForReuse);

    try {
      op.execute(tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      // If we have no HTTP response, throw the original exception.
      if (!op.hasResult()) {
        throw ex;
      }
      throw ex;
    }
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/list-blobs></a>.
   * @param relativePath to return only blobs with names that begin with the specified prefix.
   * @param recursive to return all blobs in the path, including those in subdirectories.
   * @param listMaxResults maximum number of blobs to return.
   * @param continuation marker to specify the continuation token.
   * @param tracingContext
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation or response parsing fails.
   */
  @Override
  public AbfsRestOperation listPath(final String relativePath, final boolean recursive,
      final int listMaxResults, final String continuation, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESTYPE, CONTAINER);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, LIST);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_INCLUDE, METADATA);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_PREFIX, getDirectoryQueryParameter(relativePath));
    if (!recursive) {
      abfsUriQueryBuilder.addQuery(QUERY_PARAM_DELIMITER, FORWARD_SLASH);
    }
    if (continuation != null) {
      abfsUriQueryBuilder.addQuery(QUERY_PARAM_MARKER, continuation);
    }
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_MAX_RESULTS, String.valueOf(listMaxResults));
    appendSASTokenToQuery(null, SASTokenProvider.LIST_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.ListBlobs,
        HTTP_METHOD_GET,
        url,
        requestHeaders);

    op.execute(tracingContext);
    if (op.getResult().getListResultSchema().paths().isEmpty()) {
      // If the list operation returns no paths, we need to check if the path is a file.
      // If it is a file, we need to return the file in the list.
      // If it is a non-existing path, we need to throw a FileNotFoundException.
      if (relativePath.equals(ROOT_PATH)) {
        // Root Always exists as directory. It can be a empty listing.
        return op;
      }
      AbfsRestOperation pathStatus = this.getPathStatus(relativePath, tracingContext, null, false);
      BlobListResultSchema listResultSchema = getListResultSchemaFromPathStatus(relativePath, pathStatus);
      AbfsRestOperation listOp = getAbfsRestOperation(
          AbfsRestOperationType.ListBlobs,
          HTTP_METHOD_GET,
          url,
          requestHeaders);
      listOp.hardSetGetListStatusResult(HTTP_OK, listResultSchema);
      return listOp;
    }
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/lease-blob></a>.
   * @param path on which lease has to be acquired.
   * @param duration for which lease has to be acquired.
   * @param tracingContext
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation acquireLease(final String path, final int duration,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, ACQUIRE_LEASE_ACTION));
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_DURATION, Integer.toString(duration)));
    requestHeaders.add(new AbfsHttpHeader(X_MS_PROPOSED_LEASE_ID, UUID.randomUUID().toString()));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, LEASE);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.LeaseBlob,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/lease-blob></a>.
   * @param path on which lease has to be renewed.
   * @param leaseId of the lease to be renewed.
   * @param tracingContext
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation renewLease(final String path, final String leaseId,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, RENEW_LEASE_ACTION));
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, leaseId));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, LEASE);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.LeaseBlob,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/lease-blob></a>.
   * @param path on which lease has to be released.
   * @param leaseId of the lease to be released.
   * @param tracingContext
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation releaseLease(final String path, final String leaseId,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, RELEASE_LEASE_ACTION));
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, leaseId));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, LEASE);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.LeaseBlob,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/lease-blob></a>.
   * @param path on which lease has to be broken.
   * @param tracingContext
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation breakLease(final String path,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, BREAK_LEASE_ACTION));
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_BREAK_PERIOD, DEFAULT_LEASE_BREAK_PERIOD));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, LEASE);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.LeaseBlob,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API.
   * @param source                    path to source file
   * @param destination               destination of rename.
   * @param continuation              continuation.
   * @param tracingContext            trace context
   * @param sourceEtag                etag of source file. may be null or empty
   * @param isMetadataIncompleteState was there a rename failure due to
   *                                  incomplete metadata state?
   * @param isNamespaceEnabled        whether namespace enabled account or not
   * @return
   * @throws IOException
   */
  @Override
  public AbfsClientRenameResult renamePath(final String source,
      final String destination,
      final String continuation,
      final TracingContext tracingContext,
      final String sourceEtag,
      final boolean isMetadataIncompleteState,
      final boolean isNamespaceEnabled) throws IOException {
    // Todo: To be implemented as part of rename-delete over blob endpoint work.
    // This should redirect to rename handler to be implemented.
    throw new NotImplementedException("Rename operation on Blob endpoint will be implemented in future.");
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob></a>.
   * Read the contents of the file at specified path
   * @param path of the file to be read.
   * @param position in the file from where data has to be read.
   * @param buffer to store the data read.
   * @param bufferOffset offset in the buffer to start storing the data.
   * @param bufferLength length of data to be read.
   * @param eTag to specify conditional headers.
   * @param cachedSasToken to be used for the authenticating operation.
   * @param contextEncryptionAdapter to provide encryption context.
   * @param tracingContext
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation read(final String path,
      final long position,
      final byte[] buffer,
      final int bufferOffset,
      final int bufferLength,
      final String eTag,
      final String cachedSasToken,
      final ContextEncryptionAdapter contextEncryptionAdapter,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    AbfsHttpHeader rangeHeader = new AbfsHttpHeader(RANGE, String.format(
        "bytes=%d-%d", position, position + bufferLength - 1));
    requestHeaders.add(rangeHeader);
    requestHeaders.add(new AbfsHttpHeader(IF_MATCH, eTag));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    String sasTokenForReuse = appendSASTokenToQuery(path, SASTokenProvider.READ_OPERATION,
        abfsUriQueryBuilder, cachedSasToken);

    URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.GetBlob,
        HTTP_METHOD_GET, url, requestHeaders,
        buffer, bufferOffset, bufferLength,
        sasTokenForReuse);
    op.execute(tracingContext);

    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/put-block></a>.
   * Uploads data to be appended to a file.
   * @param path to which data has to be appended.
   * @param buffer containing data to be appended.
   * @param reqParams containing parameters for append operation like offset, length etc.
   * @param cachedSasToken to be used for the authenticating operation.
   * @param contextEncryptionAdapter to provide encryption context.
   * @param tracingContext
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation append(final String path,
      final byte[] buffer,
      final AppendRequestParameters reqParams,
      final String cachedSasToken,
      final ContextEncryptionAdapter contextEncryptionAdapter,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    requestHeaders.add(new AbfsHttpHeader(CONTENT_LENGTH, String.valueOf(buffer.length)));
    requestHeaders.add(new AbfsHttpHeader(IF_MATCH, reqParams.getETag()));
    if (reqParams.getLeaseId() != null) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, reqParams.getLeaseId()));
    }
    if (reqParams.isExpectHeaderEnabled()) {
      requestHeaders.add(new AbfsHttpHeader(EXPECT, HUNDRED_CONTINUE));
    }

    if (reqParams.isRetryDueToExpect()) {
      String userAgentRetry = userAgent;
      userAgentRetry = userAgentRetry.replace(HUNDRED_CONTINUE_USER_AGENT, EMPTY_STRING);
      requestHeaders.removeIf(header -> header.getName().equalsIgnoreCase(USER_AGENT));
      requestHeaders.add(new AbfsHttpHeader(USER_AGENT, userAgentRetry));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, BLOCK);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_BLOCKID, reqParams.getBlockId());

    String sasTokenForReuse = appendSASTokenToQuery(path, SASTokenProvider.WRITE_OPERATION,
        abfsUriQueryBuilder, cachedSasToken);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.PutBlock,
        HTTP_METHOD_PUT, url, requestHeaders,
        buffer, reqParams.getoffset(), reqParams.getLength(),
        sasTokenForReuse);

    try {
      op.execute(tracingContext);
    } catch (AzureBlobFileSystemException e) {
      /*
         If the http response code indicates a user error we retry
         the same append request with expect header being disabled.
         When "100-continue" header is enabled but a non Http 100 response comes,
         the response message might not get set correctly by the server.
         So, this handling is to avoid breaking of backward compatibility
         if someone has taken dependency on the exception message,
         which is created using the error string present in the response header.
      */
      int responseStatusCode = ((AbfsRestOperationException) e).getStatusCode();
      if (checkUserError(responseStatusCode) && reqParams.isExpectHeaderEnabled()) {
        LOG.debug("User error, retrying without 100 continue enabled for the given path {}", path);
        reqParams.setExpectHeaderEnabled(false);
        reqParams.setRetryDueToExpect(true);
        return this.append(path, buffer, reqParams, cachedSasToken,
            contextEncryptionAdapter, tracingContext);
      }
      else {
        throw e;
      }
    }
    return op;
  }

  /**
   * Redirect to flush specific to blob endpoint.
   */
  @Override
  public AbfsRestOperation flush(final String path,
      final long position,
      final boolean retainUncommittedData,
      final boolean isClose,
      final String cachedSasToken,
      final String leaseId,
      final ContextEncryptionAdapter contextEncryptionAdapter,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    return this.flush(null, path, isClose, cachedSasToken, leaseId, null,
        tracingContext);
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list></a>.
   * The flush operation to commit the blocks.
   * @param buffer This has the xml in byte format with the blockIds to be flushed.
   * @param path The path to flush the data to.
   * @param isClose True when the stream is closed.
   * @param cachedSasToken The cachedSasToken if available.
   * @param leaseId The leaseId of the blob if available.
   * @param eTag The etag of the blob.
   * @param tracingContext Tracing context for the operation.
   * @return AbfsRestOperation op.
   * @throws IOException
   */
  @Override
  public AbfsRestOperation flush(byte[] buffer,
      final String path,
      boolean isClose,
      final String cachedSasToken,
      final String leaseId,
      final String eTag,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    requestHeaders.add(new AbfsHttpHeader(CONTENT_LENGTH, String.valueOf(buffer.length)));
    requestHeaders.add(new AbfsHttpHeader(CONTENT_TYPE, APPLICATION_XML));
    requestHeaders.add(new AbfsHttpHeader(IF_MATCH, eTag));
    if (leaseId != null) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, leaseId));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, BLOCKLIST);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_CLOSE, String.valueOf(isClose));
    String sasTokenForReuse = appendSASTokenToQuery(path, SASTokenProvider.WRITE_OPERATION,
        abfsUriQueryBuilder, cachedSasToken);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.PutBlockList,
        HTTP_METHOD_PUT, url, requestHeaders,
        buffer, 0, buffer.length,
        sasTokenForReuse);

    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/set-blob-metadata></a>.
   * Set the properties of a file or directory.
   * @param path on which properties have to be set.
   * @param properties comma separated list of metadata key-value pairs.
   * @param tracingContext
   * @param contextEncryptionAdapter to provide encryption context.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation setPathProperties(final String path,
      final Hashtable<String, String> properties,
      final TracingContext tracingContext,
      final ContextEncryptionAdapter contextEncryptionAdapter)
      throws AzureBlobFileSystemException {
    List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    List<AbfsHttpHeader> metadataRequestHeaders = getMetadataHeadersList(properties);
    requestHeaders.addAll(metadataRequestHeaders);

    AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, METADATA);
    appendSASTokenToQuery(path, SASTokenProvider.SET_PROPERTIES_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.SetPathProperties,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation getPathStatus(final String path,
      final TracingContext tracingContext,
      final ContextEncryptionAdapter contextEncryptionAdapter,
      final boolean isImplicitCheckRequired)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_UPN, String.valueOf(abfsConfiguration.isUpnUsed()));
    appendSASTokenToQuery(path, SASTokenProvider.GET_PROPERTIES_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.GetBlobProperties,
        HTTP_METHOD_HEAD, url, requestHeaders);
    try {
      op.execute(tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      // If we have no HTTP response, throw the original exception.
      if (!op.hasResult()) {
        throw ex;
      }
      if (op.getResult().getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND && isImplicitCheckRequired) {
        // This path could be present as an implicit directory in FNS.
        AbfsRestOperation listOp = listPath(path, false, 1, null, tracingContext);
        BlobListResultSchema listResultSchema =
            (BlobListResultSchema) listOp.getResult().getListResultSchema();
        if (listResultSchema.paths() != null && listResultSchema.paths().size() > 0) {
          AbfsRestOperation successOp = getAbfsRestOperation(
              AbfsRestOperationType.GetPathStatus,
              HTTP_METHOD_HEAD, url, requestHeaders);
          successOp.hardSetGetFileStatusResult(HTTP_OK);
          return successOp;
        }
      }
      throw ex;
    }
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob-properties></a>.
   * Get the properties of a file or directory.
   * @param path of which properties have to be fetched.
   * @param includeProperties to include user defined properties.
   * @param tracingContext
   * @param contextEncryptionAdapter to provide encryption context.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation getPathStatus(final String path,
      final boolean includeProperties,
      final TracingContext tracingContext,
      final ContextEncryptionAdapter contextEncryptionAdapter)
      throws AzureBlobFileSystemException {
    return this.getPathStatus(path, tracingContext, contextEncryptionAdapter, true);
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/delete></a>.
   * Delete the file or directory at specified path.
   * @param path to be deleted.
   * @param recursive if the path is a directory, delete recursively.
   * @param continuation to specify continuation token.
   * @param tracingContext
   * @param isNamespaceEnabled
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation deletePath(final String path,
      final boolean recursive,
      final String continuation,
      TracingContext tracingContext,
      final boolean isNamespaceEnabled) throws AzureBlobFileSystemException {
    // Todo: To be implemented as part of rename-delete over blob endpoint work.
    // This should redirect to delete handler to be implemented.
    throw new NotImplementedException("Delete operation on Blob endpoint will be implemented in future.");
  }

  @Override
  public AbfsRestOperation setOwner(final String path,
      final String owner,
      final String group,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    throw new UnsupportedOperationException(
        "SetOwner operation is only supported on HNS enabled Accounts.");
  }

  @Override
  public AbfsRestOperation setPermission(final String path,
      final String permission,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    throw new UnsupportedOperationException(
        "SetPermission operation is only supported on HNS enabled Accounts.");
  }

  @Override
  public AbfsRestOperation setAcl(final String path,
      final String aclSpecString,
      final String eTag,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    throw new UnsupportedOperationException(
        "SetAcl operation is only supported on HNS enabled Accounts.");
  }

  @Override
  public AbfsRestOperation getAclStatus(final String path, final boolean useUPN,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    throw new UnsupportedOperationException(
        "GetAclStatus operation is only supported on HNS enabled Accounts.");
  }

  @Override
  public AbfsRestOperation checkAccess(String path, String rwx, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    throw new UnsupportedOperationException(
        "CheckAccess operation is only supported on HNS enabled Accounts.");
  }

  @Override
  public boolean checkIsDir(AbfsHttpOperation result) {
    String resourceType = result.getResponseHeader(X_MS_META_HDI_ISFOLDER);
    if (resourceType != null && resourceType.equals(TRUE)) {
      return true;
    }
    return false;
  }

  /**
   * Get the continuation token from the response from DFS Endpoint Listing.
   * Continuation Token will be present in XML List response body.
   * @param result The response from the server.
   * @return The continuation token.
   */
  @Override
  public String getContinuationFromResponse(AbfsHttpOperation result) {
    BlobListResultSchema listResultSchema = (BlobListResultSchema) result.getListResultSchema();
    return listResultSchema.getNextMarker();
  }

  /**
   * Returns true if the status code lies in the range of user error.
   * In the case of HTTP_CONFLICT for PutBlockList we fallback to DFS and hence
   * this retry handling is not needed.
   * @param responseStatusCode http response status code.
   * @return True or False.
   */
  @Override
  public boolean checkUserError(int responseStatusCode) {
    return (responseStatusCode >= HttpURLConnection.HTTP_BAD_REQUEST
        && responseStatusCode < HttpURLConnection.HTTP_INTERNAL_ERROR
        && responseStatusCode != HttpURLConnection.HTTP_CONFLICT);
  }

  /**
   * GetBlockList call to the backend to get the list of committed blockId's.
   * @param path The path to get the list of blockId's.
   * @param tracingContext The tracing context for the operation.
   * @return AbfsRestOperation op.
   * @throws AzureBlobFileSystemException
   */
  @Override
  public AbfsRestOperation getBlockList(final String path, TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    String operation = SASTokenProvider.READ_OPERATION;
    appendSASTokenToQuery(path, operation, abfsUriQueryBuilder);

    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, BLOCKLIST);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_BLOCKLISTTYPE, BLOCK_TYPE_COMMITTED);
    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());

    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.GetBlockList, HTTP_METHOD_GET, url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Caller of <a href = "https://learn.microsoft.com/en-us/rest/api/storageservices/copy-blob">
   * copyBlob API</a>. This is an asynchronous API, it returns copyId and expects client
   * to poll the server on the destination and check the copy-progress.
   *
   * @param sourceBlobPath path of source to be copied
   * @param destinationBlobPath path of the destination
   * @param srcLeaseId
   * @param tracingContext tracingContext object
   *
   * @return AbfsRestOperation abfsRestOperation which contains the response from the server.
   * This method owns the logic of triggereing copyBlob API. The caller of this method have
   * to own the logic of polling the destination with the copyId returned in the response from
   * this method.
   *
   * @throws AzureBlobFileSystemException exception recevied while making server call.
   */
  public AbfsRestOperation copyBlob(Path sourceBlobPath,
      Path destinationBlobPath,
      final String srcLeaseId, TracingContext tracingContext) throws AzureBlobFileSystemException {
    AbfsUriQueryBuilder abfsUriQueryBuilderDst = createDefaultUriQueryBuilder();
    AbfsUriQueryBuilder abfsUriQueryBuilderSrc = new AbfsUriQueryBuilder();
    String dstBlobRelativePath = destinationBlobPath.toUri().getPath();
    String srcBlobRelativePath = sourceBlobPath.toUri().getPath();
    appendSASTokenToQuery(dstBlobRelativePath,
        SASTokenProvider.WRITE_OPERATION, abfsUriQueryBuilderDst);
    appendSASTokenToQuery(srcBlobRelativePath,
        SASTokenProvider.READ_OPERATION, abfsUriQueryBuilderSrc);
    final URL url = createRequestUrl(dstBlobRelativePath,
        abfsUriQueryBuilderDst.toString());
    final String sourcePathUrl = createRequestUrl(srcBlobRelativePath,
        abfsUriQueryBuilderSrc.toString()).toString();
    List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    if (srcLeaseId != null) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_SOURCE_LEASE_ID, srcLeaseId));
    }
    requestHeaders.add(new AbfsHttpHeader(X_MS_COPY_SOURCE, sourcePathUrl));
    requestHeaders.add(new AbfsHttpHeader(IF_NONE_MATCH, STAR));

    final AbfsRestOperation op = getAbfsRestOperation(AbfsRestOperationType.CopyBlob, HTTP_METHOD_PUT,
        url, requestHeaders);

    return op;
  }

  /**
   * Deletes the blob for which the path is given.
   *
   * @param blobPath path on which blob has to be deleted.
   * @param leaseId
   * @param tracingContext tracingContext object for tracing the server calls.
   *
   * @return abfsRestOpertion
   *
   * @throws AzureBlobFileSystemException exception thrown from server or due to
   * network issue.
   */
  public AbfsRestOperation deleteBlobPath(final Path blobPath,
      final String leaseId, final TracingContext tracingContext) throws AzureBlobFileSystemException {
    AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    String blobRelativePath = blobPath.toUri().getPath();
    appendSASTokenToQuery(blobRelativePath,
        SASTokenProvider.DELETE_OPERATION, abfsUriQueryBuilder);
    final URL url = createRequestUrl(blobRelativePath, abfsUriQueryBuilder.toString());
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    if(leaseId != null) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, leaseId));
    }
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.DeleteBlob, HTTP_METHOD_DELETE, url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public static String getDirectoryQueryParameter(final String path) {
    String directory = AbfsClient.getDirectoryQueryParameter(path);
    if (directory.isEmpty()) {
      return directory;
    }
    if (!directory.endsWith("/")) {
      directory = directory + "/";
    }
    return directory;
  }

  /**
   * Parse the list file response
   * @param stream InputStream contains the list results.
   * @throws IOException
   */
  @Override
  public ListResultSchema parseListPathResults(final InputStream stream) throws IOException {
    if (stream == null) {
      return null;
    }
    BlobListResultSchema listResultSchema;
    try {
      final SAXParser saxParser = saxParserThreadLocal.get();
      saxParser.reset();
      listResultSchema = new BlobListResultSchema();
      saxParser.parse(stream, new BlobListXmlParser(listResultSchema, getBaseUrl().toString()));
    } catch (SAXException | IOException e) {
      throw new RuntimeException(e);
    }

    return removeDuplicateEntries(listResultSchema);
  }

  @Override
  public List<String> parseBlockListResponse(final InputStream stream) throws IOException {
    List<String> blockIdList = new ArrayList<>();
    // Convert the input stream to a Document object

    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      Document doc = factory.newDocumentBuilder().parse(stream);

      // Find the CommittedBlocks element and extract the list of block IDs
      NodeList committedBlocksList = doc.getElementsByTagName(
          XML_TAG_COMMITTED_BLOCKS);
      if (committedBlocksList.getLength() > 0) {
        Node committedBlocks = committedBlocksList.item(0);
        NodeList blockList = committedBlocks.getChildNodes();
        for (int i = 0; i < blockList.getLength(); i++) {
          Node block = blockList.item(i);
          if (block.getNodeName().equals(XML_TAG_BLOCK_NAME)) {
            NodeList nameList = block.getChildNodes();
            for (int j = 0; j < nameList.getLength(); j++) {
              Node name = nameList.item(j);
              if (name.getNodeName().equals(XML_TAG_NAME)) {
                String blockId = name.getTextContent();
                blockIdList.add(blockId);
              }
            }
          }
        }
      }
    } catch (ParserConfigurationException | SAXException e) {
      throw new IOException(e);
    }

    return blockIdList;
  }

  @Override
  public StorageErrorResponseSchema processStorageErrorResponse(final InputStream stream) throws IOException {
    final String data = IOUtils.toString(stream, StandardCharsets.UTF_8);
    String storageErrorCode = "", storageErrorMessage = "", expectedAppendPos = "";
    int codeStartFirstInstance = data.indexOf(XML_TAG_BLOB_ERROR_CODE_START_XML);
    int codeEndFirstInstance = data.indexOf(XML_TAG_BLOB_ERROR_CODE_END_XML);
    if (codeEndFirstInstance != -1 && codeStartFirstInstance != -1) {
      storageErrorCode = data.substring(codeStartFirstInstance,
          codeEndFirstInstance).replace(XML_TAG_BLOB_ERROR_CODE_START_XML, "");
    }

    int msgStartFirstInstance = data.indexOf(XML_TAG_BLOB_ERROR_MESSAGE_START_XML);
    int msgEndFirstInstance = data.indexOf(XML_TAG_BLOB_ERROR_MESSAGE_END_XML);
    if (msgEndFirstInstance != -1 && msgStartFirstInstance != -1) {
      storageErrorMessage = data.substring(msgStartFirstInstance,
          msgEndFirstInstance).replace(XML_TAG_BLOB_ERROR_MESSAGE_START_XML, "");
    }
    return new StorageErrorResponseSchema(storageErrorCode, storageErrorMessage, expectedAppendPos);
  }

  @Override
  public Hashtable<String, String> getXMSProperties(AbfsHttpOperation result)
      throws InvalidAbfsRestOperationException {
    Hashtable<String, String> properties = new Hashtable<>();
    Map<String, List<String>> responseHeaders = result.getResponseHeaders();
    for (Map.Entry<String, List<String>> entry : responseHeaders.entrySet()) {
      String name = entry.getKey();
      if (name != null && name.startsWith(X_MS_METADATA_PREFIX)) {
        String value = null;
        try {
          value = decodeMetadataAttribute(entry.getValue().get(0));
        } catch (UnsupportedEncodingException e) {
          throw new InvalidAbfsRestOperationException(e);
        }
        properties.put(name.substring(X_MS_METADATA_PREFIX.length()), value);
      }
    }
    return properties;
  }

  @Override
  public byte[] encodeAttribute(String value) throws UnsupportedEncodingException {
    return value.getBytes(XMS_PROPERTIES_ENCODING_BLOB);
  }

  @Override
  public String decodeAttribute(byte[] value) throws UnsupportedEncodingException {
    return new String(value, XMS_PROPERTIES_ENCODING_BLOB);
  }

  private List<AbfsHttpHeader> getMetadataHeadersList(final Hashtable<String, String> properties) throws AbfsRestOperationException {
    List<AbfsHttpHeader> metadataRequestHeaders = new ArrayList<>();
    for(Map.Entry<String,String> entry : properties.entrySet()) {
      String key = X_MS_METADATA_PREFIX + entry.getKey();
      String value = null;
      try {
        value = encodeMetadataAttribute(entry.getValue());
      } catch (UnsupportedEncodingException e) {
        throw new InvalidAbfsRestOperationException(e);
      }
      metadataRequestHeaders.add(new AbfsHttpHeader(key, value));
    }
    return metadataRequestHeaders;
  }

  private final ThreadLocal<SAXParser> saxParserThreadLocal = ThreadLocal.withInitial(() -> {
    SAXParserFactory factory = SAXParserFactory.newInstance();
    factory.setNamespaceAware(true);
    try {
      return factory.newSAXParser();
    } catch (SAXException e) {
      throw new RuntimeException("Unable to create SAXParser", e);
    } catch (ParserConfigurationException e) {
      throw new RuntimeException("Check parser configuration", e);
    }
  });

  private BlobListResultSchema removeDuplicateEntries(BlobListResultSchema listResultSchema) {
    List<BlobListResultEntrySchema> uniqueEntries = new ArrayList<>();
    TreeMap<String, BlobListResultEntrySchema> nameToEntryMap = new TreeMap<>();

    for (BlobListResultEntrySchema entry : listResultSchema.paths()) {
      if (entry.eTag() != null && !entry.eTag().isEmpty()) {
        // This is a blob entry. It is either a file or a marker blob.
        // In both cases we will add this.
        nameToEntryMap.put(entry.name(), entry);
      } else {
        // This is a BlobPrefix entry. It is a directory with file inside
        // This might have already been added as a marker blob.
        if (!nameToEntryMap.containsKey(entry.name())) {
          nameToEntryMap.put(entry.name(), entry);
        }
      }
    }

    uniqueEntries.addAll(nameToEntryMap.values());
    listResultSchema.withPaths(uniqueEntries);
    return listResultSchema;
  }

  private BlobListResultSchema getListResultSchemaFromPathStatus(String relativePath, AbfsRestOperation pathStatus) {
    BlobListResultSchema listResultSchema = new BlobListResultSchema();

    BlobListResultEntrySchema entrySchema = new BlobListResultEntrySchema();
    entrySchema.setUrl(pathStatus.getUrl().toString());
    entrySchema.setPath(new Path(relativePath));
    entrySchema.setName(relativePath.charAt(0) == '/' ? relativePath.substring(1) : relativePath);
    entrySchema.setIsDirectory(checkIsDir(pathStatus.getResult()));
    entrySchema.setContentLength(Long.parseLong(pathStatus.getResult().getResponseHeader(CONTENT_LENGTH)));
    entrySchema.setLastModifiedTime(
        pathStatus.getResult().getResponseHeader(HttpHeaderConfigurations.LAST_MODIFIED));
    entrySchema.setETag(pathStatus.getResult().getResponseHeader(ETAG));

    listResultSchema.paths().add(entrySchema);
    return listResultSchema;
  }

  private static String encodeMetadataAttribute(String value)
      throws UnsupportedEncodingException {
    return value == null ? null
        : URLEncoder.encode(value, StandardCharsets.UTF_8.name());
  }

  private static String decodeMetadataAttribute(String encoded)
      throws UnsupportedEncodingException {
    return encoded == null ? null :
        java.net.URLDecoder.decode(encoded, StandardCharsets.UTF_8.name());
  }
}
