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
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsInvalidChecksumException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.contracts.services.DfsListResultSchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.StorageErrorResponseSchema;
import org.apache.hadoop.fs.azurebfs.extensions.EncryptionContextProvider;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore.extractEtagHeader;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.*;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_BLOB_DOMAIN_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_DFS_DOMAIN_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.*;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.*;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.RENAME_DESTINATION_PARENT_PATH_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.SOURCE_PATH_NOT_FOUND;

/**
 * AbfsClient interacting with the DFS Endpoint.
 */
public class AbfsDfsClient extends AbfsClient implements Closeable {

  public AbfsDfsClient(final URL baseUrl,
      final SharedKeyCredentials sharedKeyCredentials,
      final AbfsConfiguration abfsConfiguration,
      final AccessTokenProvider tokenProvider,
      final EncryptionContextProvider encryptionContextProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    super(changePrefixFromBlobToDfs(baseUrl), sharedKeyCredentials, abfsConfiguration, tokenProvider,
        encryptionContextProvider, abfsClientContext);
  }

  public AbfsDfsClient(final URL baseUrl,
      final SharedKeyCredentials sharedKeyCredentials,
      final AbfsConfiguration abfsConfiguration,
      final SASTokenProvider sasTokenProvider,
      final EncryptionContextProvider encryptionContextProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    super(changePrefixFromBlobToDfs(baseUrl), sharedKeyCredentials, abfsConfiguration, sasTokenProvider,
        encryptionContextProvider, abfsClientContext);
  }

  private static URL changePrefixFromBlobToDfs(URL url) throws MalformedURLException {
    try {
      String urlString = url.toString().replace(ABFS_BLOB_DOMAIN_NAME, ABFS_DFS_DOMAIN_NAME);
      return new URL(urlString);
    } catch (MalformedURLException ex) {
      throw new MalformedURLException(url.toString());
    }
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
   * DFS Endpoint API responses are in JSON/Stream format.
   * @param xMsVersion
   * @return default request headers
   */
  @Override
  public List<AbfsHttpHeader> createDefaultHeaders(ApiVersion xMsVersion) {
    List<AbfsHttpHeader> requestHeaders = super.createCommonHeaders(xMsVersion);
    requestHeaders.add(new AbfsHttpHeader(ACCEPT, APPLICATION_JSON
        + COMMA + SINGLE_WHITE_SPACE + APPLICATION_OCTET_STREAM));
    return requestHeaders;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/filesystem/create></a>.
   * Creates a filesystem.
   * @param tracingContext
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation createFilesystem(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = new AbfsUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.CreateFileSystem,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/filesystem/set-properties></a>.
   * Sets user-defined properties of the filesystem.
   * @param properties comma separated list of metadata key-value pairs.
   * @param tracingContext
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation setFilesystemProperties(final String properties,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    // JDK7 does not support PATCH, so to work around the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
        HTTP_METHOD_PATCH));
    requestHeaders.add(new AbfsHttpHeader(X_MS_PROPERTIES, properties));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.SetFileSystemProperties,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/filesystem/get-properties></a>.
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
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.GetFileSystemProperties,
        HTTP_METHOD_HEAD, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/filesystem/delete></a>.
   * Deletes the current filesystem.
   * @param tracingContext
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation deleteFilesystem(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.DeleteFileSystem,
        HTTP_METHOD_DELETE, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/list></a>.
   * List paths and their properties in the current filesystem.
   * @param relativePath to return only blobs within this directory.
   * @param recursive to return all blobs in the path, including those in subdirectories.
   * @param listMaxResults maximum number of blobs to return.
   * @param continuation marker to specify the continuation token.
   * @param tracingContext
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation or response parsing fails.
   */
  @Override
  public AbfsRestOperation listPath(final String relativePath,
      final boolean recursive,
      final int listMaxResults,
      final String continuation,
      TracingContext tracingContext)
      throws IOException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_DIRECTORY, getDirectoryQueryParameter(relativePath));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RECURSIVE, String.valueOf(recursive));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_CONTINUATION, continuation);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_MAXRESULTS, String.valueOf(listMaxResults));
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_UPN, String.valueOf(abfsConfiguration.isUpnUsed()));
    appendSASTokenToQuery(relativePath, SASTokenProvider.LIST_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.ListPaths,
        HTTP_METHOD_GET, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/create></a>.
   * Create a path (file or directory) in the current filesystem.
   * @param path to be created inside the filesystem.
   * @param isFile to specify if the created path is file or directory.
   * @param overwrite to specify if the path should be overwritten if it already exists.
   * @param permissions to specify the permissions of the path.
   * @param isAppendBlob to specify if the path to be created is an append blob.
   * @param eTag to specify conditional headers.
   * @param contextEncryptionAdapter to provide encryption context.
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
      final TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    if (isFile) {
      addEncryptionKeyRequestHeaders(path, requestHeaders, true,
          contextEncryptionAdapter, tracingContext);
    }
    if (!overwrite) {
      requestHeaders.add(new AbfsHttpHeader(IF_NONE_MATCH, STAR));
    }

    if (permissions.hasPermission()) {
      requestHeaders.add(
          new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_PERMISSIONS,
              permissions.getPermission()));
    }

    if (permissions.hasUmask()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_UMASK,
          permissions.getUmask()));
    }

    if (eTag != null && !eTag.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.IF_MATCH, eTag));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, isFile ? FILE : DIRECTORY);
    if (isAppendBlob) {
      abfsUriQueryBuilder.addQuery(QUERY_PARAM_BLOBTYPE, APPEND_BLOB_TYPE);
    }

    String operation = isFile
        ? SASTokenProvider.CREATE_FILE_OPERATION
        : SASTokenProvider.CREATE_DIRECTORY_OPERATION;
    appendSASTokenToQuery(path, operation, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.CreatePath,
        HTTP_METHOD_PUT, url, requestHeaders);
    try {
      op.execute(tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      // If we have no HTTP response, throw the original exception.
      if (!op.hasResult()) {
        throw ex;
      }
      if (!isFile && op.getResult().getStatusCode() == HttpURLConnection.HTTP_CONFLICT) {
        String existingResource =
            op.getResult().getResponseHeader(X_MS_EXISTING_RESOURCE_TYPE);
        if (existingResource != null && existingResource.equals(DIRECTORY)) {
          return op; //don't throw ex on mkdirs for existing directory
        }
      }
      throw ex;
    }
    return op;
  }

  @Override
  public void createMarkerBlobs(final Path path,
      final boolean overwrite,
      final AzureBlobFileSystemStore.Permissions permissions,
      final boolean isAppendBlob,
      final String eTag,
      final ContextEncryptionAdapter contextEncryptionAdapter,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/lease></a>.
   * Acquire lease on specified path.
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

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.LeasePath,
        HTTP_METHOD_POST, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/lease></a>.
   * Renew lease on specified path.
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

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.LeasePath,
        HTTP_METHOD_POST, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/lease></a>.
   * Release lease on specified path.
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

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.LeasePath,
        HTTP_METHOD_POST, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/lease></a>.
   * Break lease on specified path.
   * @param path on which lease has to be broke.
   * @param tracingContext
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation breakLease(final String path, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, BREAK_LEASE_ACTION));
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_BREAK_PERIOD, DEFAULT_LEASE_BREAK_PERIOD));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.LeasePath,
        HTTP_METHOD_POST,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  @Override
  public AbfsClientRenameResult renamePath(
      final String source,
      final String destination,
      final String continuation,
      final TracingContext tracingContext,
      String sourceEtag,
      boolean isMetadataIncompleteState,
      boolean isNamespaceEnabled)
      throws IOException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final boolean hasEtag = !isEmpty(sourceEtag);

    boolean shouldAttemptRecovery = renameResilience && isNamespaceEnabled;
    if (!hasEtag && shouldAttemptRecovery) {
      // in case eTag is already not supplied to the API
      // and rename resilience is expected and it is an HNS enabled account
      // fetch the source etag to be used later in recovery
      try {
        final AbfsRestOperation srcStatusOp = getPathStatus(source,
            false, tracingContext, null);
        if (srcStatusOp.hasResult()) {
          final AbfsHttpOperation result = srcStatusOp.getResult();
          sourceEtag = extractEtagHeader(result);
          // and update the directory status.
          boolean isDir = checkIsDir(result);
          shouldAttemptRecovery = !isDir;
          LOG.debug("Retrieved etag of source for rename recovery: {}; isDir={}", sourceEtag, isDir);
        }
      } catch (AbfsRestOperationException e) {
        throw new AbfsRestOperationException(e.getStatusCode(), SOURCE_PATH_NOT_FOUND.getErrorCode(),
            e.getMessage(), e);
      }

    }

    String encodedRenameSource = urlEncode(FORWARD_SLASH + this.getFileSystem() + source);
    if (authType == AuthType.SAS) {
      final AbfsUriQueryBuilder srcQueryBuilder = new AbfsUriQueryBuilder();
      appendSASTokenToQuery(source, SASTokenProvider.RENAME_SOURCE_OPERATION, srcQueryBuilder);
      encodedRenameSource += srcQueryBuilder.toString();
    }

    LOG.trace("Rename source queryparam added {}", encodedRenameSource);
    requestHeaders.add(new AbfsHttpHeader(X_MS_RENAME_SOURCE, encodedRenameSource));
    requestHeaders.add(new AbfsHttpHeader(IF_NONE_MATCH, STAR));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_CONTINUATION, continuation);
    appendSASTokenToQuery(destination, SASTokenProvider.RENAME_DESTINATION_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(destination, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = createRenameRestOperation(url, requestHeaders);
    try {
      incrementAbfsRenamePath();
      op.execute(tracingContext);
      // AbfsClientResult contains the AbfsOperation, If recovery happened or
      // not, and the incompleteMetaDataState is true or false.
      // If we successfully rename a path and isMetadataIncompleteState was
      // true, then rename was recovered, else it didn't, this is why
      // isMetadataIncompleteState is used for renameRecovery(as the 2nd param).
      return new AbfsClientRenameResult(op, isMetadataIncompleteState, isMetadataIncompleteState);
    } catch (AzureBlobFileSystemException e) {
      // If we have no HTTP response, throw the original exception.
      if (!op.hasResult()) {
        throw e;
      }

      // ref: HADOOP-18242. Rename failure occurring due to a rare case of
      // tracking metadata being in incomplete state.
      if (op.getResult().getStorageErrorCode()
          .equals(RENAME_DESTINATION_PARENT_PATH_NOT_FOUND.getErrorCode())
          && !isMetadataIncompleteState) {
        //Logging
        ABFS_METADATA_INCOMPLETE_RENAME_FAILURE
            .info("Rename Failure attempting to resolve tracking metadata state and retrying.");
        // rename recovery should be attempted in this case also
        shouldAttemptRecovery = true;
        isMetadataIncompleteState = true;
        String sourceEtagAfterFailure = sourceEtag;
        if (isEmpty(sourceEtagAfterFailure)) {
          // Doing a HEAD call resolves the incomplete metadata state and
          // then we can retry the rename operation.
          AbfsRestOperation sourceStatusOp = getPathStatus(source, false,
              tracingContext, null);
          isMetadataIncompleteState = true;
          // Extract the sourceEtag, using the status Op, and set it
          // for future rename recovery.
          AbfsHttpOperation sourceStatusResult = sourceStatusOp.getResult();
          sourceEtagAfterFailure = extractEtagHeader(sourceStatusResult);
        }
        renamePath(source, destination, continuation, tracingContext,
            sourceEtagAfterFailure, isMetadataIncompleteState, isNamespaceEnabled);
      }
      // if we get out of the condition without a successful rename, then
      // it isn't metadata incomplete state issue.
      isMetadataIncompleteState = false;

      // setting default rename recovery success to false
      boolean etagCheckSucceeded = false;
      if (shouldAttemptRecovery) {
        etagCheckSucceeded = renameIdempotencyCheckOp(
            source,
            sourceEtag, op, destination, tracingContext);
      }
      if (!etagCheckSucceeded) {
        // idempotency did not return different result
        // throw back the exception
        throw e;
      }
      return new AbfsClientRenameResult(op, true, isMetadataIncompleteState);
    }
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update></a>.
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
      AppendRequestParameters reqParams,
      final String cachedSasToken,
      ContextEncryptionAdapter contextEncryptionAdapter,
      TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    addEncryptionKeyRequestHeaders(path, requestHeaders, false,
        contextEncryptionAdapter, tracingContext);
    if (reqParams.isExpectHeaderEnabled()) {
      requestHeaders.add(new AbfsHttpHeader(EXPECT, HUNDRED_CONTINUE));
    }
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
        HTTP_METHOD_PATCH));
    if (reqParams.getLeaseId() != null) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, reqParams.getLeaseId()));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, APPEND_ACTION);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_POSITION, Long.toString(reqParams.getPosition()));

    if ((reqParams.getMode() == AppendRequestParameters.Mode.FLUSH_MODE) || (
        reqParams.getMode() == AppendRequestParameters.Mode.FLUSH_CLOSE_MODE)) {
      abfsUriQueryBuilder.addQuery(QUERY_PARAM_FLUSH, TRUE);
      if (reqParams.getMode() == AppendRequestParameters.Mode.FLUSH_CLOSE_MODE) {
        abfsUriQueryBuilder.addQuery(QUERY_PARAM_CLOSE, TRUE);
      }
    }

    // Check if the retry is with "Expect: 100-continue" header being present in the previous request.
    if (reqParams.isRetryDueToExpect()) {
      String userAgentRetry = userAgent;
      // Remove the specific marker related to "Expect: 100-continue" from the User-Agent string.
      userAgentRetry = userAgentRetry.replace(HUNDRED_CONTINUE_USER_AGENT, EMPTY_STRING);
      requestHeaders.removeIf(header -> header.getName().equalsIgnoreCase(USER_AGENT));
      requestHeaders.add(new AbfsHttpHeader(USER_AGENT, userAgentRetry));
    }

    // Add MD5 Hash of request content as request header if feature is enabled
    if (isChecksumValidationEnabled()) {
      addCheckSumHeaderForWrite(requestHeaders, reqParams, buffer);
    }

    // AbfsInputStream/AbfsOutputStream reuse SAS tokens for better performance
    String sasTokenForReuse = appendSASTokenToQuery(path, SASTokenProvider.WRITE_OPERATION,
        abfsUriQueryBuilder, cachedSasToken);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.Append,
        HTTP_METHOD_PUT, url, requestHeaders,
        buffer, reqParams.getoffset(), reqParams.getLength(),
        sasTokenForReuse);
    try {
      op.execute(tracingContext);
    } catch (AbfsRestOperationException e) {
      /*
         If the http response code indicates a user error we retry
         the same append request with expect header being disabled.
         When "100-continue" header is enabled but a non Http 100 response comes,
         the response message might not get set correctly by the server.
         So, this handling is to avoid breaking of backward compatibility
         if someone has taken dependency on the exception message,
         which is created using the error string present in the response header.
      */
      int responseStatusCode = e.getStatusCode();
      if (checkUserError(responseStatusCode) && reqParams.isExpectHeaderEnabled()) {
        LOG.debug("User error, retrying without 100 continue enabled for the given path {}", path);
        reqParams.setExpectHeaderEnabled(false);
        reqParams.setRetryDueToExpect(true);
        return this.append(path, buffer, reqParams, cachedSasToken,
            contextEncryptionAdapter, tracingContext);
      }
      // If we have no HTTP response, throw the original exception.
      if (!op.hasResult()) {
        throw e;
      }

      if (isMd5ChecksumError(e)) {
        throw new AbfsInvalidChecksumException(e);
      }

      if (reqParams.isAppendBlob()
          && appendSuccessCheckOp(op, path,
          (reqParams.getPosition() + reqParams.getLength()), tracingContext)) {
        final AbfsRestOperation successOp = getAbfsRestOperation(
            AbfsRestOperationType.Append,
            HTTP_METHOD_PUT, url, requestHeaders,
            buffer, reqParams.getoffset(), reqParams.getLength(),
            sasTokenForReuse);
        successOp.hardSetResult(HttpURLConnection.HTTP_OK);
        return successOp;
      }
      throw e;
    }

    catch (AzureBlobFileSystemException e) {
      // Any server side issue will be returned as AbfsRestOperationException and will be handled above.
      LOG.debug("Append request failed with non server issues for path: {}, offset: {}, position: {}",
          path, reqParams.getoffset(), reqParams.getPosition());
      throw e;
    }

    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update></a>.
   * Flush previously uploaded data to a file.
   * @param path on which data has to be flushed.
   * @param position to which data has to be flushed.
   * @param retainUncommittedData whether to retain uncommitted data after flush.
   * @param isClose specify if this is the last flush to the file
   * @param cachedSasToken to be used for the authenticating operation.
   * @param leaseId if there is an active lease on the path
   * @param contextEncryptionAdapter to provide encryption context.
   * @param tracingContext
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public AbfsRestOperation flush(final String path,
      final long position,
      boolean retainUncommittedData,
      boolean isClose,
      final String cachedSasToken,
      final String leaseId,
      ContextEncryptionAdapter contextEncryptionAdapter,
      TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    addEncryptionKeyRequestHeaders(path, requestHeaders, false,
        contextEncryptionAdapter, tracingContext);
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
        HTTP_METHOD_PATCH));
    if (leaseId != null) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, leaseId));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, FLUSH_ACTION);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_POSITION, Long.toString(position));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RETAIN_UNCOMMITTED_DATA, String.valueOf(retainUncommittedData));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_CLOSE, String.valueOf(isClose));
    // AbfsInputStream/AbfsOutputStream reuse SAS tokens for better performance
    String sasTokenForReuse = appendSASTokenToQuery(path, SASTokenProvider.WRITE_OPERATION,
        abfsUriQueryBuilder, cachedSasToken);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.Flush,
        HTTP_METHOD_PUT, url, requestHeaders,
        sasTokenForReuse);
    op.execute(tracingContext);
    return op;
  }

  @Override
  public AbfsRestOperation flush(byte[] buffer,
      final String path,
      boolean isClose,
      final String cachedSasToken,
      final String leaseId,
      final String eTag,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    throw new UnsupportedOperationException(
        "flush with blockIds not supported on DFS Endpoint");
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update></a>.
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
      final String properties,
      final TracingContext tracingContext,
      final ContextEncryptionAdapter contextEncryptionAdapter)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    addEncryptionKeyRequestHeaders(path, requestHeaders, false,
        contextEncryptionAdapter, tracingContext);
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
        HTTP_METHOD_PATCH));
    requestHeaders.add(new AbfsHttpHeader(X_MS_PROPERTIES, properties));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, SET_PROPERTIES_ACTION);
    appendSASTokenToQuery(path, SASTokenProvider.SET_PROPERTIES_OPERATION,
        abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.SetPathProperties,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/get-properties></a>.
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
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    String operation = SASTokenProvider.GET_PROPERTIES_OPERATION;
    if (!includeProperties) {
      // The default action (operation) is implicitly to get properties and this action requires read permission
      // because it reads user defined properties.  If the action is getStatus or getAclStatus, then
      // only traversal (execute) permission is required.
      abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_ACTION, GET_STATUS);
      operation = SASTokenProvider.GET_STATUS_OPERATION;
    } else {
      addEncryptionKeyRequestHeaders(path, requestHeaders, false,
          contextEncryptionAdapter,
          tracingContext);
    }
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_UPN, String.valueOf(abfsConfiguration.isUpnUsed()));
    appendSASTokenToQuery(path, operation, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.GetPathStatus,
        HTTP_METHOD_HEAD, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/read></a>.
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
      String cachedSasToken,
      ContextEncryptionAdapter contextEncryptionAdapter,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    addEncryptionKeyRequestHeaders(path, requestHeaders, false,
        contextEncryptionAdapter, tracingContext);
    AbfsHttpHeader rangeHeader = new AbfsHttpHeader(RANGE,
        String.format("bytes=%d-%d", position, position + bufferLength - 1));
    requestHeaders.add(rangeHeader);
    requestHeaders.add(new AbfsHttpHeader(IF_MATCH, eTag));

    // Add request header to fetch MD5 Hash of data returned by server.
    if (isChecksumValidationEnabled(requestHeaders, rangeHeader, bufferLength)) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_RANGE_GET_CONTENT_MD5, TRUE));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    // AbfsInputStream/AbfsOutputStream reuse SAS tokens for better performance
    String sasTokenForReuse = appendSASTokenToQuery(path, SASTokenProvider.READ_OPERATION,
        abfsUriQueryBuilder, cachedSasToken);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.ReadFile,
        HTTP_METHOD_GET, url, requestHeaders,
        buffer, bufferOffset, bufferLength,
        sasTokenForReuse);
    op.execute(tracingContext);

    // Verify the MD5 hash returned by server holds valid on the data received.
    if (isChecksumValidationEnabled(requestHeaders, rangeHeader, bufferLength)) {
      verifyCheckSumForRead(buffer, op.getResult(), bufferOffset);
    }

    return op;
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
  public AbfsRestOperation deletePath(final String path, final boolean recursive,
      final String continuation,
      TracingContext tracingContext,
      final boolean isNamespaceEnabled)
      throws AzureBlobFileSystemException {
    /*
     * If Pagination is enabled and current API version is old,
     * use the minimum required version for pagination.
     * If Pagination is enabled and current API version is later than minimum required
     * version for pagination, use current version only as azure service is backward compatible.
     * If pagination is disabled, use the current API version only.
     */
    final List<AbfsHttpHeader> requestHeaders = (isPaginatedDelete(recursive,
        isNamespaceEnabled) && xMsVersion.compareTo(
        ApiVersion.AUG_03_2023) < 0)
        ? createDefaultHeaders(ApiVersion.AUG_03_2023)
        : createDefaultHeaders();
    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();

    if (isPaginatedDelete(recursive, isNamespaceEnabled)) {
      // Add paginated query parameter
      abfsUriQueryBuilder.addQuery(QUERY_PARAM_PAGINATED, TRUE);
    }

    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RECURSIVE, String.valueOf(recursive));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_CONTINUATION, continuation);
    String operation = recursive ? SASTokenProvider.DELETE_RECURSIVE_OPERATION : SASTokenProvider.DELETE_OPERATION;
    appendSASTokenToQuery(path, operation, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.DeletePath, this,
        HTTP_METHOD_DELETE, url, requestHeaders);
    try {
      op.execute(tracingContext);
    } catch (AzureBlobFileSystemException e) {
      // If we have no HTTP response, throw the original exception.
      if (!op.hasResult()) {
        throw e;
      }
      final AbfsRestOperation idempotencyOp = deleteIdempotencyCheckOp(op);
      if (idempotencyOp.getResult().getStatusCode()
          == op.getResult().getStatusCode()) {
        // idempotency did not return different result
        // throw back the exception
        throw e;
      } else {
        return idempotencyOp;
      }
    }

    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update></a>.
   * @param path on which owner has to be set.
   * @param owner to be set.
   * @param group to be set.
   * @param tracingContext
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation setOwner(final String path, final String owner, final String group,
      TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
        HTTP_METHOD_PATCH));
    if (owner != null && !owner.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_OWNER, owner));
    }
    if (group != null && !group.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_GROUP, group));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_ACTION, SET_ACCESS_CONTROL);
    appendSASTokenToQuery(path, SASTokenProvider.SET_OWNER_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.SetOwner,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update></a>.
   * @param path on which permission has to be set.
   * @param permission to be set.
   * @param tracingContext
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation setPermission(final String path, final String permission,
      TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
        HTTP_METHOD_PATCH));
    requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_PERMISSIONS, permission));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_ACTION, SET_ACCESS_CONTROL);
    appendSASTokenToQuery(path, SASTokenProvider.SET_PERMISSION_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.SetPermissions,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update></a>.
   * @param path on which ACL has to be set.
   * @param aclSpecString to be set.
   * @param eTag to specify conditional headers. Set only if etag matches.
   * @param tracingContext
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public AbfsRestOperation setAcl(final String path, final String aclSpecString, final String eTag,
      TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
        HTTP_METHOD_PATCH));
    requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_ACL, aclSpecString));
    if (eTag != null && !eTag.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.IF_MATCH, eTag));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_ACTION, SET_ACCESS_CONTROL);
    appendSASTokenToQuery(path, SASTokenProvider.SET_ACL_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.SetAcl,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/get-properties></a>.
   * @param path of which properties have to be fetched.
   * @param useUPN
   * @param tracingContext
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation getAclStatus(final String path, final boolean useUPN,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_ACTION, GET_ACCESS_CONTROL);
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_UPN, String.valueOf(useUPN));
    appendSASTokenToQuery(path, SASTokenProvider.GET_ACL_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.GetAcl,
        HTTP_METHOD_HEAD, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/get-properties></a>.
   * @param path Path for which access check needs to be performed
   * @param rwx The permission to be checked on the path
   * @param tracingContext
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation checkAccess(String path, String rwx, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, CHECK_ACCESS);
    abfsUriQueryBuilder.addQuery(QUERY_FS_ACTION, rwx);
    appendSASTokenToQuery(path, SASTokenProvider.CHECK_ACCESS_OPERATION, abfsUriQueryBuilder);

    URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.CheckAccess,
        HTTP_METHOD_HEAD, url, createDefaultHeaders());
    op.execute(tracingContext);
    return op;
  }

  /**
   * Checks if the rest operation results indicate if the path is a directory.
   * @param result executed rest operation containing response from server.
   * @return True if the path is a directory, False otherwise.
   */
  @Override
  public boolean checkIsDir(AbfsHttpOperation result) {
    String resourceType = result.getResponseHeader(
        HttpHeaderConfigurations.X_MS_RESOURCE_TYPE);
    return resourceType != null
        && resourceType.equalsIgnoreCase(DIRECTORY);
  }

  /**
   * Get the continuation token from the response from DFS Endpoint Listing.
   * Continuation Token will be present as a response header.
   * @param result The response from the server.
   * @return The continuation token.
   */
  @Override
  public String getContinuationFromResponse(AbfsHttpOperation result) {
    return result.getResponseHeader(HttpHeaderConfigurations.X_MS_CONTINUATION);
  }

  /**
   * Returns true if the status code lies in the range of user error.
   * @param responseStatusCode http response status code.
   * @return True or False.
   */
  @Override
  public boolean checkUserError(int responseStatusCode) {
    return (responseStatusCode >= HttpURLConnection.HTTP_BAD_REQUEST
        && responseStatusCode < HttpURLConnection.HTTP_INTERNAL_ERROR);
  }

  /**
   * Parse the list file response
   * @param stream InputStream contains the list results.
   * @throws IOException
   */
  @Override
  public ListResultSchema parseListPathResults(final InputStream stream) throws IOException {
    DfsListResultSchema listResultSchema;
    try {
      final ObjectMapper objectMapper = new ObjectMapper();
      listResultSchema = objectMapper.readValue(stream, DfsListResultSchema.class);
    } catch (IOException ex) {
      LOG.error("Unable to deserialize list results", ex);
      throw ex;
    }
    return listResultSchema;
  }

  @Override
  public List<String> parseBlockListResponse(final InputStream stream) throws IOException {
    return null;
  }

  @Override
  public AbfsRestOperation getBlockList(final String path,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    return null;
  }

  @Override
  public AbfsRestOperation appendBlock(final String path,
      AppendRequestParameters requestParameters,
      final byte[] data,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    return null;
  }

  /**
   * When the request fails, this function is used to parse the responseAbfsHttpClient.LOG.debug("ExpectedError: ", ex);
   * and extract the storageErrorCode and storageErrorMessage.  Any errors
   * encountered while attempting to process the error response are logged,
   * but otherwise ignored.
   *
   * For storage errors, the response body *usually* has the following format:
   *
   * {
   *   "error":
   *   {
   *     "code": "string",
   *     "message": "string"
   *   }
   * }
   *
   */
  public StorageErrorResponseSchema processStorageErrorResponse(final InputStream stream) throws IOException {
    String storageErrorCode = "", storageErrorMessage = "", expectedAppendPos = "";
    try {
      JsonFactory jf = new JsonFactory();
      try (JsonParser jp = jf.createParser(stream)) {
        String fieldName, fieldValue;
        jp.nextToken();  // START_OBJECT - {
        jp.nextToken();  // FIELD_NAME - "error":
        jp.nextToken();  // START_OBJECT - {
        jp.nextToken();
        while (jp.hasCurrentToken()) {
          if (jp.getCurrentToken() == JsonToken.FIELD_NAME) {
            fieldName = jp.getCurrentName();
            jp.nextToken();
            fieldValue = jp.getText();
            switch (fieldName) {
            case "code":
              storageErrorCode = fieldValue;
              break;
            case "message":
              storageErrorMessage = fieldValue;
              break;
            case "ExpectedAppendPos":
              expectedAppendPos = fieldValue;
              break;
            default:
              break;
            }
          }
          jp.nextToken();
        }
      }
    } catch (IOException e) {
      throw e;
    }
    return new StorageErrorResponseSchema(storageErrorCode, storageErrorMessage, expectedAppendPos);
  }
}