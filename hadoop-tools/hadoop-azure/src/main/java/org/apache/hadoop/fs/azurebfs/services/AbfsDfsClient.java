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

import java.io.Closeable;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsInvalidChecksumException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.extensions.EncryptionContextProvider;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore.extractEtagHeader;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ACQUIRE_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPEND_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPEND_BLOB_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPLICATION_JSON;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPLICATION_OCTET_STREAM;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPLICATION_XML;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOCKLIST;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOCK_TYPE_COMMITTED;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BREAK_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHECK_ACCESS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COMMA;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DEFAULT_LEASE_BREAK_PERIOD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DIRECTORY;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FILE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FILESYSTEM;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FLUSH_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FORWARD_SLASH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_DELETE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_GET;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_HEAD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PATCH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_POST;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HUNDRED_CONTINUE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.RELEASE_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.RENEW_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SET_PROPERTIES_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SINGLE_WHITE_SPACE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.STAR;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.TRUE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.UTF_8;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.ACCEPT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.ACCEPT_CHARSET;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.EXPECT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.IF_MATCH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.IF_NONE_MATCH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.RANGE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.USER_AGENT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_HTTP_METHOD_OVERRIDE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_COPY_SOURCE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_EXISTING_RESOURCE_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_BREAK_PERIOD;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_DURATION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_ID;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_METADATA_PREFIX;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_PROPERTIES;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_PROPOSED_LEASE_ID;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_RANGE_GET_CONTENT_MD5;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_RENAME_SOURCE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_SOURCE_LEASE_ID;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_FS_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_BLOBTYPE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_BLOCKLISTTYPE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_CLOSE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_COMP;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_CONTINUATION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_DIRECTORY;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_FLUSH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_MAXRESULTS;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_PAGINATED;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_POSITION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_RECURSIVE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_RESOURCE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_RETAIN_UNCOMMITTED_DATA;
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
    super(baseUrl, sharedKeyCredentials, abfsConfiguration, tokenProvider,
        encryptionContextProvider, abfsClientContext);
  }

  public AbfsDfsClient(final URL baseUrl,
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

  /**
   * Create request headers for Rest Operation using the specified API version.
   * DFS Endpoint API responses are in JSON format.
   * @param xMsVersion
   * @return default request headers
   */
  @Override
  public List<AbfsHttpHeader> createDefaultHeaders(AbfsHttpConstants.ApiVersion xMsVersion) {
    List<AbfsHttpHeader> requestHeaders = super.createDefaultHeaders(xMsVersion);
    requestHeaders.add(new AbfsHttpHeader(ACCEPT, APPLICATION_JSON
        + COMMA + SINGLE_WHITE_SPACE + APPLICATION_OCTET_STREAM));
    return requestHeaders;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/filesystem/create></a>.
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
   * Sets user-defined properties(metadata) of the container(filesystem root).
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

    requestHeaders.add(new AbfsHttpHeader(X_MS_PROPERTIES,
        properties));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.SetFileSystemProperties,
        HTTP_METHOD_PUT,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/filesystem/get-properties></a>.
   * Gets the all properties(including metadata) of the container(filesystem root).
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
        HTTP_METHOD_HEAD,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/filesystem/delete></a>.
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
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.DeleteFileSystem,
        HTTP_METHOD_DELETE,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/list></a>.
   * @param relativePath to return only blobs with names that begin with the specified prefix
   * @param recursive to return all blobs in the path, including those in subdirectories
   * @param listMaxResults maximum number of blobs to return
   * @param continuation marker to specify the continuation token
   * @param tracingContext
   * @throws AzureBlobFileSystemException if rest operation or response parsing fails
   */
  @Override
  public AbfsRestOperation listPath(final String relativePath, final boolean recursive,
      final int listMaxResults, final String continuation, TracingContext tracingContext)
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
        HTTP_METHOD_GET,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Method for calling createPath API to the backend. Method can be called from:
   * <ol>
   *   <li>create new file</li>
   *   <li>overwrite file</li>
   *   <li>create new directory</li>
   * </ol>
   *
   * @param path: path of the file / directory to be created / overwritten.
   * @param isFile: defines if file or directory has to be created / overwritten.
   * @param overwrite: defines if the file / directory to be overwritten.
   * @param permissions: contains permission and umask
   * @param isAppendBlob: defines if directory in the path is enabled for appendBlob
   * @param eTag: required in case of overwrite of file / directory. Path would be
   * overwritten only if the provided eTag is equal to the one present in backend for
   * the path.
   * @param contextEncryptionAdapter: object that contains the encryptionContext and
   * encryptionKey created from the developer provided implementation of
   * {@link org.apache.hadoop.fs.azurebfs.extensions.EncryptionContextProvider}
   * @param tracingContext: Object of {@link org.apache.hadoop.fs.azurebfs.utils.TracingContext}
   * correlating to the current fs.create() request.
   * @return object of {@link AbfsRestOperation} which contain all the information
   * about the communication with the server. The information is in
   * {@link AbfsRestOperation#getResult()}
   * @throws AzureBlobFileSystemException throws back the exception it receives from the
   * {@link AbfsRestOperation#execute(TracingContext)} method call.
   */
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
      requestHeaders.add(new AbfsHttpHeader(IF_NONE_MATCH, AbfsHttpConstants.STAR));
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
        HTTP_METHOD_PUT,
        url,
        requestHeaders);
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
        HTTP_METHOD_POST,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

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
        HTTP_METHOD_POST,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

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
        HTTP_METHOD_POST,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

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

  /**
   * Rename a file or directory.
   * If a source etag is passed in, the operation will attempt to recover
   * from a missing source file by probing the destination for
   * existence and comparing etags.
   * The second value in the result will be true to indicate that this
   * took place.
   * As rename recovery is only attempted if the source etag is non-empty,
   * in normal rename operations rename recovery will never happen.
   *
   * @param source                    path to source file
   * @param destination               destination of rename.
   * @param continuation              continuation.
   * @param tracingContext            trace context
   * @param sourceEtag                etag of source file. may be null or empty
   * @param isMetadataIncompleteState was there a rename failure due to
   *                                  incomplete metadata state?
   * @param isNamespaceEnabled        whether namespace enabled account or not
   * @return AbfsClientRenameResult result of rename operation indicating the
   * AbfsRest operation, rename recovery and incomplete metadata state failure.
   * @throws AzureBlobFileSystemException failure, excluding any recovery from overload failures.
   */
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

  public AbfsRestOperation append(final String path, final byte[] buffer,
      AppendRequestParameters reqParams, final String cachedSasToken,
      ContextEncryptionAdapter contextEncryptionAdapter, TracingContext tracingContext)
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
        HTTP_METHOD_PUT,
        url,
        requestHeaders,
        buffer,
        reqParams.getoffset(),
        reqParams.getLength(),
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
            HTTP_METHOD_PUT,
            url,
            requestHeaders,
            buffer,
            reqParams.getoffset(),
            reqParams.getLength(),
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

  public AbfsRestOperation flush(final String path, final long position,
      boolean retainUncommittedData, boolean isClose,
      final String cachedSasToken, final String leaseId,
      ContextEncryptionAdapter contextEncryptionAdapter, TracingContext tracingContext)
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
        HTTP_METHOD_PUT,
        url,
        requestHeaders, sasTokenForReuse);
    op.execute(tracingContext);
    return op;
  }

  @Override
  public AbfsRestOperation setPathProperties(final String path, final String properties,
      final TracingContext tracingContext, final ContextEncryptionAdapter contextEncryptionAdapter)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    addEncryptionKeyRequestHeaders(path, requestHeaders, false,
        contextEncryptionAdapter, tracingContext);
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
        HTTP_METHOD_PATCH));

    requestHeaders.add(new AbfsHttpHeader(X_MS_PROPERTIES, properties));

    final AbfsUriQueryBuilder abfsUriQueryBuilder
        = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, SET_PROPERTIES_ACTION);
    appendSASTokenToQuery(path, SASTokenProvider.SET_PROPERTIES_OPERATION,
        abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.SetPathProperties,
        HTTP_METHOD_PUT,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  @Override
  public AbfsRestOperation getPathStatus(final String path,
      final boolean includeProperties, final TracingContext tracingContext,
      final ContextEncryptionAdapter contextEncryptionAdapter)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    String operation = SASTokenProvider.GET_PROPERTIES_OPERATION;
    if (!includeProperties) {
      // The default action (operation) is implicitly to get properties and this action requires read permission
      // because it reads user defined properties.  If the action is getStatus or getAclStatus, then
      // only traversal (execute) permission is required.
      abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_ACTION, AbfsHttpConstants.GET_STATUS);
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
        HTTP_METHOD_HEAD,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

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
        HTTP_METHOD_GET,
        url,
        requestHeaders,
        buffer,
        bufferOffset,
        bufferLength, sasTokenForReuse);
    op.execute(tracingContext);

    // Verify the MD5 hash returned by server holds valid on the data received.
    if (isChecksumValidationEnabled(requestHeaders, rangeHeader, bufferLength)) {
      verifyCheckSumForRead(buffer, op.getResult(), bufferOffset);
    }

    return op;
  }

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
        AbfsHttpConstants.ApiVersion.AUG_03_2023) < 0)
        ? createDefaultHeaders(AbfsHttpConstants.ApiVersion.AUG_03_2023)
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
        AbfsRestOperationType.DeletePath,
        this,
        HTTP_METHOD_DELETE,
        url,
        requestHeaders);
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
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_ACTION, AbfsHttpConstants.SET_ACCESS_CONTROL);
    appendSASTokenToQuery(path, SASTokenProvider.SET_OWNER_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.SetOwner,
        AbfsHttpConstants.HTTP_METHOD_PUT,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

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
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_ACTION, AbfsHttpConstants.SET_ACCESS_CONTROL);
    appendSASTokenToQuery(path, SASTokenProvider.SET_PERMISSION_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.SetPermissions,
        AbfsHttpConstants.HTTP_METHOD_PUT,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

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
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_ACTION, AbfsHttpConstants.SET_ACCESS_CONTROL);
    appendSASTokenToQuery(path, SASTokenProvider.SET_ACL_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.SetAcl,
        AbfsHttpConstants.HTTP_METHOD_PUT,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  @Override
  public AbfsRestOperation getAclStatus(final String path, final boolean useUPN,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_ACTION, AbfsHttpConstants.GET_ACCESS_CONTROL);
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_UPN, String.valueOf(useUPN));
    appendSASTokenToQuery(path, SASTokenProvider.GET_ACL_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.GetAcl,
        AbfsHttpConstants.HTTP_METHOD_HEAD,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

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
        AbfsHttpConstants.HTTP_METHOD_HEAD,
        url,
        createDefaultHeaders());
    op.execute(tracingContext);
    return op;
  }

  @Override
  public boolean checkIsDir(AbfsHttpOperation result) {
    String resourceType = result.getResponseHeader(
        HttpHeaderConfigurations.X_MS_RESOURCE_TYPE);
    return resourceType != null
        && resourceType.equalsIgnoreCase(AbfsHttpConstants.DIRECTORY);
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
}
