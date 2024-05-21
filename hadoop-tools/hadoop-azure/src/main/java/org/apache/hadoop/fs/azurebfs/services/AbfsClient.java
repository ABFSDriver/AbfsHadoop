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
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsInvalidChecksumException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsDriverException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.StorageErrorResponseSchema;
import org.apache.hadoop.fs.azurebfs.utils.NamespaceUtil;
import org.apache.hadoop.fs.store.LogExactlyOnce;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore.Permissions;
import org.apache.hadoop.fs.azurebfs.extensions.EncryptionContextProvider;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.utils.EncryptionType;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.FutureCallback;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableScheduledFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningScheduledExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.SASTokenProviderException;
import org.apache.hadoop.fs.azurebfs.extensions.ExtensionHelper;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.RENAME_PATH_ATTEMPTS;
import static org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore.extractEtagHeader;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.*;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_DELETE_CONSIDERED_IDEMPOTENT;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.SERVER_SIDE_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.HTTPS_SCHEME;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.*;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.*;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_TIMEOUT_ABBREVIATION;

/**
 * AbfsClient.
 */
public abstract class AbfsClient implements Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(AbfsClient.class);
  public static final String HUNDRED_CONTINUE_USER_AGENT = SINGLE_WHITE_SPACE + HUNDRED_CONTINUE + SEMICOLON;

  private final URL baseUrl;
  private final SharedKeyCredentials sharedKeyCredentials;
  protected ApiVersion xMsVersion = ApiVersion.getCurrentVersion();
  private final ExponentialRetryPolicy exponentialRetryPolicy;
  private final StaticRetryPolicy staticRetryPolicy;
  private final String filesystem;
  protected final AbfsConfiguration abfsConfiguration;
  protected final String userAgent;
  private final AbfsPerfTracker abfsPerfTracker;
  private String clientProvidedEncryptionKey = null;
  private String clientProvidedEncryptionKeySHA = null;

  private final String accountName;
  protected final AuthType authType;
  private AccessTokenProvider tokenProvider;
  private SASTokenProvider sasTokenProvider;
  private final AbfsCounters abfsCounters;
  private EncryptionContextProvider encryptionContextProvider = null;
  private EncryptionType encryptionType = EncryptionType.NONE;
  private final AbfsThrottlingIntercept intercept;

  private final ListeningScheduledExecutorService executorService;
  private Boolean isNamespaceEnabled;

  protected boolean renameResilience;

  /**
   * logging the rename failure if metadata is in an incomplete state.
   */
  protected static final LogExactlyOnce ABFS_METADATA_INCOMPLETE_RENAME_FAILURE = new LogExactlyOnce(LOG);

  private AbfsClient(final URL baseUrl,
      final SharedKeyCredentials sharedKeyCredentials,
      final AbfsConfiguration abfsConfiguration,
      final EncryptionContextProvider encryptionContextProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    this.baseUrl = baseUrl;
    this.sharedKeyCredentials = sharedKeyCredentials;
    String baseUrlString = baseUrl.toString();
    this.filesystem = baseUrlString.substring(baseUrlString.lastIndexOf(FORWARD_SLASH) + 1);
    this.abfsConfiguration = abfsConfiguration;
    this.exponentialRetryPolicy = abfsClientContext.getExponentialRetryPolicy();
    this.staticRetryPolicy = abfsClientContext.getStaticRetryPolicy();
    this.accountName = abfsConfiguration.getAccountName().substring(0, abfsConfiguration.getAccountName().indexOf(AbfsHttpConstants.DOT));
    this.authType = abfsConfiguration.getAuthType(accountName);
    this.intercept = AbfsThrottlingInterceptFactory.getInstance(accountName, abfsConfiguration);
    this.renameResilience = abfsConfiguration.getRenameResilience();

    if (encryptionContextProvider != null) {
      this.encryptionContextProvider = encryptionContextProvider;
      xMsVersion = ApiVersion.APR_10_2021; // will be default once server change deployed
      encryptionType = EncryptionType.ENCRYPTION_CONTEXT;
    } else if (abfsConfiguration.getEncodedClientProvidedEncryptionKey() != null) {
      clientProvidedEncryptionKey =
          abfsConfiguration.getEncodedClientProvidedEncryptionKey();
      this.clientProvidedEncryptionKeySHA =
          abfsConfiguration.getEncodedClientProvidedEncryptionKeySHA();
      encryptionType = EncryptionType.GLOBAL_KEY;
    }

    String sslProviderName = null;

    if (this.baseUrl.toString().startsWith(HTTPS_SCHEME)) {
      try {
        LOG.trace("Initializing DelegatingSSLSocketFactory with {} SSL "
                + "Channel Mode", this.abfsConfiguration.getPreferredSSLFactoryOption());
        DelegatingSSLSocketFactory.initializeDefaultFactory(this.abfsConfiguration.getPreferredSSLFactoryOption());
        sslProviderName = DelegatingSSLSocketFactory.getDefaultFactory().getProviderName();
      } catch (IOException e) {
        // Suppress exception, failure to init DelegatingSSLSocketFactory would have only performance impact.
        LOG.trace("NonCritFailure: DelegatingSSLSocketFactory Init failed : "
            + "{}", e.getMessage());
      }
    }

    this.userAgent = initializeUserAgent(abfsConfiguration, sslProviderName);
    this.abfsPerfTracker = abfsClientContext.getAbfsPerfTracker();
    this.abfsCounters = abfsClientContext.getAbfsCounters();

    ThreadFactory tf =
        new ThreadFactoryBuilder().setNameFormat("AbfsClient Lease Ops").setDaemon(true).build();
    this.executorService = MoreExecutors.listeningDecorator(
        HadoopExecutors.newScheduledThreadPool(this.abfsConfiguration.getNumLeaseThreads(), tf));
  }

  public AbfsClient(final URL baseUrl, final SharedKeyCredentials sharedKeyCredentials,
                    final AbfsConfiguration abfsConfiguration,
                    final AccessTokenProvider tokenProvider,
                    final EncryptionContextProvider encryptionContextProvider,
                    final AbfsClientContext abfsClientContext)
      throws IOException {
    this(baseUrl, sharedKeyCredentials, abfsConfiguration,
        encryptionContextProvider, abfsClientContext);
    this.tokenProvider = tokenProvider;
  }

  public AbfsClient(final URL baseUrl, final SharedKeyCredentials sharedKeyCredentials,
                    final AbfsConfiguration abfsConfiguration,
                    final SASTokenProvider sasTokenProvider,
      final EncryptionContextProvider encryptionContextProvider,
                    final AbfsClientContext abfsClientContext)
      throws IOException {
    this(baseUrl, sharedKeyCredentials, abfsConfiguration,
        encryptionContextProvider, abfsClientContext);
    this.sasTokenProvider = sasTokenProvider;
  }

  @Override
  public void close() throws IOException {
    if (tokenProvider instanceof Closeable) {
      IOUtils.cleanupWithLogger(LOG,
          (Closeable) tokenProvider);
    }
    HadoopExecutors.shutdown(executorService, LOG, 0, TimeUnit.SECONDS);
  }

  public String getFileSystem() {
    return filesystem;
  }

  protected AbfsPerfTracker getAbfsPerfTracker() {
    return abfsPerfTracker;
  }

  ExponentialRetryPolicy getExponentialRetryPolicy() {
    return exponentialRetryPolicy;
  }

  StaticRetryPolicy getStaticRetryPolicy() {
    return staticRetryPolicy;
  }

  /**
   * Returns the retry policy to be used for Abfs Rest Operation Failure.
   * @param failureReason helps to decide which type of retryPolicy to be used.
   * @return retry policy to be used.
   */
  public AbfsRetryPolicy getRetryPolicy(final String failureReason) {
    return CONNECTION_TIMEOUT_ABBREVIATION.equals(failureReason)
        && getAbfsConfiguration().getStaticRetryForConnectionTimeoutEnabled()
        ? getStaticRetryPolicy()
        : getExponentialRetryPolicy();
  }

  SharedKeyCredentials getSharedKeyCredentials() {
    return sharedKeyCredentials;
  }

  public void setEncryptionType(EncryptionType encryptionType) {
    this.encryptionType = encryptionType;
  }

  public EncryptionType getEncryptionType() {
    return encryptionType;
  }

  AbfsThrottlingIntercept getIntercept() {
    return intercept;
  }

  /**
   * Create request headers for Rest Operation using the current API version.
   * @return default request headers
   */
  @VisibleForTesting
  public abstract List<AbfsHttpHeader> createDefaultHeaders();

  /**
   * Create request headers for Rest Operation using the specified API version.
   * @param xMsVersion azure services API version to be used.
   * @return default request headers
   */
  @VisibleForTesting
  public abstract List<AbfsHttpHeader> createDefaultHeaders(ApiVersion xMsVersion);

  /**
   * Create request headers common to both service endpoints.
   * @param xMsVersion azure services API version to be used.
   * @return common request headers
   */
  protected List<AbfsHttpHeader> createCommonHeaders(ApiVersion xMsVersion) {
    final List<AbfsHttpHeader> requestHeaders = new ArrayList<>();
    requestHeaders.add(new AbfsHttpHeader(X_MS_VERSION, xMsVersion.toString()));
    requestHeaders.add(new AbfsHttpHeader(ACCEPT_CHARSET, UTF_8));
    requestHeaders.add(new AbfsHttpHeader(CONTENT_TYPE, EMPTY_STRING));
    requestHeaders.add(new AbfsHttpHeader(USER_AGENT, userAgent));
    return requestHeaders;
  }

  /**
   * This method adds following headers:
   * <ol>
   *   <li>X_MS_ENCRYPTION_KEY</li>
   *   <li>X_MS_ENCRYPTION_KEY_SHA256</li>
   *   <li>X_MS_ENCRYPTION_ALGORITHM</li>
   * </ol>
   * Above headers have to be added in following operations:
   * <ol>
   *   <li>createPath</li>
   *   <li>append</li>
   *   <li>flush</li>
   *   <li>setPathProperties</li>
   *   <li>getPathStatus for fs.setXAttr and fs.getXAttr</li>
   *   <li>read</li>
   * </ol>
   */
  protected void addEncryptionKeyRequestHeaders(String path,
      List<AbfsHttpHeader> requestHeaders, boolean isCreateFileRequest,
      ContextEncryptionAdapter contextEncryptionAdapter, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      return;
    }
    String encodedKey, encodedKeySHA256;
    switch (encryptionType) {
    case GLOBAL_KEY:
      encodedKey = clientProvidedEncryptionKey;
      encodedKeySHA256 = clientProvidedEncryptionKeySHA;
      break;

    case ENCRYPTION_CONTEXT:
      if (isCreateFileRequest) {
        // get new context for create file request
        requestHeaders.add(new AbfsHttpHeader(X_MS_ENCRYPTION_CONTEXT,
            contextEncryptionAdapter.getEncodedContext()));
      }
      // else use cached encryption keys from input/output streams
      encodedKey = contextEncryptionAdapter.getEncodedKey();
      encodedKeySHA256 = contextEncryptionAdapter.getEncodedKeySHA();
      break;

    default: return; // no client-provided encryption keys
    }

    requestHeaders.add(new AbfsHttpHeader(X_MS_ENCRYPTION_KEY, encodedKey));
    requestHeaders.add(
        new AbfsHttpHeader(X_MS_ENCRYPTION_KEY_SHA256, encodedKeySHA256));
    requestHeaders.add(new AbfsHttpHeader(X_MS_ENCRYPTION_ALGORITHM,
        SERVER_SIDE_ENCRYPTION_ALGORITHM));
  }

  protected AbfsUriQueryBuilder createDefaultUriQueryBuilder() {
    final AbfsUriQueryBuilder abfsUriQueryBuilder = new AbfsUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_TIMEOUT, DEFAULT_TIMEOUT);
    return abfsUriQueryBuilder;
  }

  public abstract AbfsRestOperation createFilesystem(TracingContext tracingContext)
      throws AzureBlobFileSystemException;

  public abstract AbfsRestOperation setFilesystemProperties(final String properties,
      TracingContext tracingContext) throws AzureBlobFileSystemException;

  public abstract AbfsRestOperation listPath(final String relativePath,
      final boolean recursive,
      final int listMaxResults,
      final String continuation,
      TracingContext tracingContext) throws IOException;

  public abstract AbfsRestOperation getFilesystemProperties(TracingContext tracingContext)
      throws AzureBlobFileSystemException;

  public abstract AbfsRestOperation deleteFilesystem(TracingContext tracingContext)
      throws AzureBlobFileSystemException;

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
  public abstract AbfsRestOperation createPath(final String path,
      final boolean isFile,
      final boolean overwrite,
      final Permissions permissions,
      final boolean isAppendBlob,
      final String eTag,
      final ContextEncryptionAdapter contextEncryptionAdapter,
      final TracingContext tracingContext) throws AzureBlobFileSystemException;

  public abstract AbfsRestOperation acquireLease(final String path,
      final int duration,
      TracingContext tracingContext) throws AzureBlobFileSystemException;

  public abstract AbfsRestOperation renewLease(final String path,
      final String leaseId,
      TracingContext tracingContext) throws AzureBlobFileSystemException;

  public abstract AbfsRestOperation releaseLease(final String path,
      final String leaseId,
      TracingContext tracingContext) throws AzureBlobFileSystemException;

  public abstract AbfsRestOperation breakLease(final String path,
      TracingContext tracingContext) throws AzureBlobFileSystemException;

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
  public abstract AbfsClientRenameResult renamePath(
      final String source,
      final String destination,
      final String continuation,
      final TracingContext tracingContext,
      String sourceEtag,
      boolean isMetadataIncompleteState,
      boolean isNamespaceEnabled) throws IOException;

  public abstract boolean checkIsDir(AbfsHttpOperation result);

  @VisibleForTesting
  protected AbfsRestOperation createRenameRestOperation(URL url,
      List<AbfsHttpHeader> requestHeaders) {
    AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.RenamePath,
        HTTP_METHOD_PUT, url, requestHeaders);
    return op;
  }

  protected void incrementAbfsRenamePath() {
    abfsCounters.incrementCounter(RENAME_PATH_ATTEMPTS, 1);
  }

  /**
   * Check if the rename request failure is post a retry and if earlier rename
   * request might have succeeded at back-end.
   *
   * If a source etag was passed in, and the error was 404, get the
   * etag of any file at the destination.
   * If it matches the source etag, then the rename is considered
   * a success.
   * Exceptions raised in the probe of the destination are swallowed,
   * so that they do not interfere with the original rename failures.
   * @param source source path
   * @param op Rename request REST operation response with non-null HTTP response
   * @param destination rename destination path
   * @param sourceEtag etag of source file. may be null or empty
   * @param tracingContext Tracks identifiers for request header
   * @return true if the file was successfully copied
   */
  public boolean renameIdempotencyCheckOp(
      final String source,
      final String sourceEtag,
      final AbfsRestOperation op,
      final String destination,
      TracingContext tracingContext) {
    Preconditions.checkArgument(op.hasResult(), "Operations has null HTTP response");

    // removing isDir from debug logs as it can be misleading
    LOG.debug("rename({}, {}) failure {}; retry={} etag {}",
        source, destination, op.getResult().getStatusCode(), op.isARetriedRequest(), sourceEtag);
    if (!(op.isARetriedRequest()
        && (op.getResult().getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND))) {
      // only attempt recovery if the failure was a 404 on a retried rename request.
      return false;
    }

    if (isNotEmpty(sourceEtag)) {
      // Server has returned HTTP 404, we have an etag, so see
      // if the rename has actually taken place,
      LOG.info("rename {} to {} failed, checking etag of destination",
          source, destination);
      try {
        final AbfsRestOperation destStatusOp = getPathStatus(destination,
            false, tracingContext, null);
        final AbfsHttpOperation result = destStatusOp.getResult();

        final boolean recovered = result.getStatusCode() == HttpURLConnection.HTTP_OK
            && sourceEtag.equals(extractEtagHeader(result));
        LOG.info("File rename has taken place: recovery {}",
            recovered ? "succeeded" : "failed");
        return recovered;

      } catch (AzureBlobFileSystemException ex) {
        // GetFileStatus on the destination failed, the rename did not take place
        // or some other failure. log and swallow.
        LOG.debug("Failed to get status of path {}", destination, ex);
      }
    } else {
      LOG.debug("No source etag; unable to probe for the operation's success");
    }
    return false;
  }

  @VisibleForTesting
  boolean isSourceDestEtagEqual(String sourceEtag, AbfsHttpOperation result) {
    return sourceEtag.equals(extractEtagHeader(result));
  }

  public abstract AbfsRestOperation append(final String path,
      final byte[] buffer,
      AppendRequestParameters reqParams,
      final String cachedSasToken,
      ContextEncryptionAdapter contextEncryptionAdapter,
      TracingContext tracingContext) throws AzureBlobFileSystemException;

  /**
   * Returns true if the status code lies in the range of user error.
   * @param responseStatusCode http response status code.
   * @return True or False.
   */
  public abstract boolean checkUserError(int responseStatusCode);

  /**
   * To check if the failure exception returned by server is due to MD5 Mismatch
   * @param e Exception returned by AbfsRestOperation
   * @return boolean whether exception is due to MD5Mismatch or not
   */
  protected boolean isMd5ChecksumError(final AbfsRestOperationException e) {
    AzureServiceErrorCode storageErrorCode = e.getErrorCode();
    return storageErrorCode == AzureServiceErrorCode.MD5_MISMATCH;
  }

  // For AppendBlob its possible that the append succeeded in the backend but the request failed.
  // However a retry would fail with an InvalidQueryParameterValue
  // (as the current offset would be unacceptable).
  // Hence, we pass/succeed the appendblob append call
  // in case we are doing a retry after checking the length of the file
  public boolean appendSuccessCheckOp(AbfsRestOperation op,
      final String path,
      final long length,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    if ((op.isARetriedRequest())
        && (op.getResult().getStatusCode() == HttpURLConnection.HTTP_BAD_REQUEST)) {
      final AbfsRestOperation destStatusOp = getPathStatus(path, false, tracingContext, null);
      if (destStatusOp.getResult().getStatusCode() == HttpURLConnection.HTTP_OK) {
        String fileLength = destStatusOp.getResult().getResponseHeader(
            HttpHeaderConfigurations.CONTENT_LENGTH);
        if (length <= Long.parseLong(fileLength)) {
          LOG.debug("Returning success response from append blob idempotency code");
          return true;
        }
      }
    }
    return false;
  }

  public abstract AbfsRestOperation flush(final String path,
      final long position,
      boolean retainUncommittedData,
      boolean isClose,
      final String cachedSasToken,
      final String leaseId,
      ContextEncryptionAdapter contextEncryptionAdapter,
      TracingContext tracingContext) throws AzureBlobFileSystemException;

  public abstract AbfsRestOperation flush(byte[] buffer,
      final String path,
      boolean isClose,
      final String cachedSasToken,
      final String leaseId,
      final String eTag,
      final TracingContext tracingContext) throws AzureBlobFileSystemException;

  public abstract AbfsRestOperation setPathProperties(final String path,
      final String properties,
      final TracingContext tracingContext,
      final ContextEncryptionAdapter contextEncryptionAdapter)
      throws AzureBlobFileSystemException;

  public abstract AbfsRestOperation getPathStatus(final String path,
      final boolean includeProperties,
      final TracingContext tracingContext,
      final ContextEncryptionAdapter contextEncryptionAdapter)
      throws AzureBlobFileSystemException;

  public abstract AbfsRestOperation read(final String path,
      final long position,
      final byte[] buffer,
      final int bufferOffset,
      final int bufferLength,
      final String eTag,
      String cachedSasToken,
      ContextEncryptionAdapter contextEncryptionAdapter,
      TracingContext tracingContext) throws AzureBlobFileSystemException;

  public abstract AbfsRestOperation deletePath(final String path,
      final boolean recursive,
      final String continuation,
      TracingContext tracingContext,
      final boolean isNamespaceEnabled)
      throws AzureBlobFileSystemException;

  /**
   * Check if the delete request failure is post a retry and if delete failure
   * qualifies to be a success response assuming idempotency.
   *
   * There are below scenarios where delete could be incorrectly deducted as
   * success post request retry:
   * 1. Target was originally not existing and initial delete request had to be
   * re-tried.
   * 2. Parallel delete issued from any other store interface rather than
   * delete issued from this filesystem instance.
   * These are few corner cases and usually returning a success at this stage
   * should help the job to continue.
   * @param op Delete request REST operation response with non-null HTTP response
   * @return REST operation response post idempotency check
   */
  public AbfsRestOperation deleteIdempotencyCheckOp(final AbfsRestOperation op) {
    Preconditions.checkArgument(op.hasResult(), "Operations has null HTTP response");
    if ((op.isARetriedRequest())
        && (op.getResult().getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND)
        && DEFAULT_DELETE_CONSIDERED_IDEMPOTENT) {
      // Server has returned HTTP 404, which means path no longer
      // exists. Assuming delete result to be idempotent, return success.
      final AbfsRestOperation successOp = getAbfsRestOperation(
          AbfsRestOperationType.DeletePath,
          HTTP_METHOD_DELETE,
          op.getUrl(),
          op.getRequestHeaders());
      successOp.hardSetResult(HttpURLConnection.HTTP_OK);
      LOG.debug("Returning success response from delete idempotency logic");
      return successOp;
    }

    return op;
  }

  public abstract AbfsRestOperation setOwner(final String path,
      final String owner,
      final String group,
      TracingContext tracingContext) throws AzureBlobFileSystemException;

  public abstract AbfsRestOperation setPermission(final String path,
      final String permission,
      TracingContext tracingContext) throws AzureBlobFileSystemException;

  public AbfsRestOperation setAcl(final String path,
      final String aclSpecString,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    return setAcl(path, aclSpecString, AbfsHttpConstants.EMPTY_STRING, tracingContext);
  }

  public abstract AbfsRestOperation setAcl(final String path,
      final String aclSpecString,
      final String eTag,
      TracingContext tracingContext) throws AzureBlobFileSystemException;

  public AbfsRestOperation getAclStatus(final String path,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    return getAclStatus(path, abfsConfiguration.isUpnUsed(), tracingContext);
  }

  public abstract AbfsRestOperation getAclStatus(final String path,
      final boolean useUPN,
      TracingContext tracingContext) throws AzureBlobFileSystemException;

  /**
   * Talks to the server to check whether the permission specified in
   * the rwx parameter is present for the path specified in the path parameter.
   *
   * @param path  Path for which access check needs to be performed
   * @param rwx   The permission to be checked on the path
   * @param tracingContext Tracks identifiers for request header
   * @return      The {@link AbfsRestOperation} object for the operation
   * @throws AzureBlobFileSystemException in case of bad requests
   */
  public abstract AbfsRestOperation checkAccess(String path,
      String rwx,
      TracingContext tracingContext) throws AzureBlobFileSystemException;

  /**
   * Get the directory query parameter used by the List Paths REST API and used
   * as the path in the continuation token.  If the input path is null or the
   * root path "/", empty string is returned. If the input path begins with '/',
   * the return value is the substring beginning at offset 1.  Otherwise, the
   * input path is returned.
   * @param path the path to be listed.
   * @return the value of the directory query parameter
   */
  public static String getDirectoryQueryParameter(final String path) {
    String directory = path;
    if (Strings.isNullOrEmpty(directory)) {
      directory = AbfsHttpConstants.EMPTY_STRING;
    } else if (directory.charAt(0) == '/') {
      directory = directory.substring(1);
    }
    return directory;
  }

  /**
   * If configured for SAS AuthType, appends SAS token to queryBuilder.
   * @param path
   * @param operation
   * @param queryBuilder
   * @return sasToken - returned for optional re-use.
   * @throws SASTokenProviderException
   */
  protected String appendSASTokenToQuery(String path,
      String operation,
      AbfsUriQueryBuilder queryBuilder) throws SASTokenProviderException {
    return appendSASTokenToQuery(path, operation, queryBuilder, null);
  }

  /**
   * If configured for SAS AuthType, appends SAS token to queryBuilder.
   * @param path
   * @param operation
   * @param queryBuilder
   * @param cachedSasToken - previously acquired SAS token to be reused.
   * @return sasToken - returned for optional re-use.
   * @throws SASTokenProviderException
   */
  protected String appendSASTokenToQuery(String path,
      String operation,
      AbfsUriQueryBuilder queryBuilder,
      String cachedSasToken) throws SASTokenProviderException {
    String sasToken = null;
    if (this.authType == AuthType.SAS) {
      try {
        LOG.trace("Fetch SAS token for {} on {}", operation, path);
        if (cachedSasToken == null) {
          sasToken = sasTokenProvider.getSASToken(this.accountName,
              this.filesystem, path, operation);
          if ((sasToken == null) || sasToken.isEmpty()) {
            throw new UnsupportedOperationException("SASToken received is empty or null");
          }
        } else {
          sasToken = cachedSasToken;
          LOG.trace("Using cached SAS token.");
        }
        // if SAS Token contains a prefix of ?, it should be removed
        if (sasToken.charAt(0) == '?') {
          sasToken = sasToken.substring(1);
        }
        queryBuilder.setSASToken(sasToken);
        LOG.trace("SAS token fetch complete for {} on {}", operation, path);
      } catch (Exception ex) {
        throw new SASTokenProviderException(String.format("Failed to acquire a SAS token for %s on %s due to %s",
            operation,
            path,
            ex.toString()));
      }
    }
    return sasToken;
  }

  protected URL createRequestUrl(final String query) throws AzureBlobFileSystemException {
    return createRequestUrl(EMPTY_STRING, query);
  }

  @VisibleForTesting
  protected URL createRequestUrl(final String path, final String query)
          throws AzureBlobFileSystemException {
    final String base = baseUrl.toString();
    String encodedPath = path;
    try {
      encodedPath = urlEncode(path);
    } catch (AzureBlobFileSystemException ex) {
      LOG.debug("Unexpected error.", ex);
      throw new InvalidUriException(path);
    }

    final StringBuilder sb = new StringBuilder();
    sb.append(base);
    sb.append(encodedPath);
    sb.append(query);

    final URL url;
    try {
      url = new URL(sb.toString());
    } catch (MalformedURLException ex) {
      throw new InvalidUriException(sb.toString());
    }
    return url;
  }

  public static String urlEncode(final String value) throws AzureBlobFileSystemException {
    String encodedString;
    try {
      encodedString =  URLEncoder.encode(value, UTF_8)
          .replace(PLUS, PLUS_ENCODE)
          .replace(FORWARD_SLASH_ENCODE, FORWARD_SLASH);
    } catch (UnsupportedEncodingException ex) {
        throw new InvalidUriException(value);
    }

    return encodedString;
  }

  public synchronized String getAccessToken() throws IOException {
    if (tokenProvider != null) {
      return "Bearer " + tokenProvider.getToken().getAccessToken();
    } else {
      return null;
    }
  }

  private synchronized Boolean getIsNamespaceEnabled(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    if (isNamespaceEnabled == null) {
      setIsNamespaceEnabled(NamespaceUtil.isNamespaceEnabled(this,
          tracingContext));
    }
    return isNamespaceEnabled;
  }

  protected Boolean getIsPaginatedDeleteEnabled() {
    return abfsConfiguration.isPaginatedDeleteEnabled();
  }

  protected Boolean isPaginatedDelete(boolean isRecursiveDelete, boolean isNamespaceEnabled) {
    return getIsPaginatedDeleteEnabled() && isNamespaceEnabled && isRecursiveDelete;
  }

  public AuthType getAuthType() {
    return authType;
  }

  public EncryptionContextProvider getEncryptionContextProvider() {
    return encryptionContextProvider;
  }

  @VisibleForTesting
  String initializeUserAgent(final AbfsConfiguration abfsConfiguration,
      final String sslProviderName) {

    StringBuilder sb = new StringBuilder();

    sb.append(APN_VERSION);
    sb.append(SINGLE_WHITE_SPACE);
    sb.append(CLIENT_VERSION);
    sb.append(SINGLE_WHITE_SPACE);

    sb.append("(");

    sb.append(System.getProperty(JAVA_VENDOR)
        .replaceAll(SINGLE_WHITE_SPACE, EMPTY_STRING));
    sb.append(SINGLE_WHITE_SPACE);
    sb.append("JavaJRE");
    sb.append(SINGLE_WHITE_SPACE);
    sb.append(System.getProperty(JAVA_VERSION));
    sb.append(SEMICOLON);
    sb.append(SINGLE_WHITE_SPACE);

    sb.append(System.getProperty(OS_NAME)
        .replaceAll(SINGLE_WHITE_SPACE, EMPTY_STRING));
    sb.append(SINGLE_WHITE_SPACE);
    sb.append(System.getProperty(OS_VERSION));
    sb.append(FORWARD_SLASH);
    sb.append(System.getProperty(OS_ARCH));
    sb.append(SEMICOLON);

    appendIfNotEmpty(sb, sslProviderName, true);
    appendIfNotEmpty(sb,
        ExtensionHelper.getUserAgentSuffix(tokenProvider, EMPTY_STRING), true);

    if (abfsConfiguration.isExpectHeaderEnabled()) {
      sb.append(SINGLE_WHITE_SPACE);
      sb.append(HUNDRED_CONTINUE);
      sb.append(SEMICOLON);
    }

    sb.append(SINGLE_WHITE_SPACE);
    sb.append(abfsConfiguration.getClusterName());
    sb.append(FORWARD_SLASH);
    sb.append(abfsConfiguration.getClusterType());

    sb.append(")");

    appendIfNotEmpty(sb, abfsConfiguration.getCustomUserAgentPrefix(), false);

    return String.format(Locale.ROOT, sb.toString());
  }

  private void appendIfNotEmpty(StringBuilder sb, String regEx,
      boolean shouldAppendSemiColon) {
    if (regEx == null || regEx.trim().isEmpty()) {
      return;
    }
    sb.append(SINGLE_WHITE_SPACE);
    sb.append(regEx);
    if (shouldAppendSemiColon) {
      sb.append(SEMICOLON);
    }
  }

  /**
   * Add MD5 hash as request header to the append request.
   * @param requestHeaders to be updated with checksum header
   * @param reqParams for getting offset and length
   * @param buffer for getting input data for MD5 computation
   * @throws AbfsRestOperationException if Md5 computation fails
   */
  protected void addCheckSumHeaderForWrite(List<AbfsHttpHeader> requestHeaders,
      final AppendRequestParameters reqParams, final byte[] buffer)
      throws AbfsRestOperationException {
    String md5Hash = computeMD5Hash(buffer, reqParams.getoffset(),
        reqParams.getLength());
    requestHeaders.add(new AbfsHttpHeader(CONTENT_MD5, md5Hash));
  }

  /**
   * To verify the checksum information received from server for the data read.
   * @param buffer stores the data received from server.
   * @param result HTTP Operation Result.
   * @param bufferOffset Position where data returned by server is saved in buffer.
   * @throws AbfsRestOperationException if Md5Mismatch.
   */
  protected void verifyCheckSumForRead(final byte[] buffer,
      final AbfsHttpOperation result, final int bufferOffset)
      throws AbfsRestOperationException {
    // Number of bytes returned by server could be less than or equal to what
    // caller requests. In case it is less, extra bytes will be initialized to 0
    // Server returned MD5 Hash will be computed on what server returned.
    // We need to get exact data that server returned and compute its md5 hash
    // Computed hash should be equal to what server returned.
    int numberOfBytesRead = (int) result.getBytesReceived();
    if (numberOfBytesRead == 0) {
      return;
    }
    String md5HashComputed = computeMD5Hash(buffer, bufferOffset,
        numberOfBytesRead);
    String md5HashActual = result.getResponseHeader(CONTENT_MD5);
    if (!md5HashComputed.equals(md5HashActual)) {
      LOG.debug("Md5 Mismatch Error in Read Operation. Server returned Md5: {}, Client computed Md5: {}", md5HashActual, md5HashComputed);
      throw new AbfsInvalidChecksumException(result.getRequestId());
    }
  }

  /**
   * Conditions check for allowing checksum support for read operation.
   * Sending MD5 Hash in request headers. For more details see
   * @see <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/read">
   *     Path - Read Azure Storage Rest API</a>.
   * 1. Range header must be present as one of the request headers.
   * 2. buffer length must be less than or equal to 4 MB.
   * @param requestHeaders to be checked for range header.
   * @param rangeHeader must be present.
   * @param bufferLength must be less than or equal to 4 MB.
   * @return true if all conditions are met.
   */
  protected boolean isChecksumValidationEnabled(List<AbfsHttpHeader> requestHeaders,
      final AbfsHttpHeader rangeHeader, final int bufferLength) {
    return getAbfsConfiguration().getIsChecksumValidationEnabled()
        && requestHeaders.contains(rangeHeader) && bufferLength <= 4 * ONE_MB;
  }

  /**
   * Conditions check for allowing checksum support for write operation.
   * Server will support this if client sends the MD5 Hash as a request header.
   * For azure stoage service documentation see
   * @see <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update">
   *     Path - Update Azure Rest API</a>.
   * @return true if checksum validation enabled.
   */
  protected boolean isChecksumValidationEnabled() {
    return getAbfsConfiguration().getIsChecksumValidationEnabled();
  }

  /**
   * Compute MD5Hash of the given byte array starting from given offset up to given length.
   * @param data byte array from which data is fetched to compute MD5 Hash.
   * @param off offset in the array from where actual data starts.
   * @param len length of the data to be used to compute MD5Hash.
   * @return MD5 Hash of the data as String.
   * @throws AbfsRestOperationException if computation fails.
   */
  @VisibleForTesting
  public String computeMD5Hash(final byte[] data, final int off, final int len)
      throws AbfsRestOperationException {
    try {
      MessageDigest md5Digest = MessageDigest.getInstance(MD5);
      md5Digest.update(data, off, len);
      byte[] md5Bytes = md5Digest.digest();
      return Base64.getEncoder().encodeToString(md5Bytes);
    } catch (NoSuchAlgorithmException ex) {
      throw new AbfsDriverException(ex);
    }
  }

  @VisibleForTesting
  URL getBaseUrl() {
    return baseUrl;
  }

  @VisibleForTesting
  public SASTokenProvider getSasTokenProvider() {
    return this.sasTokenProvider;
  }

  @VisibleForTesting
  void setEncryptionContextProvider(EncryptionContextProvider provider) {
    encryptionContextProvider = provider;
  }

  @VisibleForTesting
  void setIsNamespaceEnabled(final Boolean isNamespaceEnabled) {
    this.isNamespaceEnabled = isNamespaceEnabled;
  }

  /**
   * Getter for abfsCounters from AbfsClient.
   * @return AbfsCounters instance.
   */
  protected AbfsCounters getAbfsCounters() {
    return abfsCounters;
  }

  public ApiVersion getxMsVersion() {
    return xMsVersion;
  }

  /**
   * Getter for abfsConfiguration from AbfsClient.
   * @return AbfsConfiguration instance
   */
  protected AbfsConfiguration getAbfsConfiguration() {
    return abfsConfiguration;
  }

  public int getNumLeaseThreads() {
    return abfsConfiguration.getNumLeaseThreads();
  }

  public <V> ListenableScheduledFuture<V> schedule(Callable<V> callable, long delay,
      TimeUnit timeUnit) {
    return executorService.schedule(callable, delay, timeUnit);
  }

  public ListenableFuture<?> submit(Runnable runnable) {
    return executorService.submit(runnable);
  }

  public <V> void addCallback(ListenableFuture<V> future, FutureCallback<V> callback) {
    Futures.addCallback(future, callback, executorService);
  }

  @VisibleForTesting
  protected AccessTokenProvider getTokenProvider() {
    return tokenProvider;
  }

  /**
   * Creates an AbfsRestOperation with additional parameters for buffer and SAS token.
   *
   * @param operationType    The type of the operation.
   * @param httpMethod       The HTTP method of the operation.
   * @param url              The URL associated with the operation.
   * @param requestHeaders   The list of HTTP headers for the request.
   * @param buffer           The byte buffer containing data for the operation.
   * @param bufferOffset     The offset within the buffer where the data starts.
   * @param bufferLength     The length of the data within the buffer.
   * @param sasTokenForReuse The SAS token for reusing authentication.
   * @return An AbfsRestOperation instance.
   */
  AbfsRestOperation getAbfsRestOperation(final AbfsRestOperationType operationType,
      final String httpMethod,
      final URL url,
      final List<AbfsHttpHeader> requestHeaders,
      final byte[] buffer,
      final int bufferOffset,
      final int bufferLength,
      final String sasTokenForReuse) {
    return new AbfsRestOperation(
        operationType,
        this,
        httpMethod,
        url,
        requestHeaders,
        buffer,
        bufferOffset,
        bufferLength,
        sasTokenForReuse);
  }

  /**
   * Creates an AbfsRestOperation with basic parameters and no buffer or SAS token.
   *
   * @param operationType   The type of the operation.
   * @param httpMethod      The HTTP method of the operation.
   * @param url             The URL associated with the operation.
   * @param requestHeaders  The list of HTTP headers for the request.
   * @return An AbfsRestOperation instance.
   */
  AbfsRestOperation getAbfsRestOperation(final AbfsRestOperationType operationType,
      final String httpMethod,
      final URL url,
      final List<AbfsHttpHeader> requestHeaders) {
    return new AbfsRestOperation(
        operationType,
        this,
        httpMethod,
        url,
        requestHeaders
    );
  }

  /**
   * Creates an AbfsRestOperation with parameters including request headers and SAS token.
   *
   * @param operationType    The type of the operation.
   * @param httpMethod       The HTTP method of the operation.
   * @param url              The URL associated with the operation.
   * @param requestHeaders   The list of HTTP headers for the request.
   * @param sasTokenForReuse The SAS token for reusing authentication.
   * @return An AbfsRestOperation instance.
   */
  AbfsRestOperation getAbfsRestOperation(final AbfsRestOperationType operationType,
      final String httpMethod,
      final URL url,
      final List<AbfsHttpHeader> requestHeaders,
      final String sasTokenForReuse) {
    return new AbfsRestOperation(
        operationType,
        this,
        httpMethod,
        url,
        requestHeaders, sasTokenForReuse);
  }

  public abstract ListResultSchema parseListPathResults(final InputStream stream) throws IOException;

  public abstract List<String> parseBlockListResponse(final InputStream stream) throws IOException;

  public abstract StorageErrorResponseSchema processStorageErrorResponse(final InputStream stream) throws IOException;

  public abstract String getContinuationFromResponse(AbfsHttpOperation result);
}
