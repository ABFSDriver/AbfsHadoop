/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.azurebfs;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.WeakHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.VisibleForTesting;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;
import org.apache.hadoop.fs.azurebfs.enums.BlobCopyProgress;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobLease;
import org.apache.hadoop.fs.azurebfs.services.AbfsDfsLease;
import org.apache.hadoop.fs.azurebfs.services.ListBlobConsumer;
import org.apache.hadoop.fs.azurebfs.services.ListBlobProducer;
import org.apache.hadoop.fs.azurebfs.services.ListBlobQueue;
import org.apache.hadoop.fs.azurebfs.services.OperativeEndpoint;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpHeader;
import org.apache.hadoop.fs.azurebfs.services.PrefixMode;
import org.apache.hadoop.fs.azurebfs.services.BlobList;
import org.apache.hadoop.fs.azurebfs.services.BlobProperty;

import org.apache.hadoop.fs.azurebfs.services.RenameAtomicityUtils;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.EtagSource;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.ConcurrentWriteOperationDetectedException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.FileSystemOperationUnhandledException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidAbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidFileSystemPropertyException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriAuthorityException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TrileanConversionException;
import org.apache.hadoop.fs.azurebfs.enums.Trilean;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.extensions.ExtensionHelper;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADAuthenticator;
import org.apache.hadoop.fs.azurebfs.oauth2.IdentityTransformer;
import org.apache.hadoop.fs.azurebfs.oauth2.IdentityTransformerInterface;
import org.apache.hadoop.fs.azurebfs.services.AbfsAclHelper;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientContextBuilder;
import org.apache.hadoop.fs.azurebfs.services.AbfsCounters;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamStatisticsImpl;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStreamContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStreamStatisticsImpl;
import org.apache.hadoop.fs.azurebfs.services.AbfsPermission;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.services.ExponentialRetryPolicy;
import org.apache.hadoop.fs.azurebfs.services.AbfsLease;
import org.apache.hadoop.fs.azurebfs.services.SharedKeyCredentials;
import org.apache.hadoop.fs.azurebfs.services.AbfsPerfTracker;
import org.apache.hadoop.fs.azurebfs.services.AbfsPerfInfo;
import org.apache.hadoop.fs.azurebfs.services.ListingSupport;
import org.apache.hadoop.fs.azurebfs.utils.Base64;
import org.apache.hadoop.fs.azurebfs.utils.CRC64;
import org.apache.hadoop.fs.azurebfs.utils.DateTimeUtils;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.UriUtils;
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.store.DataBlocks;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.SemaphoredDelegatingExecutor;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.http.client.utils.URIBuilder;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOB_LEASE_ONE_MINUTE_DURATION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FORWARD_SLASH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HDI_ISFOLDER;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_METADATA_PREFIX;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_EQUALS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_FORWARD_SLASH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_HYPHEN;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_PLUS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_STAR;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_UNDERSCORE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COPY_STATUS_ABORTED;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COPY_STATUS_FAILED;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COPY_STATUS_SUCCESS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DIRECTORY;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FILE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SINGLE_WHITE_SPACE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.TOKEN_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.TRUE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_ABFS_ENDPOINT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_BUFFERED_PREAD_DISABLE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_IDENTITY_TRANSFORM_CLASS;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HBASE_ROOT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_LENGTH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_COPY_ID;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_COPY_SOURCE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_COPY_STATUS;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_COPY_STATUS_DESCRIPTION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_META_HDI_ISFOLDER;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.COPY_BLOB_ABORTED;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.COPY_BLOB_FAILED;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.PATH_EXISTS;
import static org.apache.hadoop.fs.azurebfs.services.RenameAtomicityUtils.SUFFIX;

/**
 * Provides the bridging logic between Hadoop's abstract filesystem and Azure Storage.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AzureBlobFileSystemStore implements Closeable, ListingSupport {
  private static final Logger LOG = LoggerFactory.getLogger(AzureBlobFileSystemStore.class);

  private AbfsClient client;
  private URI uri;
  private String userName;
  private String primaryUserGroup;
  private static final String TOKEN_DATE_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS'Z'";
  private static final String XMS_PROPERTIES_ENCODING = "ISO-8859-1";
  private static final int GET_SET_AGGREGATE_COUNT = 2;

  private final Map<AbfsLease, Object> leaseRefs;

  private final AbfsConfiguration abfsConfiguration;
  private final Set<String> azureAtomicRenameDirSet;
  private Set<String> azureInfiniteLeaseDirSet;
  private Trilean isNamespaceEnabled;
  private final AuthType authType;
  private final UserGroupInformation userGroupInformation;
  private final IdentityTransformerInterface identityTransformer;
  private final AbfsPerfTracker abfsPerfTracker;
  private final AbfsCounters abfsCounters;
  private PrefixMode prefixMode;

  /**
   * The set of directories where we should store files as append blobs.
   */
  private Set<String> appendBlobDirSet;

  /** BlockFactory being used by this instance.*/
  private DataBlocks.BlockFactory blockFactory;
  /** Number of active data blocks per AbfsOutputStream */
  private int blockOutputActiveBlocks;
  /** Bounded ThreadPool for this instance. */
  private ExecutorService boundedThreadPool;
  WriteThreadPoolSizeManager poolSizeManager;

  /**
   * FileSystem Store for {@link AzureBlobFileSystem} for Abfs operations.
   * Built using the {@link AzureBlobFileSystemStoreBuilder} with parameters
   * required.
   * @param abfsStoreBuilder Builder for AzureBlobFileSystemStore.
   * @throws IOException Throw IOE in case of failure during constructing.
   */
  public AzureBlobFileSystemStore(
      AzureBlobFileSystemStoreBuilder abfsStoreBuilder) throws IOException {
    this.uri = abfsStoreBuilder.uri;
    String[] authorityParts = authorityParts(uri);
    final String fileSystemName = authorityParts[0];
    final String accountName = authorityParts[1];

    leaseRefs = Collections.synchronizedMap(new WeakHashMap<>());

    try {
      this.abfsConfiguration = new AbfsConfiguration(abfsStoreBuilder.configuration, accountName);
    } catch (IllegalAccessException exception) {
      throw new FileSystemOperationUnhandledException(exception);
    }

    LOG.trace("AbfsConfiguration init complete");

    this.isNamespaceEnabled = abfsConfiguration.getIsNamespaceEnabledAccount();

    this.userGroupInformation = UserGroupInformation.getCurrentUser();
    this.userName = userGroupInformation.getShortUserName();
    LOG.trace("UGI init complete");
    if (!abfsConfiguration.getSkipUserGroupMetadataDuringInitialization()) {
      try {
        this.primaryUserGroup = userGroupInformation.getPrimaryGroupName();
      } catch (IOException ex) {
        LOG.error("Failed to get primary group for {}, using user name as primary group name", userName);
        this.primaryUserGroup = userName;
      }
    } else {
      //Provide a default group name
      this.primaryUserGroup = userName;
    }
    LOG.trace("primaryUserGroup is {}", this.primaryUserGroup);

    this.azureAtomicRenameDirSet = new HashSet<>(Arrays.asList(
        abfsConfiguration.getAzureAtomicRenameDirs().split(AbfsHttpConstants.COMMA)));
    this.azureAtomicRenameDirSet.add(HBASE_ROOT);
    updateInfiniteLeaseDirs();
    this.authType = abfsConfiguration.getAuthType(accountName);
    boolean usingOauth = (authType == AuthType.OAuth);
    boolean useHttps = (usingOauth || abfsConfiguration.isHttpsAlwaysUsed()) ? true : abfsStoreBuilder.isSecureScheme;
    this.abfsPerfTracker = new AbfsPerfTracker(fileSystemName, accountName, this.abfsConfiguration);
    this.abfsCounters = abfsStoreBuilder.abfsCounters;
    initializeClient(uri, fileSystemName, accountName, useHttps);
    final Class<? extends IdentityTransformerInterface> identityTransformerClass =
        abfsStoreBuilder.configuration.getClass(FS_AZURE_IDENTITY_TRANSFORM_CLASS, IdentityTransformer.class,
            IdentityTransformerInterface.class);
    try {
      this.identityTransformer =
          identityTransformerClass.getConstructor(Configuration.class).newInstance(abfsStoreBuilder.configuration);
    } catch (IllegalAccessException | InstantiationException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException e) {
      throw new IOException(e);
    }
    LOG.trace("IdentityTransformer init complete");

    // Extract the directories that should contain append blobs
    String appendBlobDirs = abfsConfiguration.getAppendBlobDirs();
    if (appendBlobDirs.trim().isEmpty()) {
      this.appendBlobDirSet = new HashSet<String>();
    } else {
      this.appendBlobDirSet = new HashSet<>(Arrays.asList(
          abfsConfiguration.getAppendBlobDirs().split(AbfsHttpConstants.COMMA)));
    }
    this.blockFactory = abfsStoreBuilder.blockFactory;
    this.blockOutputActiveBlocks = abfsStoreBuilder.blockOutputActiveBlocks;
    this.poolSizeManager = WriteThreadPoolSizeManager.getInstance();
    poolSizeManager.startCPUMonitoring();
  }

  /**
   * Checks if the given key in Azure Storage should be stored as a page
   * blob instead of block blob.
   */
  public boolean isAppendBlobKey(String key) {
    return isKeyForDirectorySet(key, appendBlobDirSet);
  }

  /**
   * @return local user name.
   * */
  public String getUser() {
    return this.userName;
  }

  /**
  * @return primary group that user belongs to.
  * */
  public String getPrimaryGroup() {
    return this.primaryUserGroup;
  }

  public PrefixMode getPrefixMode() {
    if (prefixMode == null) {
      prefixMode = abfsConfiguration.getPrefixMode();
    }
    return prefixMode;
  }

  @Override
  public void close() throws IOException {
    List<ListenableFuture<?>> futures = new ArrayList<>();
    for (AbfsLease lease : leaseRefs.keySet()) {
      if (lease == null) {
        continue;
      }
      ListenableFuture<?> future = client.submit(() -> lease.free());
      futures.add(future);
    }
    try {
      Futures.allAsList(futures).get();
      // shutdown the threadPool and set it to null.
      HadoopExecutors.shutdown(boundedThreadPool, LOG,
          30, TimeUnit.SECONDS);
      boundedThreadPool = null;
    } catch (InterruptedException e) {
      LOG.error("Interrupted freeing leases", e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      LOG.error("Error freeing leases", e);
    } finally {
      IOUtils.cleanupWithLogger(LOG, client);
    }
  }

  byte[] encodeAttribute(String value) throws UnsupportedEncodingException {
    return value.getBytes(XMS_PROPERTIES_ENCODING);
  }

  String decodeAttribute(byte[] value) throws UnsupportedEncodingException {
    return new String(value, XMS_PROPERTIES_ENCODING);
  }

  private String[] authorityParts(URI uri) throws InvalidUriAuthorityException, InvalidUriException {
    final String authority = uri.getRawAuthority();
    if (null == authority) {
      throw new InvalidUriAuthorityException(uri.toString());
    }

    if (!authority.contains(AbfsHttpConstants.AZURE_DISTRIBUTED_FILE_SYSTEM_AUTHORITY_DELIMITER)) {
      throw new InvalidUriAuthorityException(uri.toString());
    }

    final String[] authorityParts = authority.split(AbfsHttpConstants.AZURE_DISTRIBUTED_FILE_SYSTEM_AUTHORITY_DELIMITER, 2);

    if (authorityParts.length < 2 || authorityParts[0] != null
        && authorityParts[0].isEmpty()) {
      final String errMsg = String
              .format("'%s' has a malformed authority, expected container name. "
                      + "Authority takes the form "
                      + FileSystemUriSchemes.ABFS_SCHEME + "://[<container name>@]<account name>",
                      uri.toString());
      throw new InvalidUriException(errMsg);
    }
    return authorityParts;
  }

  public boolean getIsNamespaceEnabled(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    try {
      return this.isNamespaceEnabled.toBoolean();
    } catch (TrileanConversionException e) {
      LOG.debug("isNamespaceEnabled is UNKNOWN; fall back and determine through"
          + " getAcl server call", e);
    }

    LOG.debug("Get root ACL status");
    try (AbfsPerfInfo perfInfo = startTracking("getIsNamespaceEnabled",
        "getAclStatus")) {
      AbfsRestOperation op = client
          .getAclStatus(AbfsHttpConstants.ROOT_PATH, tracingContext);
      perfInfo.registerResult(op.getResult());
      isNamespaceEnabled = Trilean.getTrilean(true);
      perfInfo.registerSuccess(true);
    } catch (AbfsRestOperationException ex) {
      // Get ACL status is a HEAD request, its response doesn't contain
      // errorCode
      // So can only rely on its status code to determine its account type.
      if (HttpURLConnection.HTTP_BAD_REQUEST != ex.getStatusCode()) {
        throw ex;
      }

      isNamespaceEnabled = Trilean.getTrilean(false);
    }

    return isNamespaceEnabled.toBoolean();
  }

  @VisibleForTesting
  URIBuilder getURIBuilder(final String hostName, boolean isSecure) {
    String scheme = isSecure ? FileSystemUriSchemes.HTTPS_SCHEME : FileSystemUriSchemes.HTTP_SCHEME;

    final URIBuilder uriBuilder = new URIBuilder();
    uriBuilder.setScheme(scheme);

    // For testing purposes, an IP address and port may be provided to override
    // the host specified in the FileSystem URI.  Also note that the format of
    // the Azure Storage Service URI changes from
    // http[s]://[account][domain-suffix]/[filesystem] to
    // http[s]://[ip]:[port]/[account]/[filesystem].
    String endPoint = abfsConfiguration.get(AZURE_ABFS_ENDPOINT);
    if (endPoint == null || !endPoint.contains(AbfsHttpConstants.COLON)) {
      uriBuilder.setHost(hostName);
      return uriBuilder;
    }

    // Split ip and port
    String[] data = endPoint.split(AbfsHttpConstants.COLON);
    if (data.length != 2) {
      throw new RuntimeException(String.format("ABFS endpoint is not set correctly : %s, "
              + "Do not specify scheme when using {IP}:{PORT}", endPoint));
    }
    uriBuilder.setHost(data[0].trim());
    uriBuilder.setPort(Integer.parseInt(data[1].trim()));
    uriBuilder.setPath("/" + UriUtils.extractAccountNameFromHostName(hostName));

    return uriBuilder;
  }

  public AbfsConfiguration getAbfsConfiguration() {
    return this.abfsConfiguration;
  }

  public Hashtable<String, String> getFilesystemProperties(
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    try (AbfsPerfInfo perfInfo = startTracking("getFilesystemProperties",
            "getFilesystemProperties")) {
      LOG.debug("getFilesystemProperties for filesystem: {}",
              client.getFileSystem());

      final Hashtable<String, String> parsedXmsProperties;
      final AbfsRestOperation op;

      if (getPrefixMode() == PrefixMode.BLOB) {
        parsedXmsProperties = getAndParseContainerMetadata(tracingContext, perfInfo);

        return parsedXmsProperties;
      }

      op = client.getFilesystemProperties(tracingContext);
      final String xMsProperties = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_PROPERTIES);

      parsedXmsProperties = parseCommaSeparatedXmsProperties(xMsProperties);
      perfInfo.registerResult(op.getResult()).registerSuccess(true);
      return parsedXmsProperties;
    }
  }

  public void setFilesystemProperties(
      final Hashtable<String, String> properties, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    if (properties == null || properties.isEmpty()) {
      LOG.trace("setFilesystemProperties no properties present");
      return;
    }

    LOG.debug("setFilesystemProperties for filesystem: {} with properties: {}",
            client.getFileSystem(),
            properties);

    try (AbfsPerfInfo perfInfo = startTracking("setFilesystemProperties",
            "setFilesystemProperties")) {
      if (getPrefixMode() == PrefixMode.BLOB) {
        parseAndSetContainerMetadata(properties, tracingContext, perfInfo);
      }

      final String commaSeparatedProperties;
      try {
        commaSeparatedProperties = convertXmsPropertiesToCommaSeparatedString(properties);
      } catch (CharacterCodingException ex) {
        throw new InvalidAbfsRestOperationException(ex);
      }

      final AbfsRestOperation op = client
          .setFilesystemProperties(commaSeparatedProperties, tracingContext);
      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    }
  }

  public Hashtable<String, String> getPathStatus(final Path path,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    try (AbfsPerfInfo perfInfo = startTracking("getPathStatus", "getPathStatus")){
      LOG.debug("getPathStatus for filesystem: {} path: {}",
              client.getFileSystem(),
              path);

      final Hashtable<String, String> parsedXmsProperties;
      final AbfsRestOperation op = client
          .getPathStatus(getRelativePath(path), true, tracingContext);
      perfInfo.registerResult(op.getResult());

      final String xMsProperties = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_PROPERTIES);

      parsedXmsProperties = parseCommaSeparatedXmsProperties(xMsProperties);

      perfInfo.registerSuccess(true);

      return parsedXmsProperties;
    }
  }

  public void setPathProperties(final Path path,
      final Hashtable<String, String> properties, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    try (AbfsPerfInfo perfInfo = startTracking("setPathProperties", "setPathProperties")){
      LOG.debug("setFilesystemProperties for filesystem: {} path: {} with properties: {}",
          client.getFileSystem(),
          path,
          properties);

      final String commaSeparatedProperties;
      try {
        commaSeparatedProperties = convertXmsPropertiesToCommaSeparatedString(properties);
      } catch (CharacterCodingException ex) {
        throw new InvalidAbfsRestOperationException(ex);
      }
      final AbfsRestOperation op = client
          .setPathProperties(getRelativePath(path), commaSeparatedProperties,
              tracingContext);
      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    }
  }

  /**
   * Orchestrates the copying of blob from given source to a given destination.
   * @param srcPath source path
   * @param dstPath destination path
   * @param copySrcLeaseId leaseId on the source
   * @param tracingContext object of TracingContext used for the tracing of the
   * server calls.
   *
   * @throws AzureBlobFileSystemException exception thrown from the server calls,
   * or if it is discovered that the copying is failed or aborted.
   */
  @VisibleForTesting
  void copyBlob(Path srcPath,
      Path dstPath,
      final String copySrcLeaseId, TracingContext tracingContext) throws AzureBlobFileSystemException {
    AbfsRestOperation copyOp = null;
    try {
      copyOp = client.copyBlob(srcPath, dstPath,
          copySrcLeaseId, tracingContext);
    } catch (AbfsRestOperationException ex) {
      if (ex.getStatusCode() == HttpURLConnection.HTTP_CONFLICT) {
        final BlobProperty dstBlobProperty = getBlobProperty(dstPath,
            tracingContext);
        try {
          if (dstBlobProperty.getCopySourceUrl() != null &&
              (ROOT_PATH + client.getFileSystem() + srcPath.toUri()
                  .getPath()).equals(
                  new URL(dstBlobProperty.getCopySourceUrl()).toURI()
                      .getPath())) {
            return;
          }
        } catch (URISyntaxException | MalformedURLException e) {
          throw new RuntimeException(e);
        }
      }
      throw ex;
    }
    final String progress = copyOp.getResult()
        .getResponseHeader(X_MS_COPY_STATUS);
    if (COPY_STATUS_SUCCESS.equalsIgnoreCase(progress)) {
      return;
    }
    final String copyId = copyOp.getResult().getResponseHeader(X_MS_COPY_ID);
    final long pollWait = abfsConfiguration.getBlobCopyProgressPollWaitMillis();
    while (handleCopyInProgress(dstPath, tracingContext, copyId)
        == BlobCopyProgress.PENDING) {
      try {
        Thread.sleep(pollWait);
      } catch (Exception e) {

      }
    }
  }

  /**
   * Verifies if the blob copy is success or a failure or still in progress.
   *
   * @param dstPath path of the destination for the copying
   * @param tracingContext object of tracingContext used for the tracing of the
   * server calls.
   * @param copyId id returned by server on the copy server-call. This id gets
   * attached to blob and is returned by GetBlobProperties API on the destination.
   *
   * @return true if copying is success, false if it is still in progress.
   *
   * @throws AzureBlobFileSystemException exception returned in making server call
   * for GetBlobProperties on the path. It can be thrown if the copyStatus is failure
   * or is aborted.
   */
  @VisibleForTesting
  BlobCopyProgress handleCopyInProgress(final Path dstPath,
      final TracingContext tracingContext,
      final String copyId) throws AzureBlobFileSystemException {
    BlobProperty blobProperty = getBlobProperty(dstPath,
        tracingContext);
    if (blobProperty != null && copyId.equals(blobProperty.getCopyId())) {
      if (COPY_STATUS_SUCCESS.equalsIgnoreCase(blobProperty.getCopyStatus())) {
        return BlobCopyProgress.SUCCESS;
      }
      if (COPY_STATUS_FAILED.equalsIgnoreCase(blobProperty.getCopyStatus())) {
        throw new AbfsRestOperationException(
            COPY_BLOB_FAILED.getStatusCode(), COPY_BLOB_FAILED.getErrorCode(),
            String.format("copy to path %s failed due to: %s",
                dstPath.toUri().getPath(), blobProperty.getCopyStatusDescription()),
            new Exception(COPY_BLOB_FAILED.getErrorCode()));
      }
      if (COPY_STATUS_ABORTED.equalsIgnoreCase(blobProperty.getCopyStatus())) {
        throw new AbfsRestOperationException(
            COPY_BLOB_ABORTED.getStatusCode(), COPY_BLOB_ABORTED.getErrorCode(),
            String.format("copy to path %s aborted", dstPath.toUri().getPath()),
            new Exception(COPY_BLOB_ABORTED.getErrorCode()));
      }
    }
    return BlobCopyProgress.PENDING;
  }

  /**
   * Gets the property for the blob over Blob Endpoint.
   *
   * @param blobPath blobPath for which property information is required
   * @param tracingContext object of TracingContext required for tracing server calls.
   * @return BlobProperty for the given path
   * @throws AzureBlobFileSystemException exception thrown from
   * {@link AbfsClient#getBlobProperty(Path, TracingContext)} call
   */
  BlobProperty getBlobProperty(Path blobPath,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    AbfsRestOperation op = client.getBlobProperty(blobPath, tracingContext);
    BlobProperty blobProperty = new BlobProperty();
    final AbfsHttpOperation opResult = op.getResult();
    blobProperty.setIsDirectory(opResult
        .getResponseHeader(X_MS_META_HDI_ISFOLDER) != null);
    blobProperty.setUrl(op.getUrl().toString());
    blobProperty.setCopyId(opResult.getResponseHeader(X_MS_COPY_ID));
    blobProperty.setPath(blobPath);
    blobProperty.setCopySourceUrl(opResult.getResponseHeader(X_MS_COPY_SOURCE));
    blobProperty.setCopyStatusDescription(
        opResult.getResponseHeader(X_MS_COPY_STATUS_DESCRIPTION));
    blobProperty.setCopyStatus(opResult.getResponseHeader(X_MS_COPY_STATUS));
    blobProperty.setContentLength(
        Long.parseLong(opResult.getResponseHeader(CONTENT_LENGTH)));
    blobProperty.setETag(extractEtagHeader(opResult));
    return blobProperty;
  }

  /**
   * Gets the property for the container(filesystem) over Blob Endpoint.
   *
   * @param tracingContext object of TracingContext required for tracing server calls.
   * @return BlobProperty for the given path
   * @throws AzureBlobFileSystemException exception thrown from
   * {@link AbfsClient#getBlobProperty(Path, TracingContext)} call
   */
  BlobProperty getContainerProperty(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    try (AbfsPerfInfo perfInfo = startTracking("getContainerProperty", "getContainerProperty")) {
      LOG.debug("getContainerProperty for filesystem: {}",
          client.getFileSystem());

      AbfsRestOperation op = client.getContainerProperty(tracingContext);
      perfInfo.registerResult(op.getResult()).registerSuccess(true);

      BlobProperty blobProperty = new BlobProperty();
      blobProperty.setIsDirectory(true);
      blobProperty.setPath(new Path(FORWARD_SLASH));

      return blobProperty;
    }
  }

  /**
   * Gets user-defined properties(metadata) of the blob over blob endpoint.
   * @param path
   * @param tracingContext
   * @return hashmap containing key value pairs for blob metadata
   * @throws AzureBlobFileSystemException
   */
  public Hashtable<String, String> getBlobMetadata(final Path path,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    try (AbfsPerfInfo perfInfo = startTracking("getBlobMetadata", "getBlobMetadata")) {
      LOG.debug("getBlobMetadata for filesystem: {} path: {}",
          client.getFileSystem(),
          path);

      final AbfsRestOperation op = client.getBlobMetadata(path, tracingContext);
      perfInfo.registerResult(op.getResult()).registerSuccess(true);

      final Hashtable<String, String> metadata = parseResponseHeadersToHashTable(op.getResult());
      return metadata;
    }
    catch (AbfsRestOperationException ex) {
      // The path does not exist explicitly.
      // Check here if the path is an implicit dir
      if (ex.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND && !path.isRoot()) {
        List<BlobProperty> blobProperties = getListBlobs(path, null, null,
            tracingContext, 2, true);
        if (blobProperties.size() == 0) {
          throw ex;
        }
        else {
          // Path exists as implicit directory.
          // Return empty hashmap for properties
          return new Hashtable<>();
        }
      }
      else {
        throw ex;
      }
    }
  }

  /**
   * Sets user-defined properties(metadata) of the blob over blob endpoint.
   * @param path on which metadata is to be set
   * @param metadata set of user-defined properties to be set
   * @param tracingContext
   * @throws AzureBlobFileSystemException
   */
  public void setBlobMetadata(final Path path,
      final Hashtable<String, String> metadata, TracingContext tracingContext)
      throws IOException {
    try (AbfsPerfInfo perfInfo = startTracking("setBlobMetadata", "setBlobMetadata")) {
      LOG.debug("setBlobMetadata for filesystem: {} path: {} with properties: {}",
          client.getFileSystem(),
          path,
          metadata);

      List<AbfsHttpHeader> metadataRequestHeaders = getRequestHeadersForMetadata(metadata);

      try {
        final AbfsRestOperation op = client.setBlobMetadata(path, metadataRequestHeaders, tracingContext);
        perfInfo.registerResult(op.getResult()).registerSuccess(true);
      } catch (AbfsRestOperationException ex) {
        // The path does not exist explicitly.
        // Check here if the path is an implicit dir
        if (ex.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
          List<BlobProperty> blobProperties = getListBlobs(
              path,null, null, tracingContext, 2, true);
          if (blobProperties.size() == 0) {
            throw ex;
          }
          else {
            // The path was an implicit blob. Create Marker and set metadata on it
            createDirectory(path, null, FsPermission.getDirDefault(),
                FsPermission.getUMask(
                    getAbfsConfiguration().getRawConfiguration()),
                false, tracingContext);

            boolean xAttrExists = metadata.containsKey(HDI_ISFOLDER);
            if (!xAttrExists) {
              metadata.put(HDI_ISFOLDER, TRUE);
              metadataRequestHeaders = getRequestHeadersForMetadata(metadata);
            }

            final AbfsRestOperation op = client.setBlobMetadata(path, metadataRequestHeaders, tracingContext);
            perfInfo.registerResult(op.getResult()).registerSuccess(true);
          }
        }
        else {
          throw ex;
        }
      }
    }
  }

  /**
   * Gets user-defined properties(metadata) of the container over blob endpoint.
   * @param tracingContext
   * @return hashmap containing key value pairs for container metadata
   * @throws AzureBlobFileSystemException
   */
  public Hashtable<String, String> getContainerMetadata(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    try (AbfsPerfInfo perfInfo = startTracking("getContainerMetadata", "getContainerMetadata")) {
      LOG.debug("getContainerMetadata for filesystem: {}", client.getFileSystem());

      return getAndParseContainerMetadata(tracingContext, perfInfo);
    }
  }

  /**
   * Sets user-defined properties(metadata) of the container over blob endpoint.
   * @param metadata set of user-defined properties to be set
   * @param tracingContext
   * @throws AzureBlobFileSystemException
   */
  public void setContainerMetadata(final Hashtable<String, String> metadata,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    try (AbfsPerfInfo perfInfo = startTracking("setContainerMetadata", "setContainerMetadata")) {
      LOG.debug("setContainerMetadata for filesystem: {} with properties: {}",
          client.getFileSystem(),
          metadata);

      parseAndSetContainerMetadata(metadata, tracingContext, perfInfo);
    }
  }

  /**
   * User-Defined Properties over blob endpoint are actually response headers
   * with prefix "x-ms-meta-". Each property is a different response header.
   * This parses all the headers, removes the prefix and create a hashmap.
   * @param result AbfsHttpOperation result containing response headers.
   * @return Hashmap defining user defined metadata.
   */
  private Hashtable<String, String> parseResponseHeadersToHashTable(
      AbfsHttpOperation result) {
    final Hashtable<String, String> metadata = new Hashtable<>();
    String name, value;

    final Map<String, List<String>> responseHeaders = result.getResponseHeaders();
    for (Map.Entry<String, List<String>> entry : responseHeaders.entrySet()) {
      name = entry.getKey();

      if (name != null && name.startsWith(X_MS_METADATA_PREFIX)) {
        value = entry.getValue().get(0);
        metadata.put(name.substring(X_MS_METADATA_PREFIX.length()), value);
      }
    }
    return metadata;
  }

  /**
   * User-defined properties over blob endpoint are required to be set
   * as request header with prefix "x-ms-meta-". Each property need to be made
   * into a different request header. This parses all the properties, add prefix
   * and create request headers.
   * @param metadata Hashmap
   * @return List of request headers to be passed with API call.
   */
  private List<AbfsHttpHeader> getRequestHeadersForMetadata(Hashtable<String, String> metadata) {
    final List<AbfsHttpHeader> headers = new ArrayList<AbfsHttpHeader>();

    for(Map.Entry<String,String> entry : metadata.entrySet()) {
      headers.add(new AbfsHttpHeader(X_MS_METADATA_PREFIX + entry.getKey(), entry.getValue()));
    }
    return headers;
  }

  private Hashtable<String, String> getAndParseContainerMetadata(TracingContext tracingContext,
      AbfsPerfInfo perfInfo) throws AzureBlobFileSystemException{
    final AbfsRestOperation op = client.getContainerMetadata(tracingContext);
    perfInfo.registerResult(op.getResult()).registerSuccess(true);

    return parseResponseHeadersToHashTable(op.getResult());
  }

  private void parseAndSetContainerMetadata(final Hashtable<String, String> metadata, TracingContext tracingContext,
      AbfsPerfInfo perfInfo) throws AzureBlobFileSystemException{
    final List<AbfsHttpHeader> metadataRequestHeaders = getRequestHeadersForMetadata(metadata);
    final AbfsRestOperation op = client.setContainerMetadata(metadataRequestHeaders, tracingContext);

    perfInfo.registerResult(op.getResult()).registerSuccess(true);
  }

  /**
   * Get the list of a blob on a give path, or blob starting with the given prefix.
   *
   * @param sourceDirBlobPath path from where the list of blob is required.
   * @param prefix Optional value to be provided. If provided, API call would have
   * prefix = given value. If not provided, the API call would have prefix =
   * sourceDirBlobPath.
   * @param tracingContext object of {@link TracingContext}
   * @param maxResult defines maximum blobs the method should process
   * @param isDefinitiveDirSearch defines if (true) it is blobList search on a
   * definitive directory, if (false) it is blobList search on a prefix.
   *
   * @return List of blobProperties
   *
   * @throws AbfsRestOperationException exception from server-calls / xml-parsing
   */
  public List<BlobProperty> getListBlobs(Path sourceDirBlobPath,
      String prefix, String delimiter, TracingContext tracingContext,
      final Integer maxResult, final Boolean isDefinitiveDirSearch)
      throws AzureBlobFileSystemException {
    List<BlobProperty> blobProperties = new ArrayList<>();
    String nextMarker = null;
    if (prefix == null) {
      prefix = (!sourceDirBlobPath.isRoot()
          ? sourceDirBlobPath.toUri().getPath()
          : EMPTY_STRING) + (isDefinitiveDirSearch
          ? ROOT_PATH
          : EMPTY_STRING);
    }
    if (delimiter == null) {
      delimiter = "";
    }
    do {
      AbfsRestOperation op = getClient().getListBlobs(
          nextMarker, prefix, delimiter, maxResult, tracingContext
      );
      BlobList blobList = op.getResult().getBlobList();
      nextMarker = blobList.getNextMarker();
      blobProperties.addAll(blobList.getBlobPropertyList());
      if (maxResult != null && blobProperties.size() >= maxResult) {
        break;
      }
    } while (nextMarker != null);
    return blobProperties;
  }

  public void createFilesystem(TracingContext tracingContext, final boolean useBlobEndpoint)
      throws AzureBlobFileSystemException {
    try (AbfsPerfInfo perfInfo = startTracking("createFilesystem", "createFilesystem")){
      LOG.debug("createFilesystem for filesystem: {}",
              client.getFileSystem());
      final AbfsRestOperation op;
      if (useBlobEndpoint) {
        op = client.createContainer(tracingContext);
      } else {
        op = client.createFilesystem(tracingContext);
      }
      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    }
  }

  public void deleteFilesystem(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    try (AbfsPerfInfo perfInfo = startTracking("deleteFilesystem", "deleteFilesystem")) {
      LOG.debug("deleteFilesystem for filesystem: {}",
              client.getFileSystem());

      final AbfsRestOperation op;
      if (getPrefixMode() == PrefixMode.BLOB) {
        op = client.deleteContainer(tracingContext);
      } else {
        op = client.deleteFilesystem(tracingContext);
      }
      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    }
  }

  /**
   * Checks if we are creating a normal blob or markerFile.
   * @param metadata takes metadata as param.
   * @return true or false.
   */
  private boolean checkIsBlobOrMarker(HashMap<String, String> metadata) {
    return metadata != null && TRUE.equalsIgnoreCase(metadata.get(X_MS_META_HDI_ISFOLDER));
  }

  private AbfsRestOperation createPath(final String path, final boolean isFile, final boolean overwrite,
                                      final String permission, final String umask,
                                      final boolean isAppendBlob, final String eTag,
                                      TracingContext tracingContext) throws AzureBlobFileSystemException {
    return client.createPath(path, isFile, overwrite, permission, umask, isAppendBlob, eTag, tracingContext);
  }

  private AbfsRestOperation createPathBlob(final String path, final boolean isFile, final boolean overwrite,
                                          final HashMap<String, String> metadata,
                                          final String eTag,
                                          TracingContext tracingContext) throws AzureBlobFileSystemException {
    return client.createPathBlob(path, isFile, overwrite, metadata, eTag, tracingContext);
  }

  private AbfsRestOperation createFileOrMarker(boolean isNormalBlob, String relativePath, boolean isNamespaceEnabled,
                                               boolean overwrite, HashMap<String, String> metadata, TracingContext tracingContext,
                                               FsPermission permission, FsPermission umask, boolean isAppendBlob, String eTag) throws AzureBlobFileSystemException {
    AbfsRestOperation op;
    if (!isNormalBlob) {
      // Marker blob creation flow.
      if (OperativeEndpoint.isMkdirEnabledOnDFS(abfsConfiguration)) {
        LOG.debug("DFS fallback enabled and incorrect mkdir flow is hit for path {} and config value {} ",
                relativePath, abfsConfiguration.shouldMkdirFallbackToDfs());
        // Marker blob creation is not possible with dfs endpoint.
        throw new InvalidConfigurationValueException("Incorrect flow for create directory for dfs is hit " +
                relativePath);
      } else {
        LOG.debug("Path created via blob for mkdir call for path {} and config value {} ",
                relativePath, abfsConfiguration.shouldMkdirFallbackToDfs());
        op = createPathBlob(relativePath, false, overwrite, metadata, eTag, tracingContext);
      }
    } else {
      // Normal blob creation flow. If config for fallback is not enabled and prefix mode is blob go to blob, else go to dfs.
      if (!OperativeEndpoint.isIngressEnabledOnDFS(getPrefixMode(), abfsConfiguration)) {
        LOG.debug("Path created via blob endpoint for path {} and config value {} ",
                relativePath, abfsConfiguration.shouldIngressFallbackToDfs());
        op = createPathBlob(relativePath, true, overwrite, metadata, eTag, tracingContext);
      } else {
        LOG.debug("Path created via dfs endpoint for path {} and config value {} ",
                relativePath, abfsConfiguration.shouldIngressFallbackToDfs());
        op = createPath(relativePath, true, overwrite, isNamespaceEnabled ? getOctalNotation(permission) : null,
                isNamespaceEnabled ? getOctalNotation(umask) : null, isAppendBlob, eTag, tracingContext);
      }
    }
    return op;
  }

  // Fallback plan : default to v1 create flow which will hit dfs endpoint. Config to enable: "fs.azure.ingress.fallback.to.dfs".
  public AbfsOutputStream createFile(final Path path, final FileSystem.Statistics statistics, final boolean overwrite,
      final FsPermission permission, final FsPermission umask,
      TracingContext tracingContext, HashMap<String, String> metadata) throws IOException {
    try (AbfsPerfInfo perfInfo = startTracking("createFile", "createPath")) {
      boolean isNamespaceEnabled = getIsNamespaceEnabled(tracingContext);
      LOG.debug("createFile filesystem: {} path: {} overwrite: {} permission: {} umask: {} isNamespaceEnabled: {}",
              client.getFileSystem(),
              path,
              overwrite,
              permission,
              umask,
              isNamespaceEnabled);

      String relativePath = getRelativePath(path);
      boolean isAppendBlob = false;
      if (isAppendBlobKey(path.toString())) {
        isAppendBlob = true;
      }

      // if "fs.azure.enable.conditional.create.overwrite" is enabled and
      // is a create request with overwrite=true, create will follow different
      // flow.
      boolean triggerConditionalCreateOverwrite = false;
      if (overwrite
          && abfsConfiguration.isConditionalCreateOverwriteEnabled()) {
        triggerConditionalCreateOverwrite = true;
      }

      AbfsRestOperation op;
      if (triggerConditionalCreateOverwrite) {
        op = conditionalCreateOverwriteFile(relativePath,
            statistics,
            isNamespaceEnabled,
            permission,
            umask,
            isAppendBlob,
            metadata,
            tracingContext
        );

      } else {
        boolean isNormalBlob = !checkIsBlobOrMarker(metadata);
        op = createFileOrMarker(isNormalBlob, relativePath, isNamespaceEnabled, overwrite, metadata,
                tracingContext, permission, umask, isAppendBlob, null);
      }
      perfInfo.registerResult(op.getResult()).registerSuccess(true);

      AbfsLease lease = maybeCreateLease(relativePath, tracingContext);
      String eTag = op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG);
      checkAppendSmallWrite(isAppendBlob);

      return new AbfsOutputStream(
          populateAbfsOutputStreamContext(
              isAppendBlob,
              lease,
              client,
              statistics,
              relativePath,
              0,
              eTag,
              tracingContext, this.poolSizeManager));
    }
  }

  /**
   * Conditional create overwrite flow ensures that create overwrites is done
   * only if there is match for eTag of existing file.
   * @param relativePath
   * @param statistics
   * @param permission
   * @param umask
   * @param isAppendBlob
   * @return
   * @throws AzureBlobFileSystemException
   */
  private AbfsRestOperation conditionalCreateOverwriteFile(final String relativePath,
      final FileSystem.Statistics statistics,
      boolean isNamespaceEnabled,
      final FsPermission permission,
      final FsPermission umask,
      final boolean isAppendBlob,
      HashMap<String, String> metadata,
      TracingContext tracingContext) throws IOException {
    AbfsRestOperation op;
    VersionedFileStatus fileStatus;
    try {
      // Trigger a create with overwrite=false first so that eTag fetch can be
      // avoided for cases when no pre-existing file is present (major portion
      // of create file traffic falls into the case of no pre-existing file).
      boolean isNormalBlob = !checkIsBlobOrMarker(metadata);
      op = createFileOrMarker(isNormalBlob, relativePath, isNamespaceEnabled, false, metadata,
              tracingContext, permission, umask, isAppendBlob, null);
    } catch (AbfsRestOperationException e) {
      if (e.getStatusCode() == HTTP_CONFLICT) {
        // File pre-exists, fetch eTag
        try {
          boolean useBlobEndpoint = getPrefixMode() == PrefixMode.BLOB;
          if (OperativeEndpoint.isIngressEnabledOnDFS(
                  getAbfsConfiguration().getPrefixMode(), getAbfsConfiguration())) {
            LOG.debug("GetFileStatus over DFS for create for ingress config value {} for path {} ",
                    abfsConfiguration.shouldIngressFallbackToDfs(), relativePath);
            useBlobEndpoint = false;
          }
          fileStatus = (VersionedFileStatus) getFileStatus(new Path(relativePath), tracingContext, useBlobEndpoint);
        } catch (AbfsRestOperationException ex) {
          if (ex.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
            // Is a parallel access case, as file which was found to be
            // present went missing by this request.
            throw new ConcurrentWriteOperationDetectedException(
                "Parallel access to the create path detected. Failing request "
                    + "to honor single writer semantics");
          } else {
            throw ex;
          }
        }

        String eTag = fileStatus.getEtag();

        try {
          // overwrite only if eTag matches with the file properties fetched before.
          boolean isNormalBlob = !checkIsBlobOrMarker(metadata);
          op = createFileOrMarker(isNormalBlob, relativePath, isNamespaceEnabled, true, metadata,
                  tracingContext, permission, umask, isAppendBlob, eTag);
        } catch (AbfsRestOperationException ex) {
          if (ex.getStatusCode() == HttpURLConnection.HTTP_PRECON_FAILED) {
            // Is a parallel access case, as file with eTag was just queried
            // and precondition failure can happen only when another file with
            // different etag got created.
            throw new ConcurrentWriteOperationDetectedException(
                "Parallel access to the create path detected. Failing request "
                    + "to honor single writer semantics");
          } else {
            throw ex;
          }
        }
      } else {
        throw e;
      }
    }

    return op;
  }

  /**
   * Method to populate AbfsOutputStreamContext with different parameters to
   * be used to construct {@link AbfsOutputStream}.
   *
   * @param isAppendBlob   is Append blob support enabled?
   * @param lease          instance of AbfsLease for this AbfsOutputStream.
   * @param client         AbfsClient.
   * @param statistics     FileSystem statistics.
   * @param path           Path for AbfsOutputStream.
   * @param position       Position or offset of the file being opened, set to 0
   *                       when creating a new file, but needs to be set for APPEND
   *                       calls on the same file.
   * @param tracingContext instance of TracingContext for this AbfsOutputStream.
   * @return AbfsOutputStreamContext instance with the desired parameters.
   */
  private AbfsOutputStreamContext populateAbfsOutputStreamContext(
      boolean isAppendBlob,
      AbfsLease lease,
      AbfsClient client,
      FileSystem.Statistics statistics,
      String path,
      long position,
      String eTag,
      TracingContext tracingContext, WriteThreadPoolSizeManager poolSizeManager) {
    int bufferSize = abfsConfiguration.getWriteBufferSize();
    if (isAppendBlob && bufferSize > FileSystemConfigurations.APPENDBLOB_MAX_WRITE_BUFFER_SIZE) {
      bufferSize = FileSystemConfigurations.APPENDBLOB_MAX_WRITE_BUFFER_SIZE;
    }
    return new AbfsOutputStreamContext(abfsConfiguration.getSasTokenRenewPeriodForStreamsInSeconds())
            .withWriteBufferSize(bufferSize)
            .enableExpectHeader(abfsConfiguration.isExpectHeaderEnabled())
            .enableFlush(abfsConfiguration.isFlushEnabled())
            .enableSmallWriteOptimization(abfsConfiguration.isSmallWriteOptimizationEnabled())
            .disableOutputStreamFlush(abfsConfiguration.isOutputStreamFlushDisabled())
            .withStreamStatistics(new AbfsOutputStreamStatisticsImpl())
            .withAppendBlob(isAppendBlob)
            .withWriteMaxConcurrentRequestCount(abfsConfiguration.getWriteMaxConcurrentRequestCount())
            .withMaxWriteRequestsToQueue(abfsConfiguration.getMaxWriteRequestsToQueue())
            .withLease(lease)
            .withBlockFactory(blockFactory)
            .withBlockOutputActiveBlocks(blockOutputActiveBlocks)
            .withClient(client)
            .withPosition(position)
            .withFsStatistics(statistics)
            .withPath(path)
            .withETag(eTag)
            .withExecutorService(new SemaphoredDelegatingExecutor(boundedThreadPool,
                blockOutputActiveBlocks, true))
            .withTracingContext(tracingContext)
            .withPoolSizeManager(poolSizeManager)
            .build();
  }

  public String createDirectory(final Path path, final FileSystem.Statistics statistics, final FsPermission permission,
      final FsPermission umask,
      final Boolean checkParentChain,
      TracingContext tracingContext)
          throws IOException {
    try (AbfsPerfInfo perfInfo = startTracking("createDirectory", "createPath")) {
      if (!OperativeEndpoint.isMkdirEnabledOnDFS(abfsConfiguration)) {
        LOG.debug("Mkdir created via blob endpoint for the given path {} and config value {} ",
                path, abfsConfiguration.shouldMkdirFallbackToDfs());
        ArrayList<Path> keysToCreateAsFolder = new ArrayList<>();
        if (checkParentChain) {
          checkParentChainForFile(path, tracingContext, keysToCreateAsFolder);
        }
        boolean blobOverwrite = abfsConfiguration.isEnabledBlobMkdirOverwrite();

        AbfsOutputStream pathDirectoryOutputStream = createDirectoryMarkerBlob(
            path, statistics, permission, umask, tracingContext,
            blobOverwrite);
        for (Path pathToCreate: keysToCreateAsFolder) {
          createDirectoryMarkerBlob(pathToCreate, statistics, permission, umask,
              tracingContext, blobOverwrite);
        }
        return pathDirectoryOutputStream.getETag();
      }
      boolean isNamespaceEnabled = getIsNamespaceEnabled(tracingContext);
      LOG.debug("Mkdir created via dfs endpoint for the given path {} and config value {} ",
              path, abfsConfiguration.shouldMkdirFallbackToDfs());
      LOG.debug("createDirectory filesystem: {} path: {} permission: {} umask: {} isNamespaceEnabled: {}",
              client.getFileSystem(),
              path,
              permission,
              umask,
              isNamespaceEnabled);

      boolean overwrite =
              !isNamespaceEnabled || abfsConfiguration.isEnabledMkdirOverwrite();
      final AbfsRestOperation op = client.createPath(getRelativePath(path),
              false, overwrite,
              isNamespaceEnabled ? getOctalNotation(permission) : null,
              isNamespaceEnabled ? getOctalNotation(umask) : null, false, null,
              tracingContext);
      perfInfo.registerResult(op.getResult()).registerSuccess(true);
      return op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG);
    }
  }

  private AbfsOutputStream createDirectoryMarkerBlob(final Path path,
      final FileSystem.Statistics statistics,
      final FsPermission permission,
      final FsPermission umask,
      final TracingContext tracingContext,
      final boolean blobOverwrite) throws IOException {
    HashMap<String, String> metadata = new HashMap<>();
    metadata.put(X_MS_META_HDI_ISFOLDER, TRUE);
    return createFile(path, statistics, blobOverwrite,
        permission, umask, tracingContext, metadata);
  }

  /**
   * Checks for the entire parent hierarchy and returns if any directory exists and
   * throws an exception if any file exists.
   * @param path path to check the hierarchy for.
   * @param tracingContext the tracingcontext.
   */
  private void checkParentChainForFile(Path path, TracingContext tracingContext,
                                       List<Path> keysToCreateAsFolder) throws IOException {

    FileStatus fileStatus = tryGetPathProperty(path, tracingContext, true);
    Boolean isDirectory = fileStatus != null ? fileStatus.isDirectory() : false;
    if (fileStatus != null && !isDirectory) {
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
      fileStatus = tryGetPathProperty(current, tracingContext, true);
      isDirectory = fileStatus != null ? fileStatus.isDirectory() : false;
      if (fileStatus != null && !isDirectory) {
        throw new AbfsRestOperationException(HTTP_CONFLICT,
                AzureServiceErrorCode.PATH_CONFLICT.getErrorCode(),
                PATH_EXISTS,
                null);
      }
      if (isDirectory) {
        break;
      }
      keysToCreateAsFolder.add(current);
      current = current.getParent();
    }
  }

  public AbfsInputStream openFileForRead(final Path path,
      final FileSystem.Statistics statistics, TracingContext tracingContext)
      throws IOException {
    return openFileForRead(path, Optional.empty(), statistics,
        tracingContext);
  }

  public AbfsInputStream openFileForRead(Path path,
      final Optional<OpenFileParameters> parameters,
      final FileSystem.Statistics statistics, TracingContext tracingContext)
      throws IOException {
    try (AbfsPerfInfo perfInfo = startTracking("openFileForRead",
        "getPathStatus")) {
      LOG.debug("openFileForRead filesystem: {} path: {}",
          client.getFileSystem(), path);

      FileStatus parameterFileStatus = parameters.map(OpenFileParameters::getStatus)
          .orElse(null);
      String relativePath = getRelativePath(path);

      final VersionedFileStatus fileStatus;
      if (parameterFileStatus instanceof VersionedFileStatus) {
        Preconditions.checkArgument(parameterFileStatus.getPath()
                .equals(path.makeQualified(this.uri, path)),
            String.format(
                "Filestatus path [%s] does not match with given path [%s]",
                parameterFileStatus.getPath(), path));
        fileStatus = (VersionedFileStatus) parameterFileStatus;
      } else {
        if (parameterFileStatus != null) {
          LOG.warn(
              "Fallback to getPathStatus REST call as provided filestatus "
                  + "is not of type VersionedFileStatus");
        }
        boolean useBlobEndpoint = getPrefixMode() == PrefixMode.BLOB;
        if (OperativeEndpoint.isReadEnabledOnDFS(getAbfsConfiguration())) {
          LOG.debug("GetFileStatus over DFS for open file for read for read config value {} for path {} ",
              abfsConfiguration.shouldReadFallbackToDfs(), path);
          useBlobEndpoint = false;
        }
        fileStatus = (VersionedFileStatus) getFileStatus(path, tracingContext, useBlobEndpoint);
      }

      boolean isDirectory = fileStatus.isDirectory();

      final long contentLength = fileStatus.getLen();
      final String eTag = fileStatus.getEtag();

      if (isDirectory) {
        throw new AbfsRestOperationException(
                AzureServiceErrorCode.PATH_NOT_FOUND.getStatusCode(),
                AzureServiceErrorCode.PATH_NOT_FOUND.getErrorCode(),
                "openFileForRead must be used with files and not directories." +
                        "Attempt made for read on explicit directory.",
                null);
      }

      perfInfo.registerSuccess(true);

      // Add statistics for InputStream
      return new AbfsInputStream(client, statistics, relativePath,
          contentLength, populateAbfsInputStreamContext(
          parameters.map(OpenFileParameters::getOptions)),
          eTag, tracingContext);
    }
  }

  private AbfsInputStreamContext populateAbfsInputStreamContext(
      Optional<Configuration> options) {
    boolean bufferedPreadDisabled = options
        .map(c -> c.getBoolean(FS_AZURE_BUFFERED_PREAD_DISABLE, false))
        .orElse(false);
    return new AbfsInputStreamContext(abfsConfiguration.getSasTokenRenewPeriodForStreamsInSeconds())
            .withReadBufferSize(abfsConfiguration.getReadBufferSize())
            .withReadAheadQueueDepth(abfsConfiguration.getReadAheadQueueDepth())
            .withTolerateOobAppends(abfsConfiguration.getTolerateOobAppends())
            .isReadAheadEnabled(abfsConfiguration.isReadAheadEnabled())
            .withReadSmallFilesCompletely(abfsConfiguration.readSmallFilesCompletely())
            .withOptimizeFooterRead(abfsConfiguration.optimizeFooterRead())
            .withReadAheadRange(abfsConfiguration.getReadAheadRange())
            .withStreamStatistics(new AbfsInputStreamStatisticsImpl())
            .withShouldReadBufferSizeAlways(
                abfsConfiguration.shouldReadBufferSizeAlways())
            .withReadAheadBlockSize(abfsConfiguration.getReadAheadBlockSize())
            .withBufferedPreadDisabled(bufferedPreadDisabled)
            .build();
  }

  public OutputStream openFileForWrite(final Path path,
      final FileSystem.Statistics statistics, final boolean overwrite,
      TracingContext tracingContext) throws IOException {
    try (AbfsPerfInfo perfInfo = startTracking("openFileForWrite", "getPathStatus")) {
      LOG.debug("openFileForWrite filesystem: {} path: {} overwrite: {}",
              client.getFileSystem(),
              path,
              overwrite);

      String relativePath = getRelativePath(path);
      boolean useBlobEndpoint = getPrefixMode() == PrefixMode.BLOB;
      if (OperativeEndpoint.isIngressEnabledOnDFS(
              getAbfsConfiguration().getPrefixMode(), getAbfsConfiguration())) {
        LOG.debug("GetFileStatus over DFS for open file for write for ingress config value {} for path {} ",
                abfsConfiguration.shouldIngressFallbackToDfs(), path);
        useBlobEndpoint = false;
      }
      VersionedFileStatus fileStatus;
      fileStatus = (VersionedFileStatus) getFileStatus(path, tracingContext, useBlobEndpoint);

      final Long contentLength = fileStatus.getLen();

      boolean isDirectory = fileStatus.isDirectory();
      if (isDirectory) {
        throw new AbfsRestOperationException(
                AzureServiceErrorCode.PATH_NOT_FOUND.getStatusCode(),
                AzureServiceErrorCode.PATH_NOT_FOUND.getErrorCode(),
                "openFileForWrite must be used with files and not directories",
                null);
      }

      final long offset = overwrite ? 0 : contentLength;

      perfInfo.registerSuccess(true);

      boolean isAppendBlob = false;
      if (isAppendBlobKey(path.toString())) {
        isAppendBlob = true;
      }

      AbfsLease lease = maybeCreateLease(relativePath, tracingContext);
      final String eTag = fileStatus.getEtag();
      checkAppendSmallWrite(isAppendBlob);

      return new AbfsOutputStream(
          populateAbfsOutputStreamContext(
              isAppendBlob,
              lease,
              client,
              statistics,
              relativePath,
              offset,
              eTag,
              tracingContext, poolSizeManager));
    }
  }

  public void checkAppendSmallWrite(boolean isAppendBlob) throws IOException {
    if (getAbfsConfiguration().getPrefixMode() == PrefixMode.BLOB) {
      if (isAppendBlob) {
        throw new IOException("AppendBlob is not supported for blob endpoint.");
      }
      if (abfsConfiguration.isSmallWriteOptimizationEnabled()) {
        throw new IOException("Small write optimization is not supported for blob endpoint.");
      }
    }
  }

  /**
   * Break any current lease on an ABFS file.
   *
   * @param path file name
   * @param tracingContext TracingContext instance to track correlation IDs
   * @throws AzureBlobFileSystemException on any exception while breaking the lease
   */
  public void breakLease(final Path path, final TracingContext tracingContext) throws AzureBlobFileSystemException {
    LOG.debug("lease path: {}", path);

    client.breakLease(getRelativePath(path), tracingContext);
  }

  public void rename(final Path source, final Path destination,
      final RenameAtomicityUtils renameAtomicityUtils, TracingContext tracingContext) throws
          IOException {
    final Instant startAggregate = abfsPerfTracker.getLatencyInstant();
    long countAggregate = 0;
    boolean shouldContinue;

    if (getAbfsConfiguration().getPrefixMode() == PrefixMode.BLOB) {
      LOG.debug("Rename for src: {} dst: {} for non-HNS blob-endpoint",
          source, destination);
      /*
       * Fetch the list of blobs in the given sourcePath.
       */
      StringBuilder listSrcBuilder = new StringBuilder(
          source.toUri().getPath());
      if (!source.isRoot()) {
        listSrcBuilder.append(FORWARD_SLASH);
      }
      String listSrc = listSrcBuilder.toString();
      BlobList blobList = client.getListBlobs(null, listSrc, null, null,
              tracingContext).getResult()
          .getBlobList();
      List<BlobProperty> srcBlobProperties = blobList.getBlobPropertyList();

      if (srcBlobProperties.size() > 0) {
        orchestrateBlobRenameDir(source, destination, renameAtomicityUtils,
            tracingContext, listSrc, blobList);
      } else {
        /*
        * Source doesn't have any hierarchy. It can either be marker or non-marker blob.
        * Or there can be no blob on the path.
        * Rename procedure will start. If its a file or a marker file, it will be renamed.
        * In case there is no blob on the path, server will return exception.
        */
        LOG.debug("source {} doesn't have any blob in its hierarchy. "
            + "Starting rename process on the source.", source);

        AbfsLease lease = null;
        try {
          if (isAtomicRenameKey(source.toUri().getPath())) {
            lease = getBlobLease(source.toUri().getPath(),
                BLOB_LEASE_ONE_MINUTE_DURATION, tracingContext);
          }
          renameBlob(source, destination, lease, tracingContext);
        } catch (AzureBlobFileSystemException ex) {
          if (lease != null) {
            lease.free();
          }
          LOG.error(
              String.format("Rename of path from %s to %s failed",
                  source, destination), ex);
          if (ex instanceof AbfsRestOperationException
              && ((AbfsRestOperationException) ex).getStatusCode()
              == HTTP_NOT_FOUND) {
            AbfsRestOperationException ex1 = (AbfsRestOperationException) ex;
            throw new AbfsRestOperationException(
                ex1.getStatusCode(),
                AzureServiceErrorCode.SOURCE_PATH_NOT_FOUND.getErrorCode(),
                ex1.getErrorMessage(), ex1);
          }
          throw ex;
        }
      }
      LOG.info("Rename from source {} to destination {} done", source,
          destination);
      return;
    }

    if (isAtomicRenameKey(source.getName())) {
      LOG.warn("The atomic rename feature is not supported by the ABFS scheme; however rename,"
              +" create and delete operations are atomic if Namespace is enabled for your Azure Storage account.");
    }

    LOG.debug("renameAsync filesystem: {} source: {} destination: {}",
            client.getFileSystem(),
            source,
            destination);

    String continuation = null;

    String sourceRelativePath = getRelativePath(source);
    String destinationRelativePath = getRelativePath(destination);

    do {
      try (AbfsPerfInfo perfInfo = startTracking("rename", "renamePath")) {
        AbfsRestOperation op = client
            .renamePath(sourceRelativePath, destinationRelativePath,
                continuation, tracingContext);
        perfInfo.registerResult(op.getResult());
        continuation = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_CONTINUATION);
        perfInfo.registerSuccess(true);
        countAggregate++;
        shouldContinue = continuation != null && !continuation.isEmpty();

        if (!shouldContinue) {
          perfInfo.registerAggregates(startAggregate, countAggregate);
        }
      }
    } while (shouldContinue);
  }

  private void orchestrateBlobRenameDir(final Path source,
      final Path destination,
      final RenameAtomicityUtils renameAtomicityUtils,
      final TracingContext tracingContext,
      final String listSrc,
      final BlobList blobList) throws IOException {
    ListBlobQueue listBlobQueue = new ListBlobQueue(
        blobList.getBlobPropertyList(),
        getAbfsConfiguration().getProducerQueueMaxSize(),
        getAbfsConfiguration().getBlobDirRenameMaxThread());

    if (blobList.getNextMarker() != null) {
      getListBlobProducer(listSrc, listBlobQueue, blobList.getNextMarker(),
          tracingContext);
    } else {
      listBlobQueue.complete();
    }
    LOG.debug("src {} exists and is a directory", source);
    /*
     * Fetch if there is a marker-blob for the source blob.
     */
    BlobProperty blobPropOnSrcNullable;
    try {
      blobPropOnSrcNullable = getBlobProperty(source, tracingContext);
    } catch (AbfsRestOperationException ex) {
      if (ex.getStatusCode() != HttpURLConnection.HTTP_NOT_FOUND) {
        throw ex;
      }
      blobPropOnSrcNullable = null;
    }

    final String srcDirETag;
    if (blobPropOnSrcNullable == null) {
      /*
       * There is no marker-blob, the client has to create marker blob before
       * starting the rename.
       */
      LOG.debug("Source {} is a directory but there is no marker-blob",
          source);
      srcDirETag = createDirectory(source, null, FsPermission.getDirDefault(),
          FsPermission.getUMask(
              getAbfsConfiguration().getRawConfiguration()),
          true, tracingContext);
    } else {
      LOG.debug("Source {} is a directory but there is a marker-blob",
          source);
      srcDirETag = blobPropOnSrcNullable.getETag();
    }
    /*
     * If source is a directory, all the blobs in the directory have to be
     * individually copied and then deleted at the source.
     */
    LOG.debug("source {} is a directory", source);
    final AbfsBlobLease srcDirLease;
    final Boolean isAtomicRename;
    if (isAtomicRenameKey(source.toUri().getPath())) {
      LOG.debug("source dir {} is an atomicRenameKey",
          source.toUri().getPath());
      srcDirLease = getBlobLease(source.toUri().getPath(),
          BLOB_LEASE_ONE_MINUTE_DURATION,
          tracingContext);
      renameAtomicityUtils.preRename(
          isCreateOperationOnBlobEndpoint(), srcDirETag);
      isAtomicRename = true;
    } else {
      srcDirLease = null;
      isAtomicRename = false;
      LOG.debug("source dir {} is not an atomicRenameKey",
          source.toUri().getPath());
    }

    renameBlobDir(source, destination, tracingContext, listBlobQueue,
        srcDirLease, isAtomicRename);

    if (isAtomicRename) {
      renameAtomicityUtils.cleanup();
    }
  }

  @VisibleForTesting
  ListBlobProducer getListBlobProducer(final String listSrc,
      final ListBlobQueue listBlobQueue,
      final String initNextMarker,
      final TracingContext tracingContext) {
    return new ListBlobProducer(listSrc,
        client, listBlobQueue, initNextMarker, tracingContext);
  }

  @VisibleForTesting
  AbfsBlobLease getBlobLease(final String source,
      final Integer blobLeaseOneMinuteDuration,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    return new AbfsBlobLease(client, source, blobLeaseOneMinuteDuration,
        tracingContext);
  }

  private void renameBlobDir(final Path source,
      final Path destination,
      final TracingContext tracingContext,
      final ListBlobQueue listBlobQueue,
      final AbfsBlobLease srcDirBlobLease,
      final Boolean isAtomicRename) throws AzureBlobFileSystemException {
    List<BlobProperty> blobList;
    ListBlobConsumer listBlobConsumer = new ListBlobConsumer(listBlobQueue);
    final ExecutorService renameBlobExecutorService
        = Executors.newFixedThreadPool(
        getAbfsConfiguration().getBlobDirRenameMaxThread());
    AtomicInteger renamedBlob = new AtomicInteger(0);
    while(!listBlobConsumer.isCompleted()) {
      blobList = listBlobConsumer.consume();
      if(blobList == null) {
        continue;
      }
      List<Future> futures = new ArrayList<>();
      for (BlobProperty blobProperty : blobList) {
        futures.add(renameBlobExecutorService.submit(() -> {
          try {
            AbfsBlobLease blobLease = null;
            if (isAtomicRename) {
              /*
               * Conditionally get a lease on the source blob to prevent other writers
               * from changing it. This is used for correctness in HBase when log files
               * are renamed. It generally should do no harm other than take a little
               * more time for other rename scenarios. When the HBase master renames a
               * log file folder, the lease locks out other writers.  This
               * prevents a region server that the master thinks is dead, but is still
               * alive, from committing additional updates.  This is different than
               * when HBase runs on HDFS, where the region server recovers the lease
               * on a log file, to gain exclusive access to it, before it splits it.
               */
              blobLease = getBlobLease(blobProperty.getPath().toUri().getPath(),
                  BLOB_LEASE_ONE_MINUTE_DURATION, tracingContext);
            }
            renameBlob(
                blobProperty.getPath(),
                createDestinationPathForBlobPartOfRenameSrcDir(destination,
                    blobProperty.getPath(), source),
                blobLease,
                tracingContext);
            renamedBlob.incrementAndGet();
          } catch (AzureBlobFileSystemException e) {
            LOG.error(String.format("rename from %s to %s for blob %s failed",
                source, destination, blobProperty.getPath()), e);
            throw new RuntimeException(e);
          }
        }));
      }
      for (Future future : futures) {
        try {
          future.get();
        } catch (InterruptedException | ExecutionException e) {
          LOG.error(String.format("rename from %s to %s failed", source,
              destination), e);
          listBlobConsumer.fail();
          renameBlobExecutorService.shutdown();
          if (srcDirBlobLease != null) {
            srcDirBlobLease.free();
          }
          throw new RuntimeException(e);
        }
      }
    }
    renameBlobExecutorService.shutdown();

    tracingContext.setOperatedBlobCount(renamedBlob.get() + 1);
    renameBlob(
        source, createDestinationPathForBlobPartOfRenameSrcDir(destination,
            source, source),
        srcDirBlobLease,
        tracingContext);
    tracingContext.setOperatedBlobCount(null);
  }

  private Boolean isCreateOperationOnBlobEndpoint() {
    return !OperativeEndpoint.isIngressEnabledOnDFS(getPrefixMode(), abfsConfiguration);
  }

  /**
   * Translates the destination path for a blob part of a source directory getting
   * renamed.
   *
   * @param destinationDir destination directory for the rename operation
   * @param blobPath path of blob inside sourceDir being renamed.
   * @param sourceDir source directory for the rename operation
   *
   * @return translated path for the blob
   */
  private Path createDestinationPathForBlobPartOfRenameSrcDir(final Path destinationDir,
      final Path blobPath, final Path sourceDir) {
    String destinationPathStr = destinationDir.toUri().getPath();
    String sourcePathStr = sourceDir.toUri().getPath();
    String srcBlobPropertyPathStr = blobPath.toUri().getPath();
    if (sourcePathStr.equals(srcBlobPropertyPathStr)) {
      return destinationDir;
    }
    return new Path(destinationPathStr + ROOT_PATH + srcBlobPropertyPathStr.substring(
        sourcePathStr.length()));
  }

  /**
   * Renames blob.
   * It copies the source blob to the destination. After copy is succesful, it
   * deletes the source blob
   *
   * @param sourcePath source path which gets copied to the destination
   * @param destination destination path to which the source has to be moved
   * @param lease lease of the srcBlob
   * @param tracingContext tracingContext for tracing the API calls
   *
   * @throws AzureBlobFileSystemException exception in making server calls
   */
  private void renameBlob(final Path sourcePath, final Path destination,
      final AbfsLease lease, final TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    copyBlob(sourcePath, destination, lease != null ? lease.getLeaseID() : null,
        tracingContext);
    deleteBlob(sourcePath, lease, tracingContext);
  }

  private void deleteBlob(final Path sourcePath,
      final AbfsLease lease, final TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    try {
      client.deleteBlobPath(sourcePath,
          lease != null ? lease.getLeaseID() : null, tracingContext);
      if (lease != null) {
        lease.cancelTimer();
      }
    } catch (AbfsRestOperationException ex) {
      if (ex.getStatusCode() != HttpURLConnection.HTTP_NOT_FOUND) {
        throw ex;
      }
    }
  }

  public void delete(final Path path, final boolean recursive,
      TracingContext tracingContext) throws IOException {
    LOG.debug("delete filesystem: {} path: {} recursive: {}",
        client.getFileSystem(),
        path,
        String.valueOf(recursive));
    if (getPrefixMode() == PrefixMode.BLOB) {
      deleteBlobPath(path, recursive, tracingContext);
      return;
    }

    final Instant startAggregate = abfsPerfTracker.getLatencyInstant();
    long countAggregate = 0;
    boolean shouldContinue = true;

    String continuation = null;

    String relativePath = getRelativePath(path);

    do {
      try (AbfsPerfInfo perfInfo = startTracking("delete", "deletePath")) {
        AbfsRestOperation op = client
            .deletePath(relativePath, recursive, continuation, tracingContext);
        perfInfo.registerResult(op.getResult());
        continuation = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_CONTINUATION);
        perfInfo.registerSuccess(true);
        countAggregate++;
        shouldContinue = continuation != null && !continuation.isEmpty();

        if (!shouldContinue) {
          perfInfo.registerAggregates(startAggregate, countAggregate);
        }
      }
    } while (shouldContinue);
  }

  /**
   * Handles deletion of a path over Blob Endpoint.
   * @param path path to be deleted
   * @param recursive defines deletion to be recursive or not
   * @param tracingContext object to trace the API flows
   *
   * @throws IOException exception from server call or if recursive used on
   * non-empty directory
   */
  private void deleteBlobPath(final Path path,
      final boolean recursive,
      final TracingContext tracingContext) throws IOException {
    StringBuilder listSrcBuilder = new StringBuilder();
    String srcPathStr = path.toUri().getPath();
    listSrcBuilder.append(srcPathStr);
    if (!path.isRoot()) {
      listSrcBuilder.append(FORWARD_SLASH);
    }
    String listSrc = listSrcBuilder.toString();
    BlobList blobList = client.getListBlobs(null, listSrc, null, null,
        tracingContext).getResult().getBlobList();
    if (blobList.getBlobPropertyList().size() > 0) {
      orchestrateBlobDirDeletion(path, recursive, listSrc, blobList,
          tracingContext);
    } else {
      LOG.debug(String.format("Path %s doesn't have child-blobs", srcPathStr));
      if (!path.isRoot()) {
        try {
          client.deleteBlobPath(path, null, tracingContext);
        } catch (AbfsRestOperationException ex) {
          if (ex.getStatusCode() == HTTP_NOT_FOUND) {
            LOG.error(String.format("Path %s doesn't exist", srcPathStr), ex);
            throw new AbfsRestOperationException(
                ex.getStatusCode(),
                AzureServiceErrorCode.PATH_NOT_FOUND.getErrorCode(),
                ex.getErrorMessage(), ex);
          }
          LOG.error(String.format("Deletion failed for path %s", srcPathStr), ex);
          throw ex;
        }
      }
    }
    /*
     * Src Path here would be either a non-empty directory, marker-blob or a
     * normal-blob. Path can not be non-existing at this point.
     * If parent blob is implicit directory and in case the blob deleted was the
     * only blob in the directory, it will render path as non-existing. To prevent
     * that happening, it is needed to  create marker-based directory on the
     * parentPath. This is inspired from WASB implementation.
     */
    createParentDirectory(path, tracingContext);
    LOG.debug(String.format("Deletion of Path %s completed", srcPathStr));
  }

  private void orchestrateBlobDirDeletion(final Path path,
      final boolean recursive,
      final String listSrc,
      final BlobList blobList,
      final TracingContext tracingContext) throws IOException {
    final String srcPathStr = path.toUri().getPath();
    LOG.debug(String.format("Path %s has child-blobs", srcPathStr));
    if (!recursive) {
      LOG.error(String.format("Non-recursive delete of non-empty directory %s",
          srcPathStr));
      throw new IOException(
          "Non-recursive delete of non-empty directory");
    }
    ListBlobQueue queue = new ListBlobQueue(blobList.getBlobPropertyList(),
        getAbfsConfiguration().getProducerQueueMaxSize(),
        getAbfsConfiguration().getBlobDirDeleteMaxThread());
    if (blobList.getNextMarker() != null) {
      getListBlobProducer(listSrc, queue, blobList.getNextMarker(),
          tracingContext);
    } else {
      queue.complete();
    }
    ListBlobConsumer consumer = new ListBlobConsumer(queue);
    deleteOnConsumedBlobs(path, consumer, tracingContext);
  }

  /**
   * If parent blob is implicit directory and in case the blob deleted was the
   * only blob in the directory, it will render path as non-existing. To prevent
   * that happening, client create marker-based directory on the parentPath.
   *
   * @param path path getting deleted
   * @param tracingContext tracingContext to trace the API flow
   * @throws IOException
   */
  private void createParentDirectory(final Path path,
      final TracingContext tracingContext) throws IOException {
    if (path.isRoot()) {
      return;
    }
    Path parentPath = path.getParent();
    if (parentPath.isRoot()) {
      return;
    }

    String srcPathStr = path.toUri().getPath();
    String srcParentPathSrc = parentPath.toUri().getPath();
    LOG.debug(String.format(
        "Creating Parent of Path %s : %s", srcPathStr, srcParentPathSrc));
    createDirectory(parentPath, null, FsPermission.getDirDefault(),
        FsPermission.getUMask(
            getAbfsConfiguration().getRawConfiguration()), false,
        tracingContext);
    LOG.debug(String.format("Directory for parent of Path %s : %s created",
        srcPathStr, srcParentPathSrc));
  }

  /**
   * Consumes list of blob over the consumer. Deletes the blob listed.
   * The deletion of the consumed blobs is executed over parallel threads spawned
   * by an ExecutorService with threads equal to {@link AbfsConfiguration#getBlobDirDeleteMaxThread()}.
   * Once a list of blobs that were consumed are deleted, the next batch of blobs
   * are consumed.
   *
   * @param srcPath path of the directory which has to be deleted
   * @param consumer {@link ListBlobConsumer} object through which batches of blob
   * are consumed.
   * @param tracingContext object for tracing the API flow
   *
   * @throws AzureBlobFileSystemException exception received from server call.
   */
  private void deleteOnConsumedBlobs(final Path srcPath,
      final ListBlobConsumer consumer,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    AtomicInteger deletedBlobCount = new AtomicInteger(0);
    String srcPathStr = srcPath.toUri().getPath();
    ExecutorService deleteBlobExecutorService = Executors.newFixedThreadPool(
        getAbfsConfiguration().getBlobDirDeleteMaxThread());
    try {
      while (!consumer.isCompleted()) {
        final List<BlobProperty> blobList = consumer.consume();
        if (blobList == null) {
          continue;
        }
        List<Future> futureList = new ArrayList<>();
        for (BlobProperty blobProperty : blobList) {
          futureList.add(deleteBlobExecutorService.submit(() -> {
            String blobPropertyPathStr = blobProperty.getPath().toUri()
                .getPath();
            try {
              client.deleteBlobPath(blobProperty.getPath(), null,
                  tracingContext);
              deletedBlobCount.incrementAndGet();
            } catch (AzureBlobFileSystemException ex) {
              if (ex instanceof AbfsRestOperationException
                  && ((AbfsRestOperationException) ex).getStatusCode()
                  == HttpURLConnection.HTTP_NOT_FOUND) {
                return;
              }
              LOG.error(String.format("Deleting Path %s failed",
                  blobPropertyPathStr), ex);
              consumer.fail();
              throw new RuntimeException(ex);
            }
          }));
        }

        for (Future future : futureList) {
          try {
            future.get();
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        }
      }
      LOG.debug(String.format(
          "Deletion of child blobs in hierarchy of Path %s is done",
          srcPathStr));
    } finally {
      deleteBlobExecutorService.shutdown();
    }

    tracingContext.setOperatedBlobCount(deletedBlobCount.get() + 1);
    if (!srcPath.isRoot()) {
      try {
        LOG.debug(String.format("Deleting Path %s", srcPathStr));
        client.deleteBlobPath(srcPath, null, tracingContext);
        LOG.debug(String.format("Deleted Path %s", srcPathStr));
      } catch (AbfsRestOperationException ex) {
        if (ex.getStatusCode() != HttpURLConnection.HTTP_NOT_FOUND) {
          LOG.error(String.format("Deleting Path %s failed", srcPathStr), ex);
          throw ex;
        }
        LOG.debug(
            String.format("Path %s is an implicit directory", srcPathStr));
      }
    }
    tracingContext.setOperatedBlobCount(null);
  }

  public FileStatus getFileStatus(Path path, TracingContext tracingContext, boolean useBlobEndpoint) throws IOException {
    try {
      AbfsPerfInfo perfInfo = startTracking("getFileStatus", "undetermined");
      boolean isNamespaceEnabled = getIsNamespaceEnabled(tracingContext);
      LOG.debug("getFileStatus filesystem: {} path: {} isNamespaceEnabled: {}",
              client.getFileSystem(),
              path,
              isNamespaceEnabled);
      perfInfo.registerCallee("getPathProperty");
      return getPathProperty(path, tracingContext, useBlobEndpoint);

    } catch (AbfsRestOperationException ex) {
      if (useBlobEndpoint && ex.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND && !path.isRoot()) {
        List<BlobProperty> blobProperties = getListBlobs(path, null, null, tracingContext, 2, true);
        if (blobProperties.size() == 0) {
          throw ex;
        }
        else {
          // TODO: return properties of first child blob here like in wasb after listFileStatus is implemented over blob
          return new VersionedFileStatus(
                  userName,
                  primaryUserGroup,
                  new AbfsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL),
                  false,
                  0L,
                  true,
                  1,
                  abfsConfiguration.getAzureBlockSize(),
                  DateTimeUtils.parseLastModifiedTime(null),
                  path,
                  null);
        }
      } else {
        throw ex;
      }
    }
  }

  /**
   * Method that deals with error cases of calling getPathProperty directly.
   * getPathProperty itself does not do the error handling, as it is intended to be
   * one part of the calls constituting getFileStatus. This additional method
   * would help when getPathProperty has to be called in a direct flow and needs a check for this error.
   * @param path Current Path
   * @param tracingContext current tracing context
   * @param useBlobEndpoint whether to use blob endpoint
   * @return FileStatus or null if blob does not exist or is not explicit
   * @throws IOException
   */
  FileStatus tryGetPathProperty(Path path, TracingContext tracingContext, Boolean useBlobEndpoint) throws IOException {
    try {
      return getPathProperty(path, tracingContext, useBlobEndpoint);
    } catch(AbfsRestOperationException ex) {
      if (ex.getStatusCode() == HTTP_NOT_FOUND) {
        LOG.debug("No explicit directory/path found: {}", path);
        return null;
      }
      throw ex;
    }
  }

  /**
   * Method to make a call to get path property based on various configs-
   * like whether to go over blob/dfs endpoint, whether path provided is root etc.
   * This does not segregate between implicit and explicit paths.
   * @param path Path to call the downstream get property method on
   * @param tracingContext Current tracing context for the call
   * @param useBlobEndpoint Flag indicating whether to use blob endpoint
   * @return VersionedFileStatus object for given path
   * @throws IOException
   */
  FileStatus getPathProperty(Path path, TracingContext tracingContext, Boolean useBlobEndpoint) throws IOException {
    AbfsPerfInfo perfInfo = startTracking("getPathProperty", "undetermined");
    final AbfsRestOperation op;
    Boolean isNamespaceEnabled = getIsNamespaceEnabled(tracingContext);
    if (useBlobEndpoint) {
      LOG.debug("getPathProperty filesystem call over blob endpoint: {} path: {} isNamespaceEnabled: {}",
              client.getFileSystem(),
              path,
              isNamespaceEnabled);

      if (path.isRoot()) {
        perfInfo.registerCallee("getContainerProperties");
        op = client.getContainerProperty(tracingContext);
      } else {
        perfInfo.registerCallee("getBlobProperty");
        op = client.getBlobProperty(path, tracingContext);
      }
    } else {
      LOG.debug("getPathProperty filesystem call over dfs endpoint: {} path: {} isNamespaceEnabled: {}",
              client.getFileSystem(),
              path,
              isNamespaceEnabled);
      if (path.isRoot()) {
        if (isNamespaceEnabled) {
          perfInfo.registerCallee("getAclStatus");
          op = client.getAclStatus(getRelativePath(path), tracingContext);
        } else {
          perfInfo.registerCallee("getFilesystemProperties");
          op = client.getFilesystemProperties(tracingContext);
        }
      } else {
        perfInfo.registerCallee("getPathStatus");
        op = client.getPathStatus(getRelativePath(path), false, tracingContext);
      }
    }

    perfInfo.registerResult(op.getResult());
    final long blockSize = abfsConfiguration.getAzureBlockSize();
    final AbfsHttpOperation result = op.getResult();

    String eTag = extractEtagHeader(result);
    final String lastModified = result.getResponseHeader(HttpHeaderConfigurations.LAST_MODIFIED);
    final String permissions = result.getResponseHeader((HttpHeaderConfigurations.X_MS_PERMISSIONS));
    final boolean hasAcl = AbfsPermission.isExtendedAcl(permissions);
    final long contentLength;
    final boolean resourceIsDir;

    if (path.isRoot()) {
      contentLength = 0;
      resourceIsDir = true;
    } else {
      contentLength = parseContentLength(result.getResponseHeader(HttpHeaderConfigurations.CONTENT_LENGTH));
      if (useBlobEndpoint) {
        resourceIsDir = result.getResponseHeader(X_MS_META_HDI_ISFOLDER) != null;
      } else {
        resourceIsDir = parseIsDirectory(result.getResponseHeader(HttpHeaderConfigurations.X_MS_RESOURCE_TYPE));
      }
    }

    final String transformedOwner = identityTransformer.transformIdentityForGetRequest(
            result.getResponseHeader(HttpHeaderConfigurations.X_MS_OWNER),
            true,
            userName);

    final String transformedGroup = identityTransformer.transformIdentityForGetRequest(
            result.getResponseHeader(HttpHeaderConfigurations.X_MS_GROUP),
            false,
            primaryUserGroup);

    perfInfo.registerSuccess(true);

    return new VersionedFileStatus(
            transformedOwner,
            transformedGroup,
            permissions == null ? new AbfsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL)
                    : AbfsPermission.valueOf(permissions),
            hasAcl,
            contentLength,
            resourceIsDir,
            1,
            blockSize,
            DateTimeUtils.parseLastModifiedTime(lastModified),
            path,
            eTag);
  }

  /**
   * @param path The list path.
   * @param tracingContext Tracks identifiers for request header
   * @return the entries in the path.
   * */
  @Override
  public FileStatus[] listStatus(final Path path, TracingContext tracingContext) throws IOException {
    return listStatus(path, null, tracingContext);
  }

  /**
   * @param path Path the list path.
   * @param startFrom the entry name that list results should start with.
   *                  For example, if folder "/folder" contains four files: "afile", "bfile", "hfile", "ifile".
   *                  Then listStatus(Path("/folder"), "hfile") will return "/folder/hfile" and "folder/ifile"
   *                  Notice that if startFrom is a non-existent entry name, then the list response contains
   *                  all entries after this non-existent entry in lexical order:
   *                  listStatus(Path("/folder"), "cfile") will return "/folder/hfile" and "/folder/ifile".
   * @param tracingContext Tracks identifiers for request header
   * @return the entries in the path start from  "startFrom" in lexical order.
   * */
  @InterfaceStability.Unstable
  @Override
  public FileStatus[] listStatus(final Path path, final String startFrom, TracingContext tracingContext) throws IOException {
    List<FileStatus> fileStatuses = new ArrayList<>();
    listStatus(path, startFrom, fileStatuses, true, null, tracingContext);
    return fileStatuses.toArray(new FileStatus[fileStatuses.size()]);
  }

  @Override
  public String listStatus(final Path path, final String startFrom,
      List<FileStatus> fileStatuses, final boolean fetchAll,
      String continuation, TracingContext tracingContext) throws IOException {
    final Instant startAggregate = abfsPerfTracker.getLatencyInstant();
    long countAggregate = 0;
    boolean shouldContinue = true;

    LOG.debug("listStatus filesystem: {} path: {}, startFrom: {}",
            client.getFileSystem(),
            path,
            startFrom);

    final String relativePath = getRelativePath(path);
    boolean useBlobEndpointListing = getPrefixMode() == PrefixMode.BLOB;

    if (continuation == null || continuation.isEmpty()) {
      // generate continuation token if a valid startFrom is provided.
      if (startFrom != null && !startFrom.isEmpty()) {
        // In case startFrom is passed, fallback to DFS for now
        // TODO: Support startFrom for List Blobs on Blob Endpoint
        useBlobEndpointListing = false;
        continuation = getIsNamespaceEnabled(tracingContext)
            ? generateContinuationTokenForXns(startFrom)
            : generateContinuationTokenForNonXns(relativePath, startFrom);
      }
    }

    if (useBlobEndpointListing) {
      // For blob endpoint continuation will be used as nextMarker.
      String prefix = relativePath + ROOT_PATH;
      String delimiter = ROOT_PATH;
      if (path.isRoot()) {
        prefix = null;
      }

      TreeMap<String, FileStatus> fileMetadata = new TreeMap<>();
      long objectCountReturnedByServer = 0;

      do {
        /*
         * List Blob calls will be made with delimiter "/". This will ensure
         * that all the children of a folder not listed out separately. Instead,
         * a single entry corresponding to the directory name will be returned as BlobPrefix.
         */
        try (AbfsPerfInfo perfInfo = startTracking("listStatus", "getListBlobs")) {
          AbfsRestOperation op = client.getListBlobs(
              continuation, prefix, delimiter, abfsConfiguration.getListMaxResults(),
              tracingContext
          );
          perfInfo.registerResult(op.getResult());
          BlobList blobList = op.getResult().getBlobList();
          int blobListSize = blobList.getBlobPropertyList().size();
          LOG.debug("List Blob Call on filesystem: {} path: {} marker: {} delimiter: {} returned {} objects",
              client.getFileSystem(), prefix, continuation,
              delimiter, blobListSize);

          continuation = blobList.getNextMarker();
          objectCountReturnedByServer += blobListSize;

          addBlobListAsFileStatus(blobList, fileMetadata);

          perfInfo.registerSuccess(true);
          countAggregate++;
          shouldContinue =
              fetchAll && continuation != null && !continuation.isEmpty();

          if (!shouldContinue) {
            perfInfo.registerAggregates(startAggregate, countAggregate);
          }
        }
      } while (shouldContinue);

      fileStatuses.addAll(fileMetadata.values());

      if (fileStatuses.size() == 0) {
        FileStatus status = getPathProperty(path, tracingContext, true);
        if (status.isFile()) {
          fileStatuses.add(status);
        }
      }

      LOG.debug("List Status on Blob Endpoint on filesystem: {} path: {} received {} objects from server and returned {} objects to user",
          client.getFileSystem(), path, objectCountReturnedByServer, fileStatuses.size());

      return continuation;
    }

    do {
      try (AbfsPerfInfo perfInfo = startTracking("listStatus", "listPath")) {
        AbfsRestOperation op = client.listPath(relativePath, false,
            abfsConfiguration.getListMaxResults(), continuation,
            tracingContext);
        perfInfo.registerResult(op.getResult());
        continuation = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_CONTINUATION);
        ListResultSchema retrievedSchema = op.getResult().getListResultSchema();
        if (retrievedSchema == null) {
          throw new AbfsRestOperationException(
                  AzureServiceErrorCode.PATH_NOT_FOUND.getStatusCode(),
                  AzureServiceErrorCode.PATH_NOT_FOUND.getErrorCode(),
                  "listStatusAsync path not found",
                  null, op.getResult());
        }

        long blockSize = abfsConfiguration.getAzureBlockSize();

        for (ListResultEntrySchema entry : retrievedSchema.paths()) {
          final String owner = identityTransformer.transformIdentityForGetRequest(entry.owner(), true, userName);
          final String group = identityTransformer.transformIdentityForGetRequest(entry.group(), false, primaryUserGroup);
          final FsPermission fsPermission = entry.permissions() == null
                  ? new AbfsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL)
                  : AbfsPermission.valueOf(entry.permissions());
          final boolean hasAcl = AbfsPermission.isExtendedAcl(entry.permissions());

          long lastModifiedMillis = 0;
          long contentLength = entry.contentLength() == null ? 0 : entry.contentLength();
          boolean isDirectory = entry.isDirectory() == null ? false : entry.isDirectory();
          if (entry.lastModified() != null && !entry.lastModified().isEmpty()) {
            lastModifiedMillis = DateTimeUtils.parseLastModifiedTime(
                entry.lastModified());
          }

          Path entryPath = new Path(File.separator + entry.name());
          entryPath = entryPath.makeQualified(this.uri, entryPath);

          fileStatuses.add(
                  new VersionedFileStatus(
                          owner,
                          group,
                          fsPermission,
                          hasAcl,
                          contentLength,
                          isDirectory,
                          1,
                          blockSize,
                          lastModifiedMillis,
                          entryPath,
                          entry.eTag()));
        }

        perfInfo.registerSuccess(true);
        countAggregate++;
        shouldContinue =
            fetchAll && continuation != null && !continuation.isEmpty();

        if (!shouldContinue) {
          perfInfo.registerAggregates(startAggregate, countAggregate);
        }
      }
    } while (shouldContinue);

    return continuation;
  }

  private void addBlobListAsFileStatus(final BlobList blobList,
      TreeMap<String, FileStatus> fileMetadata) throws IOException {

    /*
     * Here before adding the data we might have to remove the duplicates.
     * List Blobs call over blob endpoint returns two types of entries: Blob
     * and BlobPrefix.  In the case where ABFS generated the data,
     * there will be a marker blob for each "directory" created by driver,
     * and we will receive them as a Blob.  If there are also files within this
     * "directory", we will also receive a BlobPrefix.  To further
     * complicate matters, the data may not be generated by ABFS Driver, in
     * which case we may not have a marker blob for each "directory". In this
     * the only way to know there is a directory is using BlobPrefix entry.
     * So, sometimes we receive both a Blob and a BlobPrefix for directories,
     * and sometimes we receive only BlobPrefix as directory. We remove duplicates
     * but prefer Blob over BlobPrefix.
     */
    List<BlobProperty> blobProperties = blobList.getBlobPropertyList();

    for (BlobProperty entry: blobProperties) {
      String blobKey = entry.getName();
      final String owner = identityTransformer.transformIdentityForGetRequest(
          entry.getOwner(), true, userName);
      final String group = identityTransformer.transformIdentityForGetRequest(
          entry.getGroup(), false, primaryUserGroup);
      final FsPermission fsPermission = entry.getPermission() == null
          ? new AbfsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL)
          : AbfsPermission.valueOf(entry.getPermission());
      final boolean hasAcl = entry.getAcl() != null;
      long blockSize = abfsConfiguration.getAzureBlockSize();

      Path entryPath = entry.getPath();
      entryPath = entryPath.makeQualified(this.uri, entryPath);

      FileStatus fileStatus = new VersionedFileStatus(
          owner,
          group,
          fsPermission,
          hasAcl,
          entry.getContentLength(),
          entry.getIsDirectory(),
          1,
          blockSize,
          entry.getLastModifiedTime(),
          entryPath,
          entry.getETag());

      if (entry.getETag() != null) {
        // This is a blob entry. It is either a file or a marker blob.
        // In both cases we will add this.
        fileMetadata.put(blobKey, fileStatus);
      } else {
        // This is a BlobPrefix entry. It is a directory with file inside
        // This might have already been added as a marker blob.
        if (!fileMetadata.containsKey(blobKey)) {
          fileMetadata.put(blobKey, fileStatus);
        }
      }
    }
  }

  // generate continuation token for xns account
  private String generateContinuationTokenForXns(final String firstEntryName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(firstEntryName)
            && !firstEntryName.startsWith(AbfsHttpConstants.ROOT_PATH),
            "startFrom must be a dir/file name and it can not be a full path");

    StringBuilder sb = new StringBuilder();
    sb.append(firstEntryName).append("#$").append("0");

    CRC64 crc64 = new CRC64();
    StringBuilder token = new StringBuilder();
    token.append(crc64.compute(sb.toString().getBytes(StandardCharsets.UTF_8)))
            .append(SINGLE_WHITE_SPACE)
            .append("0")
            .append(SINGLE_WHITE_SPACE)
            .append(firstEntryName);

    return Base64.encode(token.toString().getBytes(StandardCharsets.UTF_8));
  }

  // generate continuation token for non-xns account
  private String generateContinuationTokenForNonXns(String path, final String firstEntryName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(firstEntryName)
            && !firstEntryName.startsWith(AbfsHttpConstants.ROOT_PATH),
            "startFrom must be a dir/file name and it can not be a full path");

    // Notice: non-xns continuation token requires full path (first "/" is not included) for startFrom
    path = AbfsClient.getDirectoryQueryParameter(path);
    final String startFrom = (path.isEmpty() || path.equals(ROOT_PATH))
            ? firstEntryName
            : path + ROOT_PATH + firstEntryName;

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(TOKEN_DATE_PATTERN, Locale.US);
    String date = simpleDateFormat.format(new Date());
    String token = String.format("%06d!%s!%06d!%s!%06d!%s!",
            path.length(), path, startFrom.length(), startFrom, date.length(), date);
    String base64EncodedToken = Base64.encode(token.getBytes(StandardCharsets.UTF_8));

    StringBuilder encodedTokenBuilder = new StringBuilder(base64EncodedToken.length() + 5);
    encodedTokenBuilder.append(String.format("%s!%d!", TOKEN_VERSION, base64EncodedToken.length()));

    for (int i = 0; i < base64EncodedToken.length(); i++) {
      char current = base64EncodedToken.charAt(i);
      if (CHAR_FORWARD_SLASH == current) {
        current = CHAR_UNDERSCORE;
      } else if (CHAR_PLUS == current) {
        current = CHAR_STAR;
      } else if (CHAR_EQUALS == current) {
        current = CHAR_HYPHEN;
      }
      encodedTokenBuilder.append(current);
    }

    return encodedTokenBuilder.toString();
  }

  public void setOwner(final Path path, final String owner, final String group,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
    }

    try (AbfsPerfInfo perfInfo = startTracking("setOwner", "setOwner")) {

      LOG.debug(
              "setOwner filesystem: {} path: {} owner: {} group: {}",
              client.getFileSystem(),
              path,
              owner,
              group);

      final String transformedOwner = identityTransformer.transformUserOrGroupForSetRequest(owner);
      final String transformedGroup = identityTransformer.transformUserOrGroupForSetRequest(group);

      final AbfsRestOperation op = client.setOwner(getRelativePath(path),
              transformedOwner,
              transformedGroup,
              tracingContext);

      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    }
  }

  public void setPermission(final Path path, final FsPermission permission,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
    }

    try (AbfsPerfInfo perfInfo = startTracking("setPermission", "setPermission")) {

      LOG.debug(
              "setPermission filesystem: {} path: {} permission: {}",
              client.getFileSystem(),
              path,
              permission);

      final AbfsRestOperation op = client.setPermission(getRelativePath(path),
          String.format(AbfsHttpConstants.PERMISSION_FORMAT,
              permission.toOctal()), tracingContext);

      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    }
  }

  public void modifyAclEntries(final Path path, final List<AclEntry> aclSpec,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
    }

    try (AbfsPerfInfo perfInfoGet = startTracking("modifyAclEntries", "getAclStatus")) {

      LOG.debug(
              "modifyAclEntries filesystem: {} path: {} aclSpec: {}",
              client.getFileSystem(),
              path,
              AclEntry.aclSpecToString(aclSpec));

      identityTransformer.transformAclEntriesForSetRequest(aclSpec);
      final Map<String, String> modifyAclEntries = AbfsAclHelper.deserializeAclSpec(AclEntry.aclSpecToString(aclSpec));
      boolean useUpn = AbfsAclHelper.isUpnFormatAclEntries(modifyAclEntries);

      String relativePath = getRelativePath(path);

      final AbfsRestOperation op = client
          .getAclStatus(relativePath, useUpn, tracingContext);
      perfInfoGet.registerResult(op.getResult());
      final String eTag = op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG);

      final Map<String, String> aclEntries = AbfsAclHelper.deserializeAclSpec(op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL));

      AbfsAclHelper.modifyAclEntriesInternal(aclEntries, modifyAclEntries);

      perfInfoGet.registerSuccess(true).finishTracking();

      try (AbfsPerfInfo perfInfoSet = startTracking("modifyAclEntries", "setAcl")) {
        final AbfsRestOperation setAclOp = client
            .setAcl(relativePath, AbfsAclHelper.serializeAclSpec(aclEntries),
                eTag, tracingContext);
        perfInfoSet.registerResult(setAclOp.getResult())
                .registerSuccess(true)
                .registerAggregates(perfInfoGet.getTrackingStart(), GET_SET_AGGREGATE_COUNT);
      }
    }
  }

  public void removeAclEntries(final Path path, final List<AclEntry> aclSpec,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
    }

    try (AbfsPerfInfo perfInfoGet = startTracking("removeAclEntries", "getAclStatus")) {

      LOG.debug(
              "removeAclEntries filesystem: {} path: {} aclSpec: {}",
              client.getFileSystem(),
              path,
              AclEntry.aclSpecToString(aclSpec));

      identityTransformer.transformAclEntriesForSetRequest(aclSpec);
      final Map<String, String> removeAclEntries = AbfsAclHelper.deserializeAclSpec(AclEntry.aclSpecToString(aclSpec));
      boolean isUpnFormat = AbfsAclHelper.isUpnFormatAclEntries(removeAclEntries);

      String relativePath = getRelativePath(path);

      final AbfsRestOperation op = client
          .getAclStatus(relativePath, isUpnFormat, tracingContext);
      perfInfoGet.registerResult(op.getResult());
      final String eTag = op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG);

      final Map<String, String> aclEntries = AbfsAclHelper.deserializeAclSpec(op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL));

      AbfsAclHelper.removeAclEntriesInternal(aclEntries, removeAclEntries);

      perfInfoGet.registerSuccess(true).finishTracking();

      try (AbfsPerfInfo perfInfoSet = startTracking("removeAclEntries", "setAcl")) {
        final AbfsRestOperation setAclOp = client
            .setAcl(relativePath, AbfsAclHelper.serializeAclSpec(aclEntries),
                eTag, tracingContext);
        perfInfoSet.registerResult(setAclOp.getResult())
                .registerSuccess(true)
                .registerAggregates(perfInfoGet.getTrackingStart(), GET_SET_AGGREGATE_COUNT);
      }
    }
  }

  public void removeDefaultAcl(final Path path, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
    }

    try (AbfsPerfInfo perfInfoGet = startTracking("removeDefaultAcl", "getAclStatus")) {

      LOG.debug(
              "removeDefaultAcl filesystem: {} path: {}",
              client.getFileSystem(),
              path);

      String relativePath = getRelativePath(path);

      final AbfsRestOperation op = client
          .getAclStatus(relativePath, tracingContext);
      perfInfoGet.registerResult(op.getResult());
      final String eTag = op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG);
      final Map<String, String> aclEntries = AbfsAclHelper.deserializeAclSpec(op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL));
      final Map<String, String> defaultAclEntries = new HashMap<>();

      for (Map.Entry<String, String> aclEntry : aclEntries.entrySet()) {
        if (aclEntry.getKey().startsWith("default:")) {
          defaultAclEntries.put(aclEntry.getKey(), aclEntry.getValue());
        }
      }

      aclEntries.keySet().removeAll(defaultAclEntries.keySet());

      perfInfoGet.registerSuccess(true).finishTracking();

      try (AbfsPerfInfo perfInfoSet = startTracking("removeDefaultAcl", "setAcl")) {
        final AbfsRestOperation setAclOp = client
            .setAcl(relativePath, AbfsAclHelper.serializeAclSpec(aclEntries),
                eTag, tracingContext);
        perfInfoSet.registerResult(setAclOp.getResult())
                .registerSuccess(true)
                .registerAggregates(perfInfoGet.getTrackingStart(), GET_SET_AGGREGATE_COUNT);
      }
    }
  }

  public void removeAcl(final Path path, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
    }

    try (AbfsPerfInfo perfInfoGet = startTracking("removeAcl", "getAclStatus")){

      LOG.debug(
              "removeAcl filesystem: {} path: {}",
              client.getFileSystem(),
              path);

      String relativePath = getRelativePath(path);

      final AbfsRestOperation op = client
          .getAclStatus(relativePath, tracingContext);
      perfInfoGet.registerResult(op.getResult());
      final String eTag = op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG);

      final Map<String, String> aclEntries = AbfsAclHelper.deserializeAclSpec(op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL));
      final Map<String, String> newAclEntries = new HashMap<>();

      newAclEntries.put(AbfsHttpConstants.ACCESS_USER, aclEntries.get(AbfsHttpConstants.ACCESS_USER));
      newAclEntries.put(AbfsHttpConstants.ACCESS_GROUP, aclEntries.get(AbfsHttpConstants.ACCESS_GROUP));
      newAclEntries.put(AbfsHttpConstants.ACCESS_OTHER, aclEntries.get(AbfsHttpConstants.ACCESS_OTHER));

      perfInfoGet.registerSuccess(true).finishTracking();

      try (AbfsPerfInfo perfInfoSet = startTracking("removeAcl", "setAcl")) {
        final AbfsRestOperation setAclOp = client
            .setAcl(relativePath, AbfsAclHelper.serializeAclSpec(newAclEntries),
                eTag, tracingContext);
        perfInfoSet.registerResult(setAclOp.getResult())
                .registerSuccess(true)
                .registerAggregates(perfInfoGet.getTrackingStart(), GET_SET_AGGREGATE_COUNT);
      }
    }
  }

  public void setAcl(final Path path, final List<AclEntry> aclSpec,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
    }

    try (AbfsPerfInfo perfInfoGet = startTracking("setAcl", "getAclStatus")) {

      LOG.debug(
              "setAcl filesystem: {} path: {} aclspec: {}",
              client.getFileSystem(),
              path,
              AclEntry.aclSpecToString(aclSpec));

      identityTransformer.transformAclEntriesForSetRequest(aclSpec);
      final Map<String, String> aclEntries = AbfsAclHelper.deserializeAclSpec(AclEntry.aclSpecToString(aclSpec));
      final boolean isUpnFormat = AbfsAclHelper.isUpnFormatAclEntries(aclEntries);

      String relativePath = getRelativePath(path);

      final AbfsRestOperation op = client
          .getAclStatus(relativePath, isUpnFormat, tracingContext);
      perfInfoGet.registerResult(op.getResult());
      final String eTag = op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG);

      final Map<String, String> getAclEntries = AbfsAclHelper.deserializeAclSpec(op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL));

      AbfsAclHelper.setAclEntriesInternal(aclEntries, getAclEntries);

      perfInfoGet.registerSuccess(true).finishTracking();

      try (AbfsPerfInfo perfInfoSet = startTracking("setAcl", "setAcl")) {
        final AbfsRestOperation setAclOp =
                client.setAcl(relativePath,
                AbfsAclHelper.serializeAclSpec(aclEntries), eTag, tracingContext);
        perfInfoSet.registerResult(setAclOp.getResult())
                .registerSuccess(true)
                .registerAggregates(perfInfoGet.getTrackingStart(), GET_SET_AGGREGATE_COUNT);
      }
    }
  }

  public AclStatus getAclStatus(final Path path, TracingContext tracingContext)
      throws IOException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
    }

    try (AbfsPerfInfo perfInfo = startTracking("getAclStatus", "getAclStatus")) {

      LOG.debug(
              "getAclStatus filesystem: {} path: {}",
              client.getFileSystem(),
              path);

      AbfsRestOperation op = client
          .getAclStatus(getRelativePath(path), tracingContext);
      AbfsHttpOperation result = op.getResult();
      perfInfo.registerResult(result);

      final String transformedOwner = identityTransformer.transformIdentityForGetRequest(
              result.getResponseHeader(HttpHeaderConfigurations.X_MS_OWNER),
              true,
              userName);
      final String transformedGroup = identityTransformer.transformIdentityForGetRequest(
              result.getResponseHeader(HttpHeaderConfigurations.X_MS_GROUP),
              false,
              primaryUserGroup);

      final String permissions = result.getResponseHeader(HttpHeaderConfigurations.X_MS_PERMISSIONS);
      final String aclSpecString = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL);

      final List<AclEntry> aclEntries = AclEntry.parseAclSpec(AbfsAclHelper.processAclString(aclSpecString), true);
      identityTransformer.transformAclEntriesForGetRequest(aclEntries, userName, primaryUserGroup);
      final FsPermission fsPermission = permissions == null ? new AbfsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL)
              : AbfsPermission.valueOf(permissions);

      final AclStatus.Builder aclStatusBuilder = new AclStatus.Builder();
      aclStatusBuilder.owner(transformedOwner);
      aclStatusBuilder.group(transformedGroup);

      aclStatusBuilder.setPermission(fsPermission);
      aclStatusBuilder.stickyBit(fsPermission.getStickyBit());
      aclStatusBuilder.addEntries(aclEntries);
      perfInfo.registerSuccess(true);
      return aclStatusBuilder.build();
    }
  }

  public void access(final Path path, final FsAction mode,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    LOG.debug("access for filesystem: {}, path: {}, mode: {}",
        this.client.getFileSystem(), path, mode);
    if (!this.abfsConfiguration.isCheckAccessEnabled()
        || !getIsNamespaceEnabled(tracingContext)) {
      LOG.debug("Returning; either check access is not enabled or the account"
          + " used is not namespace enabled");
      return;
    }
    try (AbfsPerfInfo perfInfo = startTracking("access", "checkAccess")) {
      final AbfsRestOperation op = this.client
          .checkAccess(getRelativePath(path), mode.SYMBOL, tracingContext);
      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    }
  }

  public boolean isAtomicRenameKey(String key) {
    return isKeyForDirectorySet(key, azureAtomicRenameDirSet);
  }

  /**
   * Provides a standard implementation of
   * {@link RenameAtomicityUtils.RedoRenameInvocation}.
   */
  RenameAtomicityUtils.RedoRenameInvocation getRedoRenameInvocation(final TracingContext tracingContext) {
    return new RenameAtomicityUtils.RedoRenameInvocation() {
      @Override
      public void redo(final Path destination, final Path src)
          throws AzureBlobFileSystemException {

        ListBlobQueue listBlobQueue = new ListBlobQueue(
            getAbfsConfiguration().getProducerQueueMaxSize(),
            getAbfsConfiguration().getBlobDirRenameMaxThread());
        StringBuilder listSrcBuilder = new StringBuilder(src.toUri().getPath());
        if (!src.isRoot()) {
          listSrcBuilder.append(FORWARD_SLASH);
        }
        String listSrc = listSrcBuilder.toString();
        getListBlobProducer(listSrc, listBlobQueue, null, tracingContext);
        final AbfsBlobLease abfsBlobLease;
        try {
           abfsBlobLease = getBlobLease(src.toUri().getPath(),
              BLOB_LEASE_ONE_MINUTE_DURATION, tracingContext);
        } catch (AbfsRestOperationException ex) {
          /*
           * The required blob might be deleted in between the last check (from
           * GetFileStatus or ListStatus) and the leaseAcquire. Hence, catching
           * HTTP_NOT_FOUND error.
           */
          if (ex.getStatusCode() == HTTP_NOT_FOUND) {
            return;
          }
          throw ex;
        }
        renameBlobDir(src, destination, tracingContext, listBlobQueue,
            abfsBlobLease, true);
      }
    };
  }

  public boolean isInfiniteLeaseKey(String key) {
    if (azureInfiniteLeaseDirSet.isEmpty()) {
      return false;
    }
    return isKeyForDirectorySet(key, azureInfiniteLeaseDirSet);
  }

  /**
   * A on-off operation to initialize AbfsClient for AzureBlobFileSystem
   * Operations.
   *
   * @param uri            Uniform resource identifier for Abfs.
   * @param fileSystemName Name of the fileSystem being used.
   * @param accountName    Name of the account being used to access Azure
   *                       data store.
   * @param isSecure       Tells if https is being used or http.
   * @throws IOException
   */
  private void initializeClient(URI uri, String fileSystemName,
      String accountName, boolean isSecure)
      throws IOException {
    if (this.client != null) {
      return;
    }

    final String url = getBaseUrlString(fileSystemName, accountName, isSecure);

    URL baseUrl;
    try {
      baseUrl = new URL(url);
    } catch (MalformedURLException e) {
      throw new InvalidUriException(uri.toString());
    }

    SharedKeyCredentials creds = null;
    AccessTokenProvider tokenProvider = null;
    SASTokenProvider sasTokenProvider = null;

    if (authType == AuthType.OAuth) {
      AzureADAuthenticator.init(abfsConfiguration);
    }

    if (authType == AuthType.SharedKey) {
      LOG.trace("Fetching SharedKey credentials");
      int dotIndex = accountName.indexOf(AbfsHttpConstants.DOT);
      if (dotIndex <= 0) {
        throw new InvalidUriException(
                uri.toString() + " - account name is not fully qualified.");
      }
      creds = new SharedKeyCredentials(accountName.substring(0, dotIndex),
            abfsConfiguration.getStorageAccountKey());
    } else if (authType == AuthType.SAS) {
      LOG.trace("Fetching SAS token provider");
      sasTokenProvider = abfsConfiguration.getSASTokenProvider();
    } else {
      LOG.trace("Fetching token provider");
      tokenProvider = abfsConfiguration.getTokenProvider();
      ExtensionHelper.bind(tokenProvider, uri,
            abfsConfiguration.getRawConfiguration());
    }

    LOG.trace("Initializing AbfsClient for {}", baseUrl);
    if (tokenProvider != null) {
      this.client = new AbfsClient(baseUrl, creds, abfsConfiguration,
          tokenProvider,
          populateAbfsClientContext());
    } else {
      this.client = new AbfsClient(baseUrl, creds, abfsConfiguration,
          sasTokenProvider,
          populateAbfsClientContext());
    }
    LOG.trace("AbfsClient init complete");
  }

  private String getBaseUrlString(final String fileSystemName,
      final String accountName,
      final boolean isSecure) {
    final URIBuilder uriBuilder = getURIBuilder(accountName, isSecure);

    final String url = uriBuilder.toString() + AbfsHttpConstants.FORWARD_SLASH
        + fileSystemName;
    return url;
  }

  /**
   * Populate a new AbfsClientContext instance with the desired properties.
   *
   * @return an instance of AbfsClientContext.
   */
  private AbfsClientContext populateAbfsClientContext() {
    return new AbfsClientContextBuilder()
        .withExponentialRetryPolicy(
            new ExponentialRetryPolicy(abfsConfiguration))
        .withAbfsCounters(abfsCounters)
        .withAbfsPerfTracker(abfsPerfTracker)
        .build();
  }

  private String getOctalNotation(FsPermission fsPermission) {
    Preconditions.checkNotNull(fsPermission, "fsPermission");
    return String.format(AbfsHttpConstants.PERMISSION_FORMAT, fsPermission.toOctal());
  }

  private String getRelativePath(final Path path) {
    Preconditions.checkNotNull(path, "path");
    return path.toUri().getPath();
  }

  private long parseContentLength(final String contentLength) {
    if (contentLength == null) {
      return -1;
    }

    return Long.parseLong(contentLength);
  }

  private boolean parseIsDirectory(final String resourceType) {
    return resourceType != null
        && resourceType.equalsIgnoreCase(AbfsHttpConstants.DIRECTORY);
  }

  private String convertXmsPropertiesToCommaSeparatedString(final Hashtable<String, String> properties) throws
          CharacterCodingException {
    StringBuilder commaSeparatedProperties = new StringBuilder();

    final CharsetEncoder encoder = Charset.forName(XMS_PROPERTIES_ENCODING).newEncoder();

    for (Map.Entry<String, String> propertyEntry : properties.entrySet()) {
      String key = propertyEntry.getKey();
      String value = propertyEntry.getValue();

      Boolean canEncodeValue = encoder.canEncode(value);
      if (!canEncodeValue) {
        throw new CharacterCodingException();
      }

      String encodedPropertyValue = Base64.encode(encoder.encode(CharBuffer.wrap(value)).array());
      commaSeparatedProperties.append(key)
              .append(AbfsHttpConstants.EQUAL)
              .append(encodedPropertyValue);

      commaSeparatedProperties.append(AbfsHttpConstants.COMMA);
    }

    if (commaSeparatedProperties.length() != 0) {
      commaSeparatedProperties.deleteCharAt(commaSeparatedProperties.length() - 1);
    }

    return commaSeparatedProperties.toString();
  }

  private Hashtable<String, String> parseCommaSeparatedXmsProperties(String xMsProperties) throws
          InvalidFileSystemPropertyException, InvalidAbfsRestOperationException {
    Hashtable<String, String> properties = new Hashtable<>();

    final CharsetDecoder decoder = Charset.forName(XMS_PROPERTIES_ENCODING).newDecoder();

    if (xMsProperties != null && !xMsProperties.isEmpty()) {
      String[] userProperties = xMsProperties.split(AbfsHttpConstants.COMMA);

      if (userProperties.length == 0) {
        return properties;
      }

      for (String property : userProperties) {
        if (property.isEmpty()) {
          throw new InvalidFileSystemPropertyException(xMsProperties);
        }

        String[] nameValue = property.split(AbfsHttpConstants.EQUAL, 2);
        if (nameValue.length != 2) {
          throw new InvalidFileSystemPropertyException(xMsProperties);
        }

        byte[] decodedValue = Base64.decode(nameValue[1]);

        final String value;
        try {
          value = decoder.decode(ByteBuffer.wrap(decodedValue)).toString();
        } catch (CharacterCodingException ex) {
          throw new InvalidAbfsRestOperationException(ex);
        }
        properties.put(nameValue[0], value);
      }
    }

    return properties;
  }

  private boolean isKeyForDirectorySet(String key, Set<String> dirSet) {
    for (String dir : dirSet) {
      if (dir.isEmpty() || key.startsWith(dir + AbfsHttpConstants.FORWARD_SLASH)) {
        return true;
      }

      try {
        URI uri = new URI(dir);
        if (null == uri.getAuthority()) {
          if (key.startsWith(dir + "/")){
            return true;
          }
        }
      } catch (URISyntaxException e) {
        LOG.info("URI syntax error creating URI for {}", dir);
      }
    }

    return false;
  }

  private AbfsPerfInfo startTracking(String callerName, String calleeName) {
    return new AbfsPerfInfo(abfsPerfTracker, callerName, calleeName);
  }

  /**
   * Returns a pair of fileStatuses for the renamePendingJSON file and the renameSource file.
   * @param fileStatuses array of fileStatus from which pair has to be searched.
   * @return Pair of FileStatus. Left of the pair is fileStatus of renamePendingJson file.
   * Right of the pair is fileStatus of renameSource file.
   */
  public Pair<FileStatus, FileStatus> getRenamePendingFileStatus(final FileStatus[] fileStatuses) {
    Map<String, FileStatus> fileStatusMap = new HashMap<>();
    FileStatus renamePendingJsonFileStatus = null;
    String requiredRenameSrcPath = null;
    for (FileStatus fileStatus : fileStatuses) {
      String path = fileStatus.getPath().toUri().getPath();
      if (path.equals(requiredRenameSrcPath)) {
        return Pair.of(renamePendingJsonFileStatus, fileStatus);
      }
      fileStatusMap.put(path, fileStatus);
      if (path.endsWith(SUFFIX)) {
        renamePendingJsonFileStatus = fileStatus;
        requiredRenameSrcPath = path.split(SUFFIX)[0];
      }
    }
    return Pair.of(renamePendingJsonFileStatus,
        fileStatusMap.get(requiredRenameSrcPath));
  }

  /**
   * A File status with version info extracted from the etag value returned
   * in a LIST or HEAD request.
   * The etag is included in the java serialization.
   */
  static final class VersionedFileStatus extends FileStatus
      implements EtagSource {

    /**
     * The superclass is declared serializable; this subclass can also
     * be serialized.
     */
    private static final long serialVersionUID = -2009013240419749458L;

    /**
     * The etag of an object.
     * Not-final so that serialization via reflection will preserve the value.
     */
    private String version;

    private VersionedFileStatus(
            final String owner, final String group, final FsPermission fsPermission, final boolean hasAcl,
            final long length, final boolean isdir, final int blockReplication,
            final long blocksize, final long modificationTime, final Path path,
            String version) {
      super(length, isdir, blockReplication, blocksize, modificationTime, 0,
              fsPermission,
              owner,
              group,
              null,
              path,
              hasAcl, false, false);

      this.version = version;
    }

    /** Compare if this object is equal to another object.
     * @param   obj the object to be compared.
     * @return  true if two file status has the same path name; false if not.
     */
    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof FileStatus)) {
        return false;
      }

      FileStatus other = (FileStatus) obj;

      if (!this.getPath().equals(other.getPath())) {// compare the path
        return false;
      }

      if (other instanceof VersionedFileStatus) {
        return this.version.equals(((VersionedFileStatus) other).version);
      }

      return true;
    }

    /**
     * Returns a hash code value for the object, which is defined as
     * the hash code of the path name.
     *
     * @return  a hash code value for the path name and version
     */
    @Override
    public int hashCode() {
      int hash = getPath().hashCode();
      hash = 89 * hash + (this.version != null ? this.version.hashCode() : 0);
      return hash;
    }

    /**
     * Returns the version of this FileStatus
     *
     * @return  a string value for the FileStatus version
     */
    public String getVersion() {
      return this.version;
    }

    @Override
    public String getEtag() {
      return getVersion();
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "VersionedFileStatus{");
      sb.append(super.toString());
      sb.append("; version='").append(version).append('\'');
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * A builder class for AzureBlobFileSystemStore.
   */
  public static final class AzureBlobFileSystemStoreBuilder {

    private URI uri;
    private boolean isSecureScheme;
    private Configuration configuration;
    private AbfsCounters abfsCounters;
    private DataBlocks.BlockFactory blockFactory;
    private int blockOutputActiveBlocks;

    public AzureBlobFileSystemStoreBuilder withUri(URI value) {
      this.uri = value;
      return this;
    }

    public AzureBlobFileSystemStoreBuilder withSecureScheme(boolean value) {
      this.isSecureScheme = value;
      return this;
    }

    public AzureBlobFileSystemStoreBuilder withConfiguration(
        Configuration value) {
      this.configuration = value;
      return this;
    }

    public AzureBlobFileSystemStoreBuilder withAbfsCounters(
        AbfsCounters value) {
      this.abfsCounters = value;
      return this;
    }

    public AzureBlobFileSystemStoreBuilder withBlockFactory(
        DataBlocks.BlockFactory value) {
      this.blockFactory = value;
      return this;
    }

    public AzureBlobFileSystemStoreBuilder withBlockOutputActiveBlocks(
        int value) {
      this.blockOutputActiveBlocks = value;
      return this;
    }

    public AzureBlobFileSystemStoreBuilder build() {
      return this;
    }
  }

  @VisibleForTesting
  AbfsClient getClient() {
    return this.client;
  }

  @VisibleForTesting
  void setClient(AbfsClient client) {
    this.client = client;
  }

  @VisibleForTesting
  void setNamespaceEnabled(Trilean isNamespaceEnabled){
    this.isNamespaceEnabled = isNamespaceEnabled;
  }

  private void updateInfiniteLeaseDirs() {
    this.azureInfiniteLeaseDirSet = new HashSet<>(Arrays.asList(
        abfsConfiguration.getAzureInfiniteLeaseDirs().split(AbfsHttpConstants.COMMA)));
    // remove the empty string, since isKeyForDirectory returns true for empty strings
    // and we don't want to default to enabling infinite lease dirs
    this.azureInfiniteLeaseDirSet.remove("");
  }

  private AbfsLease maybeCreateLease(String relativePath, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    boolean enableInfiniteLease = isInfiniteLeaseKey(relativePath);
    if (!enableInfiniteLease) {
      return null;
    }
    final AbfsLease lease;
    if (getPrefixMode() == PrefixMode.DFS) {
      lease = new AbfsDfsLease(client, relativePath, null, tracingContext);
    } else {
      lease = getBlobLease(relativePath, null, tracingContext);
    }
    leaseRefs.put(lease, null);
    return lease;
  }

  @VisibleForTesting
  boolean areLeasesFreed() {
    for (AbfsLease lease : leaseRefs.keySet()) {
      if (lease != null && !lease.isFreed()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Get the etag header from a response, stripping any quotations.
   * see: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag
   * @param result response to process.
   * @return the quote-unwrapped etag.
   */
  static String extractEtagHeader(AbfsHttpOperation result) {
    String etag = result.getResponseHeader(HttpHeaderConfigurations.ETAG);
    if (etag != null) {
      // strip out any wrapper "" quotes which come back, for consistency with
      // list calls
      if (etag.startsWith("W/\"")) {
        // Weak etag
        etag = etag.substring(3);
      } else if (etag.startsWith("\"")) {
        // strong etag
        etag = etag.substring(1);
      }
      if (etag.endsWith("\"")) {
        // trailing quote
        etag = etag.substring(0, etag.length() - 1);
      }
    }
    return etag;
  }
}
