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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.util.Hashtable;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;
import org.apache.hadoop.fs.azurebfs.services.BlobProperty;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.services.PathInformation;
import org.apache.hadoop.fs.azurebfs.services.PrefixMode;
import org.apache.hadoop.fs.azurebfs.services.RenameAtomicityUtils;
import org.apache.hadoop.fs.azurebfs.services.RenameNonAtomicUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsListStatusRemoteIterator;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.FileSystemOperationUnhandledException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriAuthorityException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.SASTokenProviderException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.security.AbfsDelegationTokenManager;
import org.apache.hadoop.fs.azurebfs.services.AbfsCounters;
import org.apache.hadoop.fs.azurebfs.services.AbfsLocatedFileStatus;
import org.apache.hadoop.fs.azurebfs.utils.Listener;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;
import org.apache.hadoop.fs.impl.AbstractFSBuilderImpl;
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.store.DataBlocks;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.LambdaUtils;
import org.apache.hadoop.util.Progressable;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IOSTATISTICS_LOGGING_LEVEL;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IOSTATISTICS_LOGGING_LEVEL_DEFAULT;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.*;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_BLOB_ENDPOINT;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_DNS_PREFIX;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.WASB_DNS_PREFIX;
import static org.apache.hadoop.fs.azurebfs.services.RenameAtomicityUtils.SUFFIX;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.DATA_BLOCKS_BUFFER;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_BLOCK_UPLOAD_ACTIVE_BLOCKS;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_BLOCK_UPLOAD_BUFFER_DIR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.BLOCK_UPLOAD_ACTIVE_BLOCKS_DEFAULT;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DATA_BLOCKS_BUFFER_DEFAULT;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.PATH_EXISTS;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.RENAME_DESTINATION_PARENT_PATH_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.constants.InternalConstants.CAPABILITY_SAFE_READAHEAD;
import static org.apache.hadoop.fs.azurebfs.utils.UriUtils.decodeMetadataAttribute;
import static org.apache.hadoop.fs.azurebfs.utils.UriUtils.encodeMetadataAttribute;
import static org.apache.hadoop.fs.impl.PathCapabilitiesSupport.validatePathCapabilityArgs;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.logIOStatisticsAtLevel;
import static org.apache.hadoop.util.functional.RemoteIterators.filteringRemoteIterator;
import static org.apache.hadoop.util.functional.RemoteIterators.mappingRemoteIterator;

/**
 * A {@link org.apache.hadoop.fs.FileSystem} for reading and writing files stored on <a
 * href="http://store.azure.com/">Windows Azure</a>
 */
@InterfaceStability.Evolving
public class AzureBlobFileSystem extends FileSystem
    implements IOStatisticsSource {
  public static final Logger LOG = LoggerFactory.getLogger(AzureBlobFileSystem.class);
  private URI uri;
  private Path workingDir;
  private AzureBlobFileSystemStore abfsStore;
  private boolean isClosed;
  private final String fileSystemId = UUID.randomUUID().toString();

  private boolean delegationTokenEnabled = false;
  private AbfsDelegationTokenManager delegationTokenManager;
  private AbfsCounters abfsCounters;
  private String clientCorrelationId;
  private TracingHeaderFormat tracingHeaderFormat;
  private Listener listener;

  /** Name of blockFactory to be used by AbfsOutputStream. */
  private String blockOutputBuffer;
  /** BlockFactory instance to be used. */
  private DataBlocks.BlockFactory blockFactory;
  /** Maximum Active blocks per OutputStream. */
  private int blockOutputActiveBlocks;
  private PrefixMode prefixMode = PrefixMode.DFS;
  private boolean isNamespaceEnabled;
  private NativeAzureFileSystem nativeFs;

  @Override
  public void initialize(URI uri, Configuration configuration)
      throws IOException {
    uri = ensureAuthority(uri, configuration);
    super.initialize(uri, configuration);
    setConf(configuration);

    LOG.debug("Initializing AzureBlobFileSystem for {}", uri);

    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    abfsCounters = new AbfsCountersImpl(uri);
    // name of the blockFactory to be used.
    this.blockOutputBuffer = configuration.getTrimmed(DATA_BLOCKS_BUFFER,
        DATA_BLOCKS_BUFFER_DEFAULT);
    // blockFactory used for this FS instance.
    this.blockFactory =
        DataBlocks.createFactory(FS_AZURE_BLOCK_UPLOAD_BUFFER_DIR,
            configuration, blockOutputBuffer);
    this.blockOutputActiveBlocks =
        configuration.getInt(FS_AZURE_BLOCK_UPLOAD_ACTIVE_BLOCKS,
            BLOCK_UPLOAD_ACTIVE_BLOCKS_DEFAULT);
    if (blockOutputActiveBlocks < 1) {
      blockOutputActiveBlocks = 1;
    }

    if (configuration.getBoolean(FS_AZURE_ENABLE_BLOB_ENDPOINT, false)) {
      if (uri.toString().contains(FileSystemUriSchemes.ABFS_DNS_PREFIX)) {
        uri = changePrefixFromDfsToBlob(uri);
        this.uri = uri;
      }
    }

    // AzureBlobFileSystemStore with params in builder.
    AzureBlobFileSystemStore.AzureBlobFileSystemStoreBuilder
        systemStoreBuilder =
        new AzureBlobFileSystemStore.AzureBlobFileSystemStoreBuilder()
            .withUri(uri)
            .withSecureScheme(this.isSecureScheme())
            .withConfiguration(configuration)
            .withAbfsCounters(abfsCounters)
            .withBlockFactory(blockFactory)
            .withBlockOutputActiveBlocks(blockOutputActiveBlocks)
            .build();

    this.abfsStore = new AzureBlobFileSystemStore(systemStoreBuilder);
    LOG.trace("AzureBlobFileSystemStore init complete");

    final AbfsConfiguration abfsConfiguration = abfsStore
        .getAbfsConfiguration();
    clientCorrelationId = TracingContext.validateClientCorrelationID(
        abfsConfiguration.getClientCorrelationId());
    tracingHeaderFormat = abfsConfiguration.getTracingHeaderFormat();
    this.setWorkingDirectory(this.getHomeDirectory());

    TracingContext tracingContext = new TracingContext(clientCorrelationId,
            fileSystemId, FSOperationType.CREATE_FILESYSTEM, tracingHeaderFormat, listener);
    try {
      isNamespaceEnabled = getIsNamespaceEnabled(tracingContext);
    } catch (AbfsRestOperationException ex) {
      /* since the filesystem has not been created. The API for HNS account would
       * return 404 status.
       */
      if(ex.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
        isNamespaceEnabled = true;
      } else {
        throw ex;
      }
    }
    // CPK is not supported over blob endpoint and hence initialization should fail if key is not null.
    if (!isNamespaceEnabled && (abfsConfiguration.shouldEnableBlobEndPoint() ||
            uri.toString().contains(FileSystemUriSchemes.WASB_DNS_PREFIX))) {
      if (abfsConfiguration.getClientProvidedEncryptionKey() == null) {
        this.prefixMode = PrefixMode.BLOB;
      } else {
        throw new InvalidConfigurationValueException("CPK is not supported over blob endpoint " + uri);
      }
    }
    abfsConfiguration.setPrefixMode(this.prefixMode);
    if (abfsConfiguration.getCreateRemoteFileSystemDuringInitialization()) {
      if (this.tryGetFileStatus(new Path(AbfsHttpConstants.ROOT_PATH), tracingContext) == null) {
        try {
          this.createFileSystem(tracingContext);
        } catch (AzureBlobFileSystemException ex) {
          checkException(null, ex, AzureServiceErrorCode.FILE_SYSTEM_ALREADY_EXISTS);
        }
      }
    }

    LOG.trace("Initiate check for delegation token manager");
    if (UserGroupInformation.isSecurityEnabled()) {
      this.delegationTokenEnabled = abfsConfiguration.isDelegationTokenManagerEnabled();

      if (this.delegationTokenEnabled) {
        LOG.debug("Initializing DelegationTokenManager for {}", uri);
        this.delegationTokenManager = abfsConfiguration.getDelegationTokenManager();
        delegationTokenManager.bind(getUri(), configuration);
        LOG.debug("Created DelegationTokenManager {}", delegationTokenManager);
      }
    }

    boolean isRedirect = abfsConfiguration.isRedirection();
    if (isRedirect) {
      String abfsUrl = uri.toString();
      URI wasbUri = null;
      try {
        wasbUri = new URI(abfsUrlToWasbUrl(abfsUrl,
            abfsStore.getAbfsConfiguration().isHttpsAlwaysUsed()));
      } catch (URISyntaxException e) {
        e.printStackTrace();
      }
      nativeFs = new NativeAzureFileSystem();
      Configuration config = getConf();
      try {
        nativeFs.initialize(wasbUri, config);
      } catch (IOException e) {
        LOG.debug("Initializing  NativeAzureBlobFileSystem failed ", e);
        throw e;
      }
    }
    LOG.debug("Initializing AzureBlobFileSystem for {} complete", uri);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "AzureBlobFileSystem{");
    sb.append("uri=").append(uri);
    sb.append(", user='").append(abfsStore.getUser()).append('\'');
    sb.append(", primaryUserGroup='").append(abfsStore.getPrimaryGroup()).append('\'');
    sb.append("[" + CAPABILITY_SAFE_READAHEAD + "]");
    sb.append('}');
    return sb.toString();
  }

  public boolean isSecureScheme() {
    return false;
  }

  @Override
  public URI getUri() {
    return this.uri;
  }

  public void registerListener(Listener listener1) {
    listener = listener1;
  }

  private static String convertTestUrls(
      final String url,
      final String fromNonSecureScheme,
      final String fromSecureScheme,
      final String fromDnsPrefix,
      final String toNonSecureScheme,
      final String toSecureScheme,
      final String toDnsPrefix,
      final boolean isAlwaysHttpsUsed) {
    String data = null;
    if (url.startsWith(fromNonSecureScheme + "://") && isAlwaysHttpsUsed) {
      data = url.replace(fromNonSecureScheme + "://", toSecureScheme + "://");
    } else if (url.startsWith(fromNonSecureScheme + "://")) {
      data = url.replace(fromNonSecureScheme + "://", toNonSecureScheme + "://");
    } else if (url.startsWith(fromSecureScheme + "://")) {
      data = url.replace(fromSecureScheme + "://", toSecureScheme + "://");
    }

    if (data != null) {
      data = data.replace(fromDnsPrefix , toDnsPrefix);
    }
    return data;
  }

  protected static String abfsUrlToWasbUrl(final String abfsUrl, final boolean isAlwaysHttpsUsed) {
    return convertTestUrls(
            abfsUrl, FileSystemUriSchemes.ABFS_SCHEME, FileSystemUriSchemes.ABFS_SECURE_SCHEME, FileSystemUriSchemes.ABFS_DNS_PREFIX,
            FileSystemUriSchemes.WASB_SCHEME, FileSystemUriSchemes.WASB_SECURE_SCHEME, FileSystemUriSchemes.WASB_DNS_PREFIX, isAlwaysHttpsUsed);
  }

  private URI changePrefixFromDfsToBlob(URI uri) throws InvalidUriException {
    try {
      String uriString = uri.toString().replace(ABFS_DNS_PREFIX, WASB_DNS_PREFIX);
      return new URI(uriString);
    } catch (URISyntaxException ex) {
      throw new InvalidUriException(uri.toString());
    }
  }

  @Override
  public FSDataInputStream open(final Path path, final int bufferSize) throws IOException {
    LOG.debug("AzureBlobFileSystem.open path: {} bufferSize: {}", path, bufferSize);
    // bufferSize is unused.
    return open(path, Optional.empty());
  }

  private FSDataInputStream open(final Path path,
      final Optional<Configuration> options) throws IOException {
    statIncrement(CALL_OPEN);
    Path qualifiedPath = makeQualified(path);

    try {
      TracingContext tracingContext = new TracingContext(clientCorrelationId,
          fileSystemId, FSOperationType.OPEN, tracingHeaderFormat,
          listener);
      InputStream inputStream = getAbfsStore().openFileForRead(qualifiedPath,
          options, statistics, tracingContext);
      return new FSDataInputStream(inputStream);
    } catch(AzureBlobFileSystemException ex) {
      checkException(path, ex);
      return null;
    }
  }

  @Override
  protected CompletableFuture<FSDataInputStream> openFileWithOptions(
      final Path path, final OpenFileParameters parameters) throws IOException {
    LOG.debug("AzureBlobFileSystem.openFileWithOptions path: {}", path);
    AbstractFSBuilderImpl.rejectUnknownMandatoryKeys(
        parameters.getMandatoryKeys(),
        Collections.emptySet(),
        "for " + path);
    return LambdaUtils.eval(
        new CompletableFuture<>(), () ->
            open(path, Optional.of(parameters.getOptions())));
  }

  /**
   * This handling makes sure that if request to create file for an existing directory or subpath
   * comes that should fail.
   * @param path The path to validate.
   * @param tracingContext The tracingContext.
   */
  private void validatePathOrSubPathDoesNotExist(final Path path, TracingContext tracingContext) throws IOException {
    List<BlobProperty> blobList = abfsStore.getListBlobs(path, null,
            tracingContext, 2, true);
    if (blobList.size() > 0 || abfsStore.checkIsDirectory(path, tracingContext)) {
      throw new AbfsRestOperationException(HTTP_CONFLICT,
              AzureServiceErrorCode.PATH_CONFLICT.getErrorCode(),
              PATH_EXISTS,
              null);
    }
  }

  private boolean shouldRedirect(FSOperationType type, TracingContext context)
          throws AzureBlobFileSystemException {
    if (getIsNamespaceEnabled(context)) {
      return false;
    }
    switch (type) {
      case DELETE:
        return abfsStore.getAbfsConfiguration().shouldRedirectDelete();
      case RENAME:
        return abfsStore.getAbfsConfiguration().shouldRedirectRename();
    }

    return false;
  }

  // Fallback plan : default to v1 create flow which will hit dfs endpoint. Config to enable: "fs.azure.ingress.fallback.to.dfs".
  @Override
  public FSDataOutputStream create(final Path f, final FsPermission permission, final boolean overwrite, final int bufferSize,
      final short replication, final long blockSize, final Progressable progress) throws IOException {
    LOG.debug("AzureBlobFileSystem.create path: {} permission: {} overwrite: {} bufferSize: {}",
        f,
        permission,
        overwrite,
        blockSize);

    statIncrement(CALL_CREATE);
    trailingPeriodCheck(f);
    TracingContext tracingContext = new TracingContext(clientCorrelationId,
            fileSystemId, FSOperationType.CREATE, overwrite, tracingHeaderFormat, listener);

    Path qualifiedPath = makeQualified(f);
    // This fix is needed for create idempotency, should throw error if overwrite is false and file status is not null.
    boolean fileOverwrite = overwrite;
    if (!fileOverwrite) {
      FileStatus fileStatus = tryGetFileStatus(qualifiedPath, tracingContext);
      if (fileStatus != null) {
        // path references a file and overwrite is disabled
        throw new FileAlreadyExistsException(f + " already exists");
      }
      fileOverwrite = true;
    }

    if (prefixMode == PrefixMode.BLOB) {
      validatePathOrSubPathDoesNotExist(qualifiedPath, tracingContext);
      Path parent = qualifiedPath.getParent();
      if (parent != null && !parent.isRoot()) {
          mkdirs(parent);
      }
    }

    try {
      OutputStream outputStream = getAbfsStore().createFile(qualifiedPath, statistics, fileOverwrite,
          permission == null ? FsPermission.getFileDefault() : permission,
          FsPermission.getUMask(getConf()), tracingContext, null);
      statIncrement(FILES_CREATED);
      return new FSDataOutputStream(outputStream, statistics);
    } catch(AzureBlobFileSystemException ex) {
      checkException(f, ex);
      return null;
    }
  }

  @Override
  @SuppressWarnings("deprecation")
  public FSDataOutputStream createNonRecursive(final Path f, final FsPermission permission,
      final boolean overwrite, final int bufferSize, final short replication, final long blockSize,
      final Progressable progress) throws IOException {

    statIncrement(CALL_CREATE_NON_RECURSIVE);
    final Path parent = f.getParent();
    TracingContext tracingContext = new TracingContext(clientCorrelationId,
        fileSystemId, FSOperationType.CREATE_NON_RECURSIVE, tracingHeaderFormat,
        listener);
    final FileStatus parentFileStatus = tryGetFileStatus(parent, tracingContext);

    if (parentFileStatus == null) {
      throw new FileNotFoundException("Cannot create file "
          + f.getName() + " because parent folder does not exist.");
    }

    return create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  @SuppressWarnings("deprecation")
  public FSDataOutputStream createNonRecursive(final Path f, final FsPermission permission,
      final EnumSet<CreateFlag> flags, final int bufferSize, final short replication, final long blockSize,
      final Progressable progress) throws IOException {

    // Check if file should be appended or overwritten. Assume that the file
    // is overwritten on if the CREATE and OVERWRITE create flags are set.
    final EnumSet<CreateFlag> createflags =
        EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE);
    final boolean overwrite = flags.containsAll(createflags);

    // Delegate the create non-recursive call.
    return this.createNonRecursive(f, permission, overwrite,
        bufferSize, replication, blockSize, progress);
  }

  @Override
  @SuppressWarnings("deprecation")
  public FSDataOutputStream createNonRecursive(final Path f,
      final boolean overwrite, final int bufferSize, final short replication, final long blockSize,
      final Progressable progress) throws IOException {
    return this.createNonRecursive(f, FsPermission.getFileDefault(),
        overwrite, bufferSize, replication, blockSize, progress);
  }

  // Fallback plan : default to v1 append flow which will hit dfs endpoint. Config to enable: "fs.azure.ingress.fallback.to.dfs".
  @Override
  public FSDataOutputStream append(final Path f, final int bufferSize, final Progressable progress) throws IOException {
    LOG.debug(
        "AzureBlobFileSystem.append path: {} bufferSize: {}",
        f.toString(),
        bufferSize);
    statIncrement(CALL_APPEND);
    Path qualifiedPath = makeQualified(f);

    try {
      TracingContext tracingContext = new TracingContext(clientCorrelationId,
          fileSystemId, FSOperationType.APPEND, tracingHeaderFormat,
          listener);
      OutputStream outputStream = getAbfsStore()
          .openFileForWrite(qualifiedPath, statistics, false, tracingContext);
      return new FSDataOutputStream(outputStream, statistics);
    } catch(AzureBlobFileSystemException ex) {
      checkException(f, ex);
      return null;
    }
  }

  /**
   * Checks if the paths are allowable for executing rename steps over Blob endpoint.
   *
   * @param src source which has to be renamed
   * @param dst destination which will contain the renamed blobs.
   * @param fnsDstPathInformation pathInformation for the given dst
   * @param tracingContext tracingContext for tracing the server calls
   *
   * @return true if all checks pass.
   * @throws IOException exceptions received in the server calls. Or the exceptions
   * thrown from the client which are equal to the exceptions that would be thrown
   * from the DFS endpoint server for certain scenarios.
   */
  private Boolean applyBlobRenameChecks(final Path src,
      final Path dst,
      final PathInformation fnsDstPathInformation,
      final TracingContext tracingContext)
      throws IOException {
    if (containsColon(dst)) {
      throw new IOException("Cannot rename to file " + dst
          + " that has colons in the name through blob endpoint");
    }

    Path qualifiedSrcPath = makeQualified(src);
    Path qualifiedDstPath = makeQualified(dst);
    Path nestedDstParent = dst.getParent();

    /*
     * Special case 1:
     * For blob endpoint with non-HNS account, client has to ensure that destination
     * is not a sub-directory of source.
     */
    LOG.debug("Check if the destination is subDirectory");
    if (nestedDstParent != null && makeQualified(nestedDstParent).toUri()
        .getPath()
        .indexOf(qualifiedSrcPath.toUri().getPath()) == 0) {
      LOG.info("Rename src: {} dst: {} failed as dst is subDir of src",
          qualifiedSrcPath, qualifiedDstPath);
      return false;
    }

    if (fnsDstPathInformation.getPathExists()) {
      if (fnsDstPathInformation.getIsDirectory()) {
        /*
         * For blob-endpoint with nonHNS account, check if the qualifiedDstPath
         * exist in backend. If yes, HTTP_CONFLICT exception has to be thrown.
         */

        String sourceFileName = src.getName();
        Path adjustedDst = new Path(dst, sourceFileName);
        qualifiedDstPath = makeQualified(adjustedDst);

        final PathInformation qualifiedDstPathInformation
            = getPathInformation(qualifiedDstPath, tracingContext
        );
        final Boolean isQualifiedDstExists
            = qualifiedDstPathInformation.getPathExists();
        if (isQualifiedDstExists) {
          //destination already there. Rename should not be overwriting.
          LOG.info(
              "Rename src: {} dst: {} failed as qualifiedDst already exists",
              qualifiedSrcPath, qualifiedDstPath);
          throw new AbfsRestOperationException(
              HttpURLConnection.HTTP_CONFLICT,
              AzureServiceErrorCode.PATH_ALREADY_EXISTS.getErrorCode(), null,
              null);
        }
      }
    } else {
      /*
       * If the destination doesn't exist, check if parent of destination exists.
       */
      Path adjustedDst = dst;
      qualifiedDstPath = makeQualified(adjustedDst);
      Path parent = qualifiedDstPath.getParent();
      if (parent != null && !parent.isRoot()) {
        PathInformation dstParentPathInformation = getPathInformation(parent,
            tracingContext
        );
        final Boolean dstParentPathExists
            = dstParentPathInformation.getPathExists();
        final Boolean isDstParentPathDirectory
            = dstParentPathInformation.getIsDirectory();
        if (!dstParentPathExists || !isDstParentPathDirectory) {
          LOG.info("parent of {} is {} is not directory. Failing rename",
              adjustedDst, parent);
          throw new AbfsRestOperationException(
              HttpURLConnection.HTTP_NOT_FOUND,
              RENAME_DESTINATION_PARENT_PATH_NOT_FOUND.getErrorCode(), null,
              new Exception(
                  RENAME_DESTINATION_PARENT_PATH_NOT_FOUND.getErrorCode()));
        }
      }
    }
    return true;
  }

  public boolean rename(final Path src, final Path dst) throws IOException {
    LOG.debug("AzureBlobFileSystem.rename src: {} dst: {} via {} endpoint", src, dst,
        getAbfsStore().getAbfsConfiguration().getPrefixMode());
    statIncrement(CALL_RENAME);

    TracingContext tracingContext = new TracingContext(clientCorrelationId,
            fileSystemId, FSOperationType.RENAME, true, tracingHeaderFormat,
            listener);

    if (shouldRedirect(FSOperationType.RENAME, tracingContext)) {
      LOG.debug("Rename redirected for the given src {} and the given destination {}", src, dst);
      Path wasbSrc = src;
      Path wasbDest = dst;
      if (FileSystemUriSchemes.ABFS_SCHEME.equalsIgnoreCase(src.toUri().getScheme())
              || FileSystemUriSchemes.ABFS_SECURE_SCHEME.equalsIgnoreCase(src.toUri().getScheme())) {
        wasbSrc = new Path(abfsUrlToWasbUrl(src.toString(),
                abfsStore.getAbfsConfiguration().isHttpsAlwaysUsed()));
      }
      if (FileSystemUriSchemes.ABFS_SCHEME.equalsIgnoreCase(dst.toUri().getScheme())
              || FileSystemUriSchemes.ABFS_SECURE_SCHEME.equalsIgnoreCase(dst.toUri().getScheme())) {
        wasbDest = new Path(abfsUrlToWasbUrl(dst.toString(),
                abfsStore.getAbfsConfiguration().isHttpsAlwaysUsed()));
      }
      try {
        return getNativeFs().rename(wasbSrc, wasbDest);
      } catch (IOException e) {
        LOG.debug("Rename redirection failed for the given src {} and the given destination {}", src, dst);
        throw e;
      }
    }

    trailingPeriodCheck(dst);

    Path parentFolder = src.getParent();
    if (parentFolder == null) {
      return false;
    }
    Path qualifiedSrcPath = makeQualified(src);
    Path qualifiedDstPath = makeQualified(dst);

    // special case 2:
    // rename under same folder;
    if (makeQualified(parentFolder).equals(qualifiedDstPath)) {
      PathInformation pathInformation = getPathInformation(qualifiedSrcPath,
          tracingContext);
      return pathInformation.getPathExists();
    }

    //special case 3:
    if (qualifiedSrcPath.equals(qualifiedDstPath)) {
      // rename to itself
      // - if it doesn't exist, return false
      // - if it is file, return true
      // - if it is dir, return false.

      final PathInformation pathInformation = getPathInformation(
          qualifiedDstPath, tracingContext
      );
      final Boolean isDstExists = pathInformation.getPathExists();
      final Boolean isDstDirectory = pathInformation.getIsDirectory();
      if (!isDstExists) {
        return false;
      }
      return isDstDirectory ? false : true;
    }

    // special case 4:
    // Non-HNS account need to check dst status on driver side.
    PathInformation fnsPathInformation = null;
    if (!abfsStore.getIsNamespaceEnabled(tracingContext)) {
      fnsPathInformation = getPathInformation(qualifiedDstPath, tracingContext
      );
    }

    try {
      final Boolean isFnsDstExists, isFnsDstDirectory;
      if (fnsPathInformation != null) {
        isFnsDstDirectory = fnsPathInformation.getIsDirectory();
        isFnsDstExists = fnsPathInformation.getPathExists();
      } else {
        isFnsDstExists = false;
        isFnsDstDirectory = false;
      }
      String sourceFileName = src.getName();
      Path adjustedDst = dst;

      if (isFnsDstExists) {
        if (!isFnsDstDirectory) {
          return qualifiedSrcPath.equals(qualifiedDstPath);
        }
        adjustedDst = new Path(dst, sourceFileName);
      }
      qualifiedDstPath = makeQualified(adjustedDst);
      LOG.debug("Qualified dst path: {}", qualifiedDstPath);

      final RenameAtomicityUtils renameAtomicityUtils;
      if (getAbfsStore().getAbfsConfiguration().getPrefixMode()
          == PrefixMode.BLOB &&
          abfsStore.isAtomicRenameKey(qualifiedSrcPath.toUri().getPath())) {
        renameAtomicityUtils = new RenameAtomicityUtils(this,
            qualifiedSrcPath, qualifiedDstPath, tracingContext);
      } else {
        renameAtomicityUtils = new RenameNonAtomicUtils(this,
            qualifiedSrcPath, qualifiedDstPath, tracingContext);
      }
      if(getAbfsStore().getAbfsConfiguration().getPrefixMode() == PrefixMode.BLOB) {
        if (!applyBlobRenameChecks(src, dst, fnsPathInformation,
            tracingContext)) {
          return false;
        }
      }
      getAbfsStore().rename(qualifiedSrcPath, qualifiedDstPath, renameAtomicityUtils,
          tracingContext);
      return true;
    } catch (AzureBlobFileSystemException ex) {
      LOG.debug("Rename operation failed. ", ex);
      checkException(
              src,
              ex,
              AzureServiceErrorCode.PATH_ALREADY_EXISTS,
              AzureServiceErrorCode.INVALID_RENAME_SOURCE_PATH,
              AzureServiceErrorCode.SOURCE_PATH_NOT_FOUND,
              AzureServiceErrorCode.INVALID_SOURCE_OR_DESTINATION_RESOURCE_TYPE,
              AzureServiceErrorCode.RENAME_DESTINATION_PARENT_PATH_NOT_FOUND,
              AzureServiceErrorCode.INTERNAL_OPERATION_ABORT);
      return false;
    }
  }

  /**
   * Defines if the given path exists and if is it a directory.<br>
   * If the path check is for a blob (non-HNS over blob-endpoint). First it will
   * call the listBlob API on the given path. If it returns list of object, then
   * it can be defined that it exist and it is a directory. Else, it will call
   * getBlobProperties API on the path. If it returns an object, it can be defined
   * that the path exists. If the object contains the metadata
   * {@link org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations#X_MS_META_HDI_ISFOLDER}.
   * If yes, the path can be defined as directory.<br>
   * If the path check is for a path on dfs-endpoint, getPathStatus API for the path
   * shall be called. If the response returned an object, the path can be defined
   * as existing. If the response's metadata contains it is directory, the path
   * can be defined as a directory.
   *
   * @param path path for which information is requried.
   * @param tracingContext tracingContext for the operations.
   *
   * @return pathInformation containing if path exists and is a directory.
   *
   * @throws AzureBlobFileSystemException exceptions caught from the server calls.
   */
  private PathInformation getPathInformation(final Path path,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    if (getAbfsStore().getAbfsConfiguration().getPrefixMode()
        == PrefixMode.BLOB) {
      List<BlobProperty> blobProperties = getAbfsStore()
          .getListBlobs(path, null, tracingContext, 2, true);
      if (blobProperties.size() > 0) {
        return new PathInformation(true, true);
      }
      BlobProperty blobProperty;
      try {
        blobProperty = getAbfsStore().getBlobProperty(path, tracingContext);
      } catch (AbfsRestOperationException ex) {
        if (ex.getStatusCode() != HttpURLConnection.HTTP_NOT_FOUND) {
          throw ex;
        }
        blobProperty = null;
      }
      if (blobProperty != null) {
        return new PathInformation(true, blobProperty.getIsDirectory());
      }
    } else {
      final FileStatus fileStatus = tryGetFileStatus(path,
          tracingContext);
      if (fileStatus != null) {
        return new PathInformation(true, fileStatus.isDirectory());
      }
    }
    return new PathInformation(false, false);
  }

  @Override
  public boolean delete(final Path f, final boolean recursive) throws IOException {
    LOG.debug(
        "AzureBlobFileSystem.delete path: {} recursive: {}", f.toString(), recursive);
    statIncrement(CALL_DELETE);
    Path qualifiedPath = makeQualified(f);
    TracingContext tracingContext = new TracingContext(clientCorrelationId,
            fileSystemId, FSOperationType.DELETE, tracingHeaderFormat,
            listener);

    if (shouldRedirect(FSOperationType.DELETE, tracingContext)) {
      LOG.debug("Delete redirected for the given path {} ", qualifiedPath);
      Path wasbPath = f;
      if (FileSystemUriSchemes.ABFS_SCHEME.equalsIgnoreCase(wasbPath.toUri().getScheme())
              || FileSystemUriSchemes.ABFS_SECURE_SCHEME.equalsIgnoreCase(wasbPath.toUri().getScheme())) {
        wasbPath = new Path(abfsUrlToWasbUrl(wasbPath.toString(),
                abfsStore.getAbfsConfiguration().isHttpsAlwaysUsed()));
      }
      try {
        return getNativeFs().delete(wasbPath, recursive);
      } catch (IOException e) {
        LOG.debug("Delete redirection failed for the given path {} ", qualifiedPath);
        throw e;
      }
    }

    if (f.isRoot()) {
      if (!recursive) {
        return false;
      }

      return deleteRoot();
    }

    try {
      getAbfsStore().delete(qualifiedPath, recursive, tracingContext);
      return true;
    } catch (AzureBlobFileSystemException ex) {
      checkException(f, ex, AzureServiceErrorCode.PATH_NOT_FOUND);
      return false;
    }

  }

  @Override
  public FileStatus[] listStatus(final Path f) throws IOException {
    LOG.debug(
        "AzureBlobFileSystem.listStatus path: {}", f.toString());
    statIncrement(CALL_LIST_STATUS);
    Path qualifiedPath = makeQualified(f);

    try {
      TracingContext tracingContext = new TracingContext(clientCorrelationId,
          fileSystemId, FSOperationType.LISTSTATUS, true, tracingHeaderFormat,
          listener);
      FileStatus[] result = abfsStore.listStatus(qualifiedPath, tracingContext);
      if (getAbfsStore().getAbfsConfiguration().getPrefixMode()
          == PrefixMode.BLOB) {
        FileStatus renamePendingFileStatus
            = abfsStore.getRenamePendingFileStatus(result);
        if (renamePendingFileStatus != null) {
          RenameAtomicityUtils renameAtomicityUtils =
              new RenameAtomicityUtils(this,
                  renamePendingFileStatus.getPath(),
                  abfsStore.getRedoRenameInvocation(tracingContext));
          renameAtomicityUtils.cleanup(renamePendingFileStatus.getPath());
          result = abfsStore.listStatus(qualifiedPath, tracingContext);
        }
      }
      return result;
    } catch (AzureBlobFileSystemException ex) {
      checkException(f, ex);
      return null;
    }
  }

  /**
   * Increment of an Abfs statistic.
   *
   * @param statistic AbfsStatistic that needs increment.
   */
  private void statIncrement(AbfsStatistic statistic) {
    incrementStatistic(statistic);
  }

  /**
   * Method for incrementing AbfsStatistic by a long value.
   *
   * @param statistic the Statistic to be incremented.
   */
  private void incrementStatistic(AbfsStatistic statistic) {
    if (abfsCounters != null) {
      abfsCounters.incrementCounter(statistic, 1);
    }
  }

  /**
   * Performs a check for (.) until root in the path to throw an exception.
   * The purpose is to differentiate between dir/dir1 and dir/dir1.
   * Without the exception the behavior seen is dir1. will appear
   * to be present without it's actual creation as dir/dir1 and dir/dir1. are
   * treated as identical.
   * @param path the path to be checked for trailing period (.)
   * @throws IllegalArgumentException if the path has a trailing period (.)
   */
  private void trailingPeriodCheck(Path path) throws IllegalArgumentException {
    while (!path.isRoot()){
      String pathToString = path.toString();
      if (pathToString.length() != 0) {
        if (pathToString.charAt(pathToString.length() - 1) == '.') {
          throw new IllegalArgumentException(
              "ABFS does not allow files or directories to end with a dot.");
        }
        path = path.getParent();
      }
      else {
        break;
      }
    }
  }

  // Fallback plan : default to v1 Mkdir flow which will hit dfs endpoint. Config to enable: "fs.azure.mkdirs.fallback.to.dfs".
  @Override
  public boolean mkdirs(final Path f, final FsPermission permission) throws IOException {
    LOG.debug(
        "AzureBlobFileSystem.mkdirs path: {} permissions: {}", f, permission);
    statIncrement(CALL_MKDIRS);
    trailingPeriodCheck(f);

    final Path parentFolder = f.getParent();
    if (parentFolder == null) {
      // Cannot create root
      return true;
    }

    Path qualifiedPath = makeQualified(f);

    try {
      TracingContext tracingContext = new TracingContext(clientCorrelationId,
              fileSystemId, FSOperationType.MKDIR, false, tracingHeaderFormat,
              listener);
      abfsStore.createDirectory(qualifiedPath, statistics,
              permission == null ? FsPermission.getDirDefault() : permission,
              FsPermission.getUMask(getConf()), tracingContext);
      statIncrement(DIRECTORIES_CREATED);
      return true;
    } catch (AzureBlobFileSystemException ex) {
      checkException(f, ex);
      return true;
    }
  }


  @Override
  public synchronized void close() throws IOException {
    if (isClosed) {
      return;
    }
    // does all the delete-on-exit calls, and may be slow.
    super.close();
    LOG.debug("AzureBlobFileSystem.close");
    if (getConf() != null) {
      String iostatisticsLoggingLevel =
          getConf().getTrimmed(IOSTATISTICS_LOGGING_LEVEL,
              IOSTATISTICS_LOGGING_LEVEL_DEFAULT);
      logIOStatisticsAtLevel(LOG, iostatisticsLoggingLevel, getIOStatistics());
    }
    IOUtils.cleanupWithLogger(LOG, abfsStore, delegationTokenManager);
    this.isClosed = true;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Closing Abfs: {}", toString());
    }
  }

  @Override
  public FileStatus getFileStatus(final Path f) throws IOException {
      TracingContext tracingContext = new TracingContext(clientCorrelationId,
          fileSystemId, FSOperationType.GET_FILESTATUS, tracingHeaderFormat,
          listener);
      return getFileStatus(f, tracingContext);
  }

  private FileStatus getFileStatus(final Path path,
      TracingContext tracingContext) throws IOException {
    LOG.debug("AzureBlobFileSystem.getFileStatus path: {}", path);
    statIncrement(CALL_GET_FILE_STATUS);
    Path qualifiedPath = makeQualified(path);
    FileStatus fileStatus;

    try {
      if (abfsStore.getPrefixMode() == PrefixMode.BLOB) {
        /**
         * Get File Status over Blob Endpoint will Have an additional call
         * to check if directory is implicit.
         */
        fileStatus = abfsStore.getFileStatus(qualifiedPath, tracingContext, true);
      }
      else {
        fileStatus = abfsStore.getFileStatus(qualifiedPath, tracingContext, false);
      }
      if (abfsStore.getPrefixMode() == PrefixMode.BLOB && fileStatus != null && fileStatus.isDirectory()
          &&
          abfsStore.isAtomicRenameKey(fileStatus.getPath().toUri().getPath()) &&
          abfsStore.getRenamePendingFileStatusInDirectory(fileStatus,
              tracingContext)) {
        RenameAtomicityUtils renameAtomicityUtils = new RenameAtomicityUtils(
            this,
            new Path(fileStatus.getPath().toUri().getPath() + SUFFIX),
            abfsStore.getRedoRenameInvocation(tracingContext));
        renameAtomicityUtils.cleanup(
            new Path(fileStatus.getPath().toUri().getPath() + SUFFIX));
        throw new AbfsRestOperationException(HttpURLConnection.HTTP_NOT_FOUND,
            AzureServiceErrorCode.PATH_NOT_FOUND.getErrorCode(), null,
            new FileNotFoundException(
                qualifiedPath + ": No such file or directory."));
      }
      return fileStatus;
    } catch (AzureBlobFileSystemException ex) {
      checkException(path, ex);
      return null;
    }
  }

  /**
   * Break the current lease on an ABFS file if it exists. A lease that is broken cannot be
   * renewed. A new lease may be obtained on the file immediately.
   *
   * @param f file name
   * @throws IOException on any exception while breaking the lease
   */
  public void breakLease(final Path f) throws IOException {
    LOG.debug("AzureBlobFileSystem.breakLease path: {}", f);

    Path qualifiedPath = makeQualified(f);

    try (DurationInfo ignored = new DurationInfo(LOG, false, "Break lease for %s",
        qualifiedPath)) {
      TracingContext tracingContext = new TracingContext(clientCorrelationId,
          fileSystemId, FSOperationType.BREAK_LEASE, tracingHeaderFormat,
          listener);
      abfsStore.breakLease(qualifiedPath, tracingContext);
    } catch(AzureBlobFileSystemException ex) {
      checkException(f, ex);
    }
  }

  /**
   * Qualify a path to one which uses this FileSystem and, if relative,
   * made absolute.
   * @param path to qualify.
   * @return this path if it contains a scheme and authority and is absolute, or
   * a new path that includes a path and authority and is fully qualified
   * @see Path#makeQualified(URI, Path)
   * @throws IllegalArgumentException if the path has a schema/URI different
   * from this FileSystem.
   */
  @Override
  public Path makeQualified(Path path) {
    // To support format: abfs://{dfs.nameservices}/file/path,
    // path need to be first converted to URI, then get the raw path string,
    // during which {dfs.nameservices} will be omitted.
    if (path != null) {
      String uriPath = path.toUri().getPath();
      path = uriPath.isEmpty() ? path : new Path(uriPath);
    }
    return super.makeQualified(path);
  }


  @Override
  public Path getWorkingDirectory() {
    return this.workingDir;
  }

  @Override
  public void setWorkingDirectory(final Path newDir) {
    if (newDir.isAbsolute()) {
      this.workingDir = newDir;
    } else {
      this.workingDir = new Path(workingDir, newDir);
    }
  }

  @Override
  public String getScheme() {
    return FileSystemUriSchemes.ABFS_SCHEME;
  }

  @Override
  public Path getHomeDirectory() {
    return makeQualified(new Path(
            FileSystemConfigurations.USER_HOME_DIRECTORY_PREFIX
                + "/" + abfsStore.getUser()));
  }

  /**
   * Return an array containing hostnames, offset and size of
   * portions of the given file. For ABFS we'll just lie and give
   * fake hosts to make sure we get many splits in MR jobs.
   */
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file,
      long start, long len) {
    if (file == null) {
      return null;
    }

    if ((start < 0) || (len < 0)) {
      throw new IllegalArgumentException("Invalid start or len parameter");
    }

    if (file.getLen() < start) {
      return new BlockLocation[0];
    }
    final String blobLocationHost = abfsStore.getAbfsConfiguration().getAzureBlockLocationHost();

    final String[] name = { blobLocationHost };
    final String[] host = { blobLocationHost };
    long blockSize = file.getBlockSize();
    if (blockSize <= 0) {
      throw new IllegalArgumentException(
          "The block size for the given file is not a positive number: "
              + blockSize);
    }
    int numberOfLocations = (int) (len / blockSize)
        + ((len % blockSize == 0) ? 0 : 1);
    BlockLocation[] locations = new BlockLocation[numberOfLocations];
    for (int i = 0; i < locations.length; i++) {
      long currentOffset = start + (i * blockSize);
      long currentLength = Math.min(blockSize, start + len - currentOffset);
      locations[i] = new BlockLocation(name, host, currentOffset, currentLength);
    }

    return locations;
  }

  @Override
  protected void finalize() throws Throwable {
    LOG.debug("finalize() called.");
    close();
    super.finalize();
  }

  /**
   * Get the username of the FS.
   * @return the short name of the user who instantiated the FS
   */
  public String getOwnerUser() {
    return abfsStore.getUser();
  }

  /**
   * Get the group name of the owner of the FS.
   * @return primary group name
   */
  public String getOwnerUserPrimaryGroup() {
    return abfsStore.getPrimaryGroup();
  }

  private boolean deleteRoot() throws IOException {
    LOG.debug("Deleting root content");

    final ExecutorService executorService = Executors.newFixedThreadPool(10);

    try {
      final FileStatus[] ls = listStatus(makeQualified(new Path(File.separator)));
      final ArrayList<Future> deleteTasks = new ArrayList<>();
      for (final FileStatus fs : ls) {
        final Future deleteTask = executorService.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            delete(fs.getPath(), fs.isDirectory());
            if (fs.isDirectory()) {
              statIncrement(DIRECTORIES_DELETED);
            } else {
              statIncrement(FILES_DELETED);
            }
            return null;
          }
        });
        deleteTasks.add(deleteTask);
      }

      for (final Future deleteTask : deleteTasks) {
        execute("deleteRoot", new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            deleteTask.get();
            return null;
          }
        });
      }
    }
    finally {
      executorService.shutdownNow();
    }

    return true;
  }

   /**
   * Set owner of a path (i.e. a file or a directory).
   * The parameters owner and group cannot both be null.
   *
   * @param path  The path
   * @param owner If it is null, the original username remains unchanged.
   * @param group If it is null, the original groupname remains unchanged.
   */
  @Override
  public void setOwner(final Path path, final String owner, final String group)
      throws IOException {
    LOG.debug(
        "AzureBlobFileSystem.setOwner path: {}", path);
    TracingContext tracingContext = new TracingContext(clientCorrelationId,
        fileSystemId, FSOperationType.SET_OWNER, true, tracingHeaderFormat,
        listener);

    if (!getIsNamespaceEnabled(tracingContext)) {
      super.setOwner(path, owner, group);
      return;
    }

    if ((owner == null || owner.isEmpty()) && (group == null || group.isEmpty())) {
      throw new IllegalArgumentException("A valid owner or group must be specified.");
    }

    Path qualifiedPath = makeQualified(path);

    try {
      abfsStore.setOwner(qualifiedPath,
              owner,
              group,
              tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      checkException(path, ex);
    }
  }

  /**
   * Set the value of an attribute for a path.
   *
   * @param path The path on which to set the attribute
   * @param name The attribute to set
   * @param value The byte value of the attribute to set (encoded in latin-1)
   * @param flag The mode in which to set the attribute
   * @throws IOException If there was an issue setting the attribute on Azure
   * @throws IllegalArgumentException If name is null or empty or if value is null
   */
  @Override
  public void setXAttr(final Path path, final String name, final byte[] value, final EnumSet<XAttrSetFlag> flag)
      throws IOException {
    LOG.debug("AzureBlobFileSystem.setXAttr path: {}", path);

    if (name == null || name.isEmpty() || value == null) {
      throw new IllegalArgumentException("A valid name and value must be specified.");
    }

    Path qualifiedPath = makeQualified(path);

    try {
      TracingContext tracingContext = new TracingContext(clientCorrelationId,
          fileSystemId, FSOperationType.SET_ATTR, true, tracingHeaderFormat,
          listener);
      Hashtable<String, String> properties;
      String xAttrName = ensureValidAttributeName(name);
      String xAttrValue;

      if (abfsStore.getPrefixMode() == PrefixMode.BLOB) {
        properties = abfsStore.getBlobMetadata(qualifiedPath, tracingContext);

        boolean xAttrExists = properties.containsKey(xAttrName);
        XAttrSetFlag.validate(name, xAttrExists, flag);

        // On Blob Endpoint metadata are passed as HTTP Request Headers
        // Values in UTF_8 needed to be URL encoded after decoding into String
        xAttrValue = encodeMetadataAttribute(new String(value, StandardCharsets.UTF_8));
        properties.put(xAttrName, xAttrValue);
        abfsStore.setBlobMetadata(qualifiedPath, properties, tracingContext);

        return;
      }

      properties = abfsStore.getPathStatus(qualifiedPath, tracingContext);
      boolean xAttrExists = properties.containsKey(xAttrName);
      XAttrSetFlag.validate(name, xAttrExists, flag);

      xAttrValue = abfsStore.decodeAttribute(value);
      properties.put(xAttrName, xAttrValue);
      abfsStore.setPathProperties(qualifiedPath, properties, tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      checkException(path, ex);
    }
  }

  /**
   * Get the value of an attribute for a path.
   *
   * @param path The path on which to get the attribute
   * @param name The attribute to get
   * @return The bytes of the attribute's value (encoded in latin-1)
   *         or null if the attribute does not exist
   * @throws IOException If there was an issue getting the attribute from Azure
   * @throws IllegalArgumentException If name is null or empty
   */
  @Override
  public byte[] getXAttr(final Path path, final String name)
      throws IOException {
    LOG.debug("AzureBlobFileSystem.getXAttr path: {}", path);

    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("A valid name must be specified.");
    }

    Path qualifiedPath = makeQualified(path);

    byte[] value = null;
    try {
      TracingContext tracingContext = new TracingContext(clientCorrelationId,
          fileSystemId, FSOperationType.GET_ATTR, true, tracingHeaderFormat,
          listener);
      Hashtable<String, String> properties;
      String xAttrName = ensureValidAttributeName(name);

      if (abfsStore.getPrefixMode() == PrefixMode.BLOB) {
        properties = abfsStore.getBlobMetadata(qualifiedPath, tracingContext);
        if (properties.containsKey(xAttrName)) {
          String xAttrValue = properties.get(xAttrName);
          value = decodeMetadataAttribute(xAttrValue).getBytes(
              StandardCharsets.UTF_8);
        }
        return value;
      }

      properties = abfsStore.getPathStatus(qualifiedPath, tracingContext);

      if (properties.containsKey(xAttrName)) {
        String xAttrValue = properties.get(xAttrName);
        value = abfsStore.encodeAttribute(xAttrValue);
      }
    } catch (AzureBlobFileSystemException ex) {
      checkException(path, ex);
    }
    return value;
  }

  private static String ensureValidAttributeName(String attribute) {
    // to avoid HTTP 400 Bad Request, InvalidPropertyName
    return attribute.replace('.', '_');
  }

  /**
   * Set permission of a path.
   *
   * @param path       The path
   * @param permission Access permission
   */
  @Override
  public void setPermission(final Path path, final FsPermission permission)
      throws IOException {
    LOG.debug("AzureBlobFileSystem.setPermission path: {}", path);
    TracingContext tracingContext = new TracingContext(clientCorrelationId,
        fileSystemId, FSOperationType.SET_PERMISSION, true, tracingHeaderFormat, listener);

    if (!getIsNamespaceEnabled(tracingContext)) {
      super.setPermission(path, permission);
      return;
    }

    if (permission == null) {
      throw new IllegalArgumentException("The permission can't be null");
    }

    Path qualifiedPath = makeQualified(path);

    try {
      abfsStore.setPermission(qualifiedPath, permission, tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      checkException(path, ex);
    }
  }

  /**
   * Modifies ACL entries of files and directories.  This method can add new ACL
   * entries or modify the permissions on existing ACL entries.  All existing
   * ACL entries that are not specified in this call are retained without
   * changes.  (Modifications are merged into the current ACL.)
   *
   * @param path    Path to modify
   * @param aclSpec List of AbfsAclEntry describing modifications
   * @throws IOException if an ACL could not be modified
   */
  @Override
  public void modifyAclEntries(final Path path, final List<AclEntry> aclSpec)
      throws IOException {
    LOG.debug("AzureBlobFileSystem.modifyAclEntries path: {}", path);
    TracingContext tracingContext = new TracingContext(clientCorrelationId,
        fileSystemId, FSOperationType.MODIFY_ACL, true, tracingHeaderFormat,
        listener);

    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "modifyAclEntries is only supported by storage accounts with the "
          + "hierarchical namespace enabled.");
    }

    if (aclSpec == null || aclSpec.isEmpty()) {
      throw new IllegalArgumentException("The value of the aclSpec parameter is invalid.");
    }

    Path qualifiedPath = makeQualified(path);

    try {
      abfsStore.modifyAclEntries(qualifiedPath, aclSpec, tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      checkException(path, ex);
    }
  }

  /**
   * Removes ACL entries from files and directories.  Other ACL entries are
   * retained.
   *
   * @param path    Path to modify
   * @param aclSpec List of AclEntry describing entries to remove
   * @throws IOException if an ACL could not be modified
   */
  @Override
  public void removeAclEntries(final Path path, final List<AclEntry> aclSpec)
      throws IOException {
    LOG.debug("AzureBlobFileSystem.removeAclEntries path: {}", path);
    TracingContext tracingContext = new TracingContext(clientCorrelationId,
        fileSystemId, FSOperationType.REMOVE_ACL_ENTRIES, true,
        tracingHeaderFormat, listener);

    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "removeAclEntries is only supported by storage accounts with the "
          + "hierarchical namespace enabled.");
    }

    if (aclSpec == null || aclSpec.isEmpty()) {
      throw new IllegalArgumentException("The aclSpec argument is invalid.");
    }

    Path qualifiedPath = makeQualified(path);

    try {
      abfsStore.removeAclEntries(qualifiedPath, aclSpec, tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      checkException(path, ex);
    }
  }

  /**
   * Removes all default ACL entries from files and directories.
   *
   * @param path Path to modify
   * @throws IOException if an ACL could not be modified
   */
  @Override
  public void removeDefaultAcl(final Path path) throws IOException {
    LOG.debug("AzureBlobFileSystem.removeDefaultAcl path: {}", path);
    TracingContext tracingContext = new TracingContext(clientCorrelationId,
        fileSystemId, FSOperationType.REMOVE_DEFAULT_ACL, true,
        tracingHeaderFormat, listener);

    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "removeDefaultAcl is only supported by storage accounts with the "
          + "hierarchical namespace enabled.");
    }

    Path qualifiedPath = makeQualified(path);

    try {
      abfsStore.removeDefaultAcl(qualifiedPath, tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      checkException(path, ex);
    }
  }

  /**
   * Removes all but the base ACL entries of files and directories.  The entries
   * for user, group, and others are retained for compatibility with permission
   * bits.
   *
   * @param path Path to modify
   * @throws IOException if an ACL could not be removed
   */
  @Override
  public void removeAcl(final Path path) throws IOException {
    LOG.debug("AzureBlobFileSystem.removeAcl path: {}", path);
    TracingContext tracingContext = new TracingContext(clientCorrelationId,
        fileSystemId, FSOperationType.REMOVE_ACL, true, tracingHeaderFormat,
        listener);

    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "removeAcl is only supported by storage accounts with the "
          + "hierarchical namespace enabled.");
    }

    Path qualifiedPath = makeQualified(path);

    try {
      abfsStore.removeAcl(qualifiedPath, tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      checkException(path, ex);
    }
  }

  /**
   * Fully replaces ACL of files and directories, discarding all existing
   * entries.
   *
   * @param path    Path to modify
   * @param aclSpec List of AclEntry describing modifications, must include
   *                entries for user, group, and others for compatibility with
   *                permission bits.
   * @throws IOException if an ACL could not be modified
   */
  @Override
  public void setAcl(final Path path, final List<AclEntry> aclSpec)
      throws IOException {
    LOG.debug("AzureBlobFileSystem.setAcl path: {}", path);
    TracingContext tracingContext = new TracingContext(clientCorrelationId,
        fileSystemId, FSOperationType.SET_ACL, true, tracingHeaderFormat,
        listener);

    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "setAcl is only supported by storage accounts with the hierarchical "
          + "namespace enabled.");
    }

    if (aclSpec == null || aclSpec.size() == 0) {
      throw new IllegalArgumentException("The aclSpec argument is invalid.");
    }

    Path qualifiedPath = makeQualified(path);

    try {
      abfsStore.setAcl(qualifiedPath, aclSpec, tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      checkException(path, ex);
    }
  }

  /**
   * Gets the ACL of a file or directory.
   *
   * @param path Path to get
   * @return AbfsAclStatus describing the ACL of the file or directory
   * @throws IOException if an ACL could not be read
   */
  @Override
  public AclStatus getAclStatus(final Path path) throws IOException {
    LOG.debug("AzureBlobFileSystem.getAclStatus path: {}", path);
    TracingContext tracingContext = new TracingContext(clientCorrelationId,
        fileSystemId, FSOperationType.GET_ACL_STATUS, true, tracingHeaderFormat, listener);

    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "getAclStatus is only supported by storage account with the "
          + "hierarchical namespace enabled.");
    }

    Path qualifiedPath = makeQualified(path);

    try {
      return abfsStore.getAclStatus(qualifiedPath, tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      checkException(path, ex);
      return null;
    }
  }

  /**
   * Checks if the user can access a path.  The mode specifies which access
   * checks to perform.  If the requested permissions are granted, then the
   * method returns normally.  If access is denied, then the method throws an
   * {@link AccessControlException}.
   *
   * @param path Path to check
   * @param mode type of access to check
   * @throws AccessControlException        if access is denied
   * @throws java.io.FileNotFoundException if the path does not exist
   * @throws IOException                   see specific implementation
   */
  @Override
  public void access(final Path path, final FsAction mode) throws IOException {
    LOG.debug("AzureBlobFileSystem.access path : {}, mode : {}", path, mode);
    Path qualifiedPath = makeQualified(path);
    try {
      TracingContext tracingContext = new TracingContext(clientCorrelationId,
          fileSystemId, FSOperationType.ACCESS, tracingHeaderFormat,
          listener);
      this.abfsStore.access(qualifiedPath, mode, tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      checkCheckAccessException(path, ex);
    }
  }

  /**
   * Incrementing exists() calls from superclass for statistic collection.
   *
   * @param f source path.
   * @return true if the path exists.
   * @throws IOException
   */
  @Override
  public boolean exists(Path f) throws IOException {
    statIncrement(CALL_EXIST);
    return super.exists(f);
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(Path path)
      throws IOException {
    LOG.debug("AzureBlobFileSystem.listStatusIterator path : {}", path);
    if (abfsStore.getAbfsConfiguration().enableAbfsListIterator()) {
      TracingContext tracingContext = new TracingContext(clientCorrelationId,
          fileSystemId, FSOperationType.LISTSTATUS, true, tracingHeaderFormat, listener);
      AbfsListStatusRemoteIterator abfsLsItr =
          new AbfsListStatusRemoteIterator(getFileStatus(path, tracingContext), abfsStore,
              tracingContext);
      return RemoteIterators.typeCastingRemoteIterator(abfsLsItr);
    } else {
      return super.listStatusIterator(path);
    }
  }

  /**
   * Incremental listing of located status entries,
   * preserving etags.
   * @param path path to list
   * @param filter a path filter
   * @return iterator of results.
   * @throws FileNotFoundException source path not found.
   * @throws IOException other values.
   */
  @Override
  protected RemoteIterator<LocatedFileStatus> listLocatedStatus(
      final Path path,
      final PathFilter filter)
      throws FileNotFoundException, IOException {

    LOG.debug("AzureBlobFileSystem.listStatusIterator path : {}", path);
    // get a paged iterator over the source data, filtering out non-matching
    // entries.
    final RemoteIterator<FileStatus> sourceEntries = filteringRemoteIterator(
        listStatusIterator(path),
        (st) -> filter.accept(st.getPath()));
    // and then map that to a remote iterator of located file status
    // entries, propagating any etags.
    return mappingRemoteIterator(sourceEntries,
        st -> new AbfsLocatedFileStatus(st,
            st.isFile()
                ? getFileBlockLocations(st, 0, st.getLen())
                : null));
  }

  private FileStatus tryGetFileStatus(final Path f, TracingContext tracingContext) {
    try {
      return getFileStatus(f, tracingContext);
    } catch (IOException ex) {
      LOG.debug("File not found {}", f);
      statIncrement(ERROR_IGNORED);
      return null;
    }
  }

  private boolean fileSystemExists() throws IOException {
    LOG.debug(
            "AzureBlobFileSystem.fileSystemExists uri: {}", uri);
    try {
      TracingContext tracingContext = new TracingContext(clientCorrelationId,
          fileSystemId, FSOperationType.TEST_OP, tracingHeaderFormat, listener);
      abfsStore.getFilesystemProperties(tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      try {
        checkException(null, ex);
        // Because HEAD request won't contain message body,
        // there is not way to get the storage error code
        // workaround here is to check its status code.
      } catch (FileNotFoundException e) {
        statIncrement(ERROR_IGNORED);
        return false;
      }
    }
    return true;
  }

  private void createFileSystem(TracingContext tracingContext) throws IOException {
    LOG.debug(
        "AzureBlobFileSystem.createFileSystem uri: {}", uri);
    try {
      abfsStore.createFilesystem(tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      checkException(null, ex);
    }
  }

  private URI ensureAuthority(URI uri, final Configuration conf) {

    Preconditions.checkNotNull(uri, "uri");

    if (uri.getAuthority() == null) {
      final URI defaultUri = FileSystem.getDefaultUri(conf);

      if (defaultUri != null && isAbfsScheme(defaultUri.getScheme())) {
        try {
          // Reconstruct the URI with the authority from the default URI.
          uri = new URI(
              uri.getScheme(),
              defaultUri.getAuthority(),
              uri.getPath(),
              uri.getQuery(),
              uri.getFragment());
        } catch (URISyntaxException e) {
          // This should never happen.
          throw new IllegalArgumentException(new InvalidUriException(uri.toString()));
        }
      }
    }

    if (uri.getAuthority() == null) {
      throw new IllegalArgumentException(new InvalidUriAuthorityException(uri.toString()));
    }

    return uri;
  }

  private boolean isAbfsScheme(final String scheme) {
    if (scheme == null) {
      return false;
    }

    if (scheme.equals(FileSystemUriSchemes.ABFS_SCHEME)
        || scheme.equals(FileSystemUriSchemes.ABFS_SECURE_SCHEME)) {
      return true;
    }

    return false;
  }

  @VisibleForTesting
  <T> FileSystemOperation<T> execute(
      final String scopeDescription,
      final Callable<T> callableFileOperation) throws IOException {
    return execute(scopeDescription, callableFileOperation, null);
  }

  @VisibleForTesting
  <T> FileSystemOperation<T> execute(
      final String scopeDescription,
      final Callable<T> callableFileOperation,
      T defaultResultValue) throws IOException {

    try {
      final T executionResult = callableFileOperation.call();
      return new FileSystemOperation<>(executionResult, null);
    } catch (AbfsRestOperationException abfsRestOperationException) {
      return new FileSystemOperation<>(defaultResultValue, abfsRestOperationException);
    } catch (AzureBlobFileSystemException azureBlobFileSystemException) {
      throw new IOException(azureBlobFileSystemException);
    } catch (Exception exception) {
      if (exception instanceof ExecutionException) {
        exception = (Exception) getRootCause(exception);
      }
      final FileSystemOperationUnhandledException fileSystemOperationUnhandledException
          = new FileSystemOperationUnhandledException(exception);
      throw new IOException(fileSystemOperationUnhandledException);
    }
  }

  private void checkCheckAccessException(final Path path,
      final AzureBlobFileSystemException exception) throws IOException {
    if (exception instanceof AbfsRestOperationException) {
      AbfsRestOperationException ere = (AbfsRestOperationException) exception;
      if (ere.getStatusCode() == HttpURLConnection.HTTP_FORBIDDEN) {
        throw (IOException) new AccessControlException(ere.getMessage())
            .initCause(exception);
      }
    }
    checkException(path, exception);
  }

  /**
   * Given a path and exception, choose which IOException subclass
   * to create.
   * Will return if and only iff the error code is in the list of allowed
   * error codes.
   * @param path path of operation triggering exception; may be null
   * @param exception the exception caught
   * @param allowedErrorCodesList varargs list of error codes.
   * @throws IOException if the exception error code is not on the allowed list.
   */
  @VisibleForTesting
  static void checkException(final Path path,
                              final AzureBlobFileSystemException exception,
                              final AzureServiceErrorCode... allowedErrorCodesList) throws IOException {
    if (exception instanceof AbfsRestOperationException) {
      AbfsRestOperationException ere = (AbfsRestOperationException) exception;

      if (ArrayUtils.contains(allowedErrorCodesList, ere.getErrorCode())) {
        return;
      }
      //AbfsRestOperationException.getMessage() contains full error info including path/uri.
      String message = ere.getMessage();

      switch (ere.getStatusCode()) {
      case HttpURLConnection.HTTP_NOT_FOUND:
        throw (IOException) new FileNotFoundException(message)
            .initCause(exception);
      case HttpURLConnection.HTTP_CONFLICT:
        throw (IOException) new FileAlreadyExistsException(message)
            .initCause(exception);
      case HttpURLConnection.HTTP_FORBIDDEN:
      case HttpURLConnection.HTTP_UNAUTHORIZED:
        throw (IOException) new AccessDeniedException(message)
            .initCause(exception);
      default:
        throw ere;
      }
    } else if (exception instanceof SASTokenProviderException) {
      throw exception;
    } else {
      if (path == null) {
        throw exception;
      }
      // record info of path
      throw new PathIOException(path.toString(), exception);
    }
  }

  /**
   * Gets the root cause of a provided {@link Throwable}.  If there is no cause for the
   * {@link Throwable} provided into this function, the original {@link Throwable} is returned.
   *
   * @param throwable starting {@link Throwable}
   * @return root cause {@link Throwable}
   */
  private Throwable getRootCause(Throwable throwable) {
    if (throwable == null) {
      throw new IllegalArgumentException("throwable can not be null");
    }

    Throwable result = throwable;
    while (result.getCause() != null) {
      result = result.getCause();
    }

    return result;
  }

  private boolean containsColon(Path p) {
    return p.toUri().getPath().contains(":");
  }

  /**
   * Get a delegation token from remote service endpoint if
   * 'fs.azure.enable.kerberos.support' is set to 'true', and
   * 'fs.azure.enable.delegation.token' is set to 'true'.
   * @param renewer the account name that is allowed to renew the token.
   * @return delegation token
   * @throws IOException thrown when getting the current user.
   */
  @Override
  public synchronized Token<?> getDelegationToken(final String renewer) throws IOException {
    statIncrement(CALL_GET_DELEGATION_TOKEN);
    return this.delegationTokenEnabled ? this.delegationTokenManager.getDelegationToken(renewer)
        : super.getDelegationToken(renewer);
  }

  /**
   * If Delegation tokens are enabled, the canonical service name of
   * this filesystem is the filesystem URI.
   * @return either the filesystem URI as a string, or null.
   */
  @Override
  public String getCanonicalServiceName() {
    String name = null;
    if (delegationTokenManager != null) {
      name = delegationTokenManager.getCanonicalServiceName();
    }
    return name != null ? name : super.getCanonicalServiceName();
  }

  @VisibleForTesting
  FileSystem.Statistics getFsStatistics() {
    return this.statistics;
  }

  @VisibleForTesting
  void setListenerOperation(FSOperationType operation) {
    listener.setOperation(operation);
  }

  @VisibleForTesting
  static class FileSystemOperation<T> {
    private final T result;
    private final AbfsRestOperationException exception;

    FileSystemOperation(final T result, final AbfsRestOperationException exception) {
      this.result = result;
      this.exception = exception;
    }

    public boolean failed() {
      return this.exception != null;
    }
  }

  @VisibleForTesting
  AzureBlobFileSystemStore getAbfsStore() {
    return abfsStore;
  }

  @VisibleForTesting
  AbfsClient getAbfsClient() {
    return abfsStore.getClient();
  }

  /**
   * Get any Delegation Token manager created by the filesystem.
   * @return the DT manager or null.
   */
  @VisibleForTesting
  AbfsDelegationTokenManager getDelegationTokenManager() {
    return delegationTokenManager;
  }

  @VisibleForTesting
  boolean getIsNamespaceEnabled(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    return abfsStore.getIsNamespaceEnabled(tracingContext);
  }

  @VisibleForTesting
  NativeAzureFileSystem getNativeFs() {
    return nativeFs;
  }

  /**
   * Returns the counter() map in IOStatistics containing all the counters
   * and their values.
   *
   * @return Map of IOStatistics counters.
   */
  @VisibleForTesting
  Map<String, Long> getInstrumentationMap() {
    return abfsCounters.toMap();
  }

  @VisibleForTesting
  String getFileSystemId() {
    return fileSystemId;
  }

  @VisibleForTesting
  String getClientCorrelationId() {
    return clientCorrelationId;
  }

  @Override
  public boolean hasPathCapability(final Path path, final String capability)
      throws IOException {
    // qualify the path to make sure that it refers to the current FS.
    final Path p = makeQualified(path);
    switch (validatePathCapabilityArgs(p, capability)) {
    case CommonPathCapabilities.FS_PERMISSIONS:
    case CommonPathCapabilities.FS_APPEND:
    case CommonPathCapabilities.ETAGS_AVAILABLE:
      return true;
    case CommonPathCapabilities.ETAGS_PRESERVED_IN_RENAME:
    case CommonPathCapabilities.FS_ACLS:
      return getIsNamespaceEnabled(
          new TracingContext(clientCorrelationId, fileSystemId,
              FSOperationType.HAS_PATH_CAPABILITY, tracingHeaderFormat,
              listener));

      // probe for presence of the HADOOP-18546 readahead fix.
    case CAPABILITY_SAFE_READAHEAD:
      return true;

    default:
      return super.hasPathCapability(p, capability);
    }
  }

  /**
   * Getter for IOStatistic instance in AzureBlobFilesystem.
   *
   * @return the IOStatistic instance from abfsCounters.
   */
  @Override
  public IOStatistics getIOStatistics() {
    return abfsCounters != null ? abfsCounters.getIOStatistics() : null;
  }

}
