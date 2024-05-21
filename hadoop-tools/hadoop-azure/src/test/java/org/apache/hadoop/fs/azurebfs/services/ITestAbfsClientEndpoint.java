package org.apache.hadoop.fs.azurebfs.services;

import java.net.URI;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.utils.Base64;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_APPEND_BLOB_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_CONTRACT_TEST_URI;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_APPENDBLOB_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.TEST_CONFIGURATION_FILE_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.TEST_CONTAINER_PREFIX;

public class ITestAbfsClientEndpoint {

  @Test
  public void testAbfsDFSClient() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem(AbfsServiceType.DFS)) {
      AbfsClient client = fs.getAbfsStore().getClient();
      Assertions.assertThat(client).isInstanceOf(AbfsDfsClient.class);
      // Make sure all client.REST_API_CALLS succeed with right parameters
      testClientAPIs(client, getTestTracingContext(fs));
    }
  }

  @Test
  public void testAbfsBlobClient() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem(AbfsServiceType.BLOB)) {
      AbfsClient client = fs.getAbfsStore().getClient();
      Assertions.assertThat(client).isInstanceOf(AbfsBlobClient.class);
      // Make sure all client.REST_API_CALLS succeed with right parameters
      testClientAPIs(client, getTestTracingContext(fs));
    }
  }

  private void testClientAPIs(AbfsClient client, TracingContext tracingContext) throws Exception {
    // 1. Set File System Properties
    String val1 = Base64.encode("value1".getBytes());
    String val2 = Base64.encode("value2".getBytes());
    String properties = "key1=" + val1 + ",key2=" + val2;
    client.setFilesystemProperties(properties, tracingContext);

    // 2. Get File System Properties
    client.getFilesystemProperties(tracingContext);

    // 3. List Path
    client.listPath("/", false, 5, null, tracingContext);

    // N. Delete File System
    client.deleteFilesystem(tracingContext);
  }

  private AzureBlobFileSystem getFileSystem(AbfsServiceType serviceType) throws Exception {
    Configuration rawConfig = new Configuration();
    rawConfig.addResource(TEST_CONFIGURATION_FILE_NAME);

    String fileSystemName = TEST_CONTAINER_PREFIX + UUID.randomUUID().toString();
    String accountName = rawConfig.get(FS_AZURE_ACCOUNT_NAME, "");
    if (accountName.isEmpty()) {
      // check if accountName is set using different config key
      accountName = rawConfig.get(FS_AZURE_ABFS_ACCOUNT_NAME, "");
    }
    Assume.assumeFalse("Skipping test as account name is not provided", accountName.isEmpty());

    switch (serviceType) {
      case DFS:
        accountName = setDFSEndpoint(accountName);
        break;
      case BLOB:
        Assume.assumeFalse("Blob Endpoint Works only with FNS Accounts",
            rawConfig.getBoolean(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, true));
        accountName = setBlobEndpoint(accountName);
        break;
      default:
        throw new IllegalArgumentException("Invalid service type: " + serviceType);
    }

    AbfsConfiguration abfsConfig = new AbfsConfiguration(rawConfig, accountName);
    AuthType authType = abfsConfig.getEnum(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.SharedKey);
    String abfsScheme = authType == AuthType.SharedKey ? FileSystemUriSchemes.ABFS_SCHEME
        : FileSystemUriSchemes.ABFS_SECURE_SCHEME;
    final String abfsUrl = fileSystemName + "@" + accountName;
    URI defaultUri = null;

    try {
      defaultUri = new URI(abfsScheme, abfsUrl, null, null, null);
    } catch (Exception ex) {
      throw new AssertionError(ex);
    }

    String testUrl = defaultUri.toString();
    abfsConfig.set(FS_DEFAULT_NAME_KEY, defaultUri.toString());
    abfsConfig.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, true);
    if (rawConfig.getBoolean(FS_AZURE_TEST_APPENDBLOB_ENABLED, false)) {
      String appendblobDirs = testUrl + "," + abfsConfig.get(FS_AZURE_CONTRACT_TEST_URI);
      rawConfig.set(FS_AZURE_APPEND_BLOB_KEY, appendblobDirs);
    }

    return (AzureBlobFileSystem) FileSystem.newInstance(rawConfig);
  }

  private String setDFSEndpoint(String accountName) {
    return accountName.replace(".blob.", ".dfs.");
  }

  private String setBlobEndpoint(String accountName) {
    return accountName.replace(".dfs.", ".blob.");
  }

  public TracingContext getTestTracingContext(AzureBlobFileSystem fs) {
    String correlationId = "test-corr-id", fsId = "test-filesystem-id";
    TracingHeaderFormat format = TracingHeaderFormat.ALL_ID_FORMAT;;
    return new TracingContext(correlationId, fsId, FSOperationType.TEST_OP, false, format, null);
  }
}
