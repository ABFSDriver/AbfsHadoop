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

import java.io.IOException;
import java.util.HashMap;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_BLOB_ENDPOINT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_MKDIRS_FALLBACK_TO_DFS;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_REDIRECT_DELETE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_DNS_PREFIX;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.WASB_DNS_PREFIX;

public class ITestAzureBlobFileSystemBlobConfig
    extends AbstractAbfsIntegrationTest {

  public ITestAzureBlobFileSystemBlobConfig() throws Exception {
    super();
  }

  @Test
  public void testDfsEndpointWhenBlobEndpointConfigIsDisabled()
      throws Exception {
    AzureBlobFileSystem fs = createFileSystemForEndpointConfigPair(
        FS_AZURE_ENABLE_BLOB_ENDPOINT, false, true);
    AbfsClient client = Mockito.spy(fs.getAbfsClient());
    fs.getAbfsStore().setClient(client);
    fs.create(new Path("/tmp"));
    veriCreatePathExecution(client);
  }

  private void veriCreatePathExecution(final AbfsClient client)
      throws AzureBlobFileSystemException {
    Mockito.verify(client, Mockito.times(1))
        .createPath(Mockito.anyString(), Mockito.anyBoolean(),
            Mockito.anyBoolean(), Mockito.nullable(String.class),
            Mockito.nullable(String.class),
            Mockito.anyBoolean(), Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));
  }

  @Test
  public void testDfsEndpointWhenBlobEndpointConfigIsEnabled()
      throws Exception {
    AzureBlobFileSystem fs = createFileSystemForEndpointConfigPair(
        FS_AZURE_ENABLE_BLOB_ENDPOINT, true, true);
    AbfsClient client = Mockito.spy(fs.getAbfsClient());
    fs.getAbfsStore().setClient(client);
    fs.create(new Path("/tmp"));
    verifyCreatePathBlobExecution(client);
  }

  @Test
  public void testBlobEndpointWhenBlobEndpointConfigIsEnabled()
      throws Exception {
    AzureBlobFileSystem fs = createFileSystemForEndpointConfigPair(
        FS_AZURE_ENABLE_BLOB_ENDPOINT, true, false);
    AbfsClient client = Mockito.spy(fs.getAbfsClient());
    fs.getAbfsStore().setClient(client);
    fs.create(new Path("/tmp"));
    verifyCreatePathBlobExecution(client);
  }

  @Test
  public void testBlobEndpointWhenBlobEndpointConfigIsDisabled()
      throws Exception {
    AzureBlobFileSystem fs = createFileSystemForEndpointConfigPair(
        FS_AZURE_ENABLE_BLOB_ENDPOINT, false, false);
    AbfsClient client = Mockito.spy(fs.getAbfsClient());
    fs.getAbfsStore().setClient(client);
    fs.create(new Path("/tmp"));
    verifyCreatePathBlobExecution(client);
  }

  @Test
  public void testBlobEndpointWhenBlobEndpointConfigIsNull()
      throws Exception {
    AzureBlobFileSystem fs = createFileSystemForEndpointConfigPair(
        FS_AZURE_ENABLE_BLOB_ENDPOINT, null, false);
    AbfsClient client = Mockito.spy(fs.getAbfsClient());
    fs.getAbfsStore().setClient(client);
    fs.create(new Path("/tmp"));
    verifyCreatePathBlobExecution(client);
  }

  @Test
  public void testBlobEndpointWithMkdirsOnDFS() throws Exception {
    AzureBlobFileSystem fs = createFileSystemForEndpointConfigPair(
        FS_AZURE_MKDIRS_FALLBACK_TO_DFS, true, false);
    AbfsClient client = Mockito.spy(fs.getAbfsClient());
    fs.getAbfsStore().setClient(client);
    int[] dirCreatedOverDFSExecCount = new int[1];
    dirCreatedOverDFSExecCount[0] = 0;
    int[] fileCreatedOverDFSExecCount = new int[1];
    fileCreatedOverDFSExecCount[0] = 0;
    checkDirAndFileCreationOnDFS(client, dirCreatedOverDFSExecCount,
        fileCreatedOverDFSExecCount);
    fs.mkdirs(new Path("/tmp"));
    fs.create(new Path("/file"));
    verifyCreatePathBlobExecution(client);
    Assert.assertTrue(dirCreatedOverDFSExecCount[0] == 1);
  }

  @Test
  public void testBlobEndpointWithMkdirsOnDfsNoOverride() throws Exception {
    AzureBlobFileSystem fs = createFileSystemForEndpointConfigPair(
        FS_AZURE_MKDIRS_FALLBACK_TO_DFS, false, false);
    AbfsClient client = Mockito.spy(fs.getAbfsClient());
    fs.getAbfsStore().setClient(client);
    int[] dirCreatedOverBlobExecCount = new int[1];
    dirCreatedOverBlobExecCount[0] = 0;
    int[] fileCreatedOverBlobExecCount = new int[1];
    fileCreatedOverBlobExecCount[0] = 0;

    checkDirAndFileCreationOnBlob(client, dirCreatedOverBlobExecCount,
        fileCreatedOverBlobExecCount);

    fs.mkdirs(new Path("/tmp"));
    fs.create(new Path("/file"));
    Assert.assertTrue(dirCreatedOverBlobExecCount[0] == 1);
    Assert.assertTrue(fileCreatedOverBlobExecCount[0] == 1);
  }

  private void checkDirAndFileCreationOnBlob(final AbfsClient client,
      final int[] dirCreatedOverBlobExecCount,
      final int[] fileCreatedOverBlobExecCount)
      throws AzureBlobFileSystemException {
    Mockito.doAnswer(answer -> {
          if (!(Boolean) answer.getArgument(1)) {
            dirCreatedOverBlobExecCount[0]++;
          } else {
            fileCreatedOverBlobExecCount[0]++;
          }
          return answer.callRealMethod();
        }).when(client)
        .createPathBlob(Mockito.anyString(), Mockito.anyBoolean(),
            Mockito.anyBoolean(), Mockito.nullable(
                HashMap.class), Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));
  }

  @Test
  public void testDFSEndpointWithMkdirsOnDFS() throws Exception {
    AzureBlobFileSystem fs = createFileSystemForEndpointConfigPair(
        FS_AZURE_MKDIRS_FALLBACK_TO_DFS, true, true);
    AbfsClient client = Mockito.spy(fs.getAbfsClient());
    fs.getAbfsStore().setClient(client);
    int[] dirCreatedOverDFSExecCount = new int[1];
    dirCreatedOverDFSExecCount[0] = 0;
    int[] fileCreatedOverDFSExecCount = new int[1];
    fileCreatedOverDFSExecCount[0] = 0;
    checkDirAndFileCreationOnDFS(client, dirCreatedOverDFSExecCount,
        fileCreatedOverDFSExecCount);
    fs.mkdirs(new Path("/tmp"));
    fs.create(new Path("/file"));
    Assert.assertTrue(dirCreatedOverDFSExecCount[0] == 1);
    Assert.assertTrue(fileCreatedOverDFSExecCount[0] == 1);
  }

  @Test
  public void testDFSEndpointWithMkdirsOnDFSNoOverride() throws Exception {
    AzureBlobFileSystem fs = createFileSystemForEndpointConfigPair(
        FS_AZURE_MKDIRS_FALLBACK_TO_DFS, false, true);
    AbfsClient client = Mockito.spy(fs.getAbfsClient());
    fs.getAbfsStore().setClient(client);
    int[] dirCreatedOverDFSExecCount = new int[1];
    dirCreatedOverDFSExecCount[0] = 0;
    int[] fileCreatedOverDFSExecCount = new int[1];
    fileCreatedOverDFSExecCount[0] = 0;
    checkDirAndFileCreationOnDFS(client, dirCreatedOverDFSExecCount, fileCreatedOverDFSExecCount);
    fs.mkdirs(new Path("/tmp"));
    fs.create(new Path("/file"));
    Assert.assertTrue(dirCreatedOverDFSExecCount[0] == 1);
    Assert.assertTrue(fileCreatedOverDFSExecCount[0] == 1);
  }

  @Test
  public void testBlobEndpointNoDeleteWasbRedirect() throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(createFileSystemForEndpointConfigPair(
        FS_AZURE_REDIRECT_DELETE, false, false));
    Assert.assertNull(fs.getNativeFs());
    NativeAzureFileSystem nativeAzureFileSystem = null;
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    AbfsClient client = Mockito.spy(fs.getAbfsClient());
    assignStoreMocksToFs(fs, nativeAzureFileSystem, store, client);

    Boolean[] isDeleteOverNativeFS = new Boolean[1];
    isDeleteOverNativeFS[0] = false;
    Boolean[] isDeleteOverDFSEndpoint = new Boolean[1];
    isDeleteOverDFSEndpoint[0] = false;

    countDeleteOverAbfsAndWasb(nativeAzureFileSystem, client, isDeleteOverNativeFS,
        isDeleteOverDFSEndpoint);

    fs.create(new Path("/file"));
    fs.delete(new Path("/file"), true);

    Assert.assertTrue(isDeleteOverDFSEndpoint[0]);
    Assert.assertFalse(isDeleteOverNativeFS[0]);
  }

  @Test
  public void testBlobEndpointWithDeleteWasbRedirect() throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(createFileSystemForEndpointConfigPair(
        FS_AZURE_REDIRECT_DELETE, true, false));
    Assert.assertNotNull(fs.getNativeFs());
    NativeAzureFileSystem nativeAzureFileSystem = Mockito.spy(fs.getNativeFs());
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    AbfsClient client = Mockito.spy(fs.getAbfsClient());
    assignStoreMocksToFs(fs, nativeAzureFileSystem, store, client);

    Boolean[] isDeleteOverNativeFS = new Boolean[1];
    isDeleteOverNativeFS[0] = false;
    Boolean[] isDeleteOverDFSEndpoint = new Boolean[1];
    isDeleteOverDFSEndpoint[0] = false;

    countDeleteOverAbfsAndWasb(nativeAzureFileSystem, client, isDeleteOverNativeFS,
        isDeleteOverDFSEndpoint);

    fs.create(new Path("/file"));
    fs.delete(new Path("/file"), true);

    Assert.assertFalse(isDeleteOverDFSEndpoint[0]);
    Assert.assertTrue(isDeleteOverNativeFS[0]);
  }

  @Test
  public void testDfsEndpointWithDeleteWasbRedirect() throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(createFileSystemForEndpointConfigPair(
        FS_AZURE_REDIRECT_DELETE, true, true));
    Assert.assertNotNull(fs.getNativeFs());
    NativeAzureFileSystem nativeAzureFileSystem = Mockito.spy(fs.getNativeFs());
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    AbfsClient client = Mockito.spy(fs.getAbfsClient());
    assignStoreMocksToFs(fs, nativeAzureFileSystem, store, client);

    Boolean[] isDeleteOverNativeFS = new Boolean[1];
    isDeleteOverNativeFS[0] = false;
    Boolean[] isDeleteOverDFSEndpoint = new Boolean[1];
    isDeleteOverDFSEndpoint[0] = false;

    countDeleteOverAbfsAndWasb(nativeAzureFileSystem, client, isDeleteOverNativeFS,
        isDeleteOverDFSEndpoint);

    fs.create(new Path("/file"));
    fs.delete(new Path("/file"), true);

    Assert.assertFalse(isDeleteOverDFSEndpoint[0]);
    Assert.assertTrue(isDeleteOverNativeFS[0]);
  }

  @Test
  public void testDfsEndpointNoDeleteWasbRedirect() throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(createFileSystemForEndpointConfigPair(
        FS_AZURE_REDIRECT_DELETE, false, true));
    Assert.assertNull(fs.getNativeFs());
    NativeAzureFileSystem nativeAzureFileSystem = null;
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    AbfsClient client = Mockito.spy(fs.getAbfsClient());
    assignStoreMocksToFs(fs, nativeAzureFileSystem, store, client);

    Boolean[] isDeleteOverNativeFS = new Boolean[1];
    isDeleteOverNativeFS[0] = false;
    Boolean[] isDeleteOverDFSEndpoint = new Boolean[1];
    isDeleteOverDFSEndpoint[0] = false;

    countDeleteOverAbfsAndWasb(nativeAzureFileSystem, client, isDeleteOverNativeFS,
        isDeleteOverDFSEndpoint);

    fs.create(new Path("/file"));
    fs.delete(new Path("/file"), true);

    Assert.assertTrue(isDeleteOverDFSEndpoint[0]);
    Assert.assertFalse(isDeleteOverNativeFS[0]);
  }

  private void countDeleteOverAbfsAndWasb(final NativeAzureFileSystem nativeAzureFileSystem,
      final AbfsClient client,
      final Boolean[] isDeleteOverNativeFS,
      final Boolean[] isDeleteOverDFSEndpoint) throws IOException {
    Mockito.doAnswer(answer -> {
      isDeleteOverDFSEndpoint[0] = true;
      return answer.callRealMethod();
    }).when(client).deletePath(Mockito.any(String.class), Mockito.anyBoolean(), Mockito.nullable(String.class), Mockito.any(TracingContext.class));

    if(nativeAzureFileSystem != null) {
      Mockito.doAnswer(answer -> {
        isDeleteOverNativeFS[0] = true;
        return answer.callRealMethod();
      }).when(nativeAzureFileSystem).delete(Mockito.any(Path.class), Mockito.anyBoolean());
    }
  }

  private void assignStoreMocksToFs(final AzureBlobFileSystem fs,
      final NativeAzureFileSystem nativeAzureFileSystem,
      final AzureBlobFileSystemStore store,
      final AbfsClient client) {
    Mockito.doReturn(store).when(fs).getAbfsStore();
    store.setClient(client);
    Mockito.doReturn(nativeAzureFileSystem).when(fs).getNativeFs();
  }

  private void checkDirAndFileCreationOnDFS(final AbfsClient client,
      final int[] dirCreatedOverDFSExecCount,
      final int[] fileCreatedOverDFSExecCount)
      throws AzureBlobFileSystemException {
    Mockito.doAnswer(answer -> {
          if (!(Boolean) answer.getArgument(1)) {
            dirCreatedOverDFSExecCount[0]++;
          } else {
            fileCreatedOverDFSExecCount[0]++;
          }
          return answer.callRealMethod();
        })
        .when(client)
        .createPath(Mockito.anyString(), Mockito.anyBoolean(),
            Mockito.anyBoolean(), Mockito.nullable(String.class),
            Mockito.nullable(String.class), Mockito.anyBoolean(),
            Mockito.nullable(String.class), Mockito.any(TracingContext.class));
  }


  private void verifyCreatePathBlobExecution(final AbfsClient client)
      throws AzureBlobFileSystemException {
    Mockito.verify(client, Mockito.times(1))
        .createPathBlob(Mockito.anyString(), Mockito.anyBoolean(),
            Mockito.anyBoolean(), Mockito.nullable(
                HashMap.class), Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));
  }

  private AzureBlobFileSystem createFileSystemForEndpointConfigPair(String configName,
      Boolean configVal,
      Boolean dfsEndpoint) throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Assume.assumeFalse(
        fs.getIsNamespaceEnabled(Mockito.mock(TracingContext.class)));
    Configuration configuration = Mockito.spy(getRawConfiguration());
    fixEndpointAsPerTest(configuration, dfsEndpoint);
    if (configVal != null) {
      getRawConfiguration().set(configName, configVal.toString());
    }
    return (AzureBlobFileSystem) FileSystem.newInstance(getRawConfiguration());
  }

  private void fixEndpointAsPerTest(Configuration configuration,
      final Boolean dfsEndpoint) {
    if (dfsEndpoint) {
      String url = getTestUrl();
      if (url.contains(WASB_DNS_PREFIX)) {
        url = url.replace(WASB_DNS_PREFIX, ABFS_DNS_PREFIX);
        configuration.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
            url);
      }
    } else {
      String url = getTestUrl();
      if (url.contains(ABFS_DNS_PREFIX)) {
        url = url.replace(ABFS_DNS_PREFIX, WASB_DNS_PREFIX);
        configuration.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
            url);
      }
    }
  }
}