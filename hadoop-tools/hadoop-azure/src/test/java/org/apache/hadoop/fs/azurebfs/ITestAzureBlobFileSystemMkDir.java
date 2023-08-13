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

import java.util.UUID;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.OperativeEndpoint;
import org.apache.hadoop.fs.azurebfs.services.PrefixMode;
import org.apache.hadoop.fs.azurebfs.services.ITestAbfsClient;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;

import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_MKDIR_OVERWRITE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_MKDIRS_FALLBACK_TO_DFS;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_FS_AZURE_ENABLE_MKDIR_OVERWRITE;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertMkdirs;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test mkdir operation.
 */
public class ITestAzureBlobFileSystemMkDir extends AbstractAbfsIntegrationTest {

  public ITestAzureBlobFileSystemMkDir() throws Exception {
    super();
  }

  @Test
  public void testCreateDirWithExistingDir() throws Exception {
    Assume.assumeTrue(
        DEFAULT_FS_AZURE_ENABLE_MKDIR_OVERWRITE || !getIsNamespaceEnabled(
            getFileSystem()));
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("testFolder");
    assertMkdirs(fs, path);
    assertMkdirs(fs, path);
  }

  @Test
  public void testMkdirExistingDirOverwriteFalse() throws Exception {
    Assume.assumeFalse("Ignore test until default overwrite is set to false",
        DEFAULT_FS_AZURE_ENABLE_MKDIR_OVERWRITE);
    Assume.assumeTrue("Ignore test for Non-HNS accounts",
        getIsNamespaceEnabled(getFileSystem()));
    //execute test only for HNS account with default overwrite=false
    Configuration config = new Configuration(this.getRawConfiguration());
    config.set(FS_AZURE_ENABLE_MKDIR_OVERWRITE, Boolean.toString(false));
    AzureBlobFileSystem fs = getFileSystem(config);
    Path path = new Path("testFolder");
    assertMkdirs(fs, path); //checks that mkdirs returns true
    long timeCreated = fs.getFileStatus(path).getModificationTime();
    assertMkdirs(fs, path); //call to existing dir should return success
    assertEquals("LMT should not be updated for existing dir", timeCreated,
        fs.getFileStatus(path).getModificationTime());
  }

  @Test
  public void createDirWithExistingFilename() throws Exception {
    Assume.assumeFalse("Ignore test until default overwrite is set to false",
        DEFAULT_FS_AZURE_ENABLE_MKDIR_OVERWRITE && getIsNamespaceEnabled(
            getFileSystem()));
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("testFilePath");
    fs.create(path);
    assertTrue(fs.getFileStatus(path).isFile());
    intercept(FileAlreadyExistsException.class, () -> fs.mkdirs(path));
  }

  @Test
  public void testCreateRoot() throws Exception {
    assertMkdirs(getFileSystem(), new Path("/"));
  }

  /**
   * Test mkdir for possible values of fs.azure.disable.default.create.overwrite
   * @throws Exception
   */
  @Test
  public void testDefaultCreateOverwriteDirTest() throws Throwable {
    // the config fs.azure.disable.default.create.overwrite should have no
    // effect on mkdirs
    testCreateDirOverwrite(true);
    testCreateDirOverwrite(false);
  }

  public void testCreateDirOverwrite(boolean enableConditionalCreateOverwrite)
      throws Throwable {
    final AzureBlobFileSystem currentFs = getFileSystem();
    Configuration config = new Configuration(this.getRawConfiguration());
    config.set("fs.azure.enable.conditional.create.overwrite",
        Boolean.toString(enableConditionalCreateOverwrite));

    final AzureBlobFileSystem fs =
        (AzureBlobFileSystem) FileSystem.newInstance(currentFs.getUri(),
            config);

    long totalConnectionMadeBeforeTest = fs.getInstrumentationMap()
        .get(CONNECTIONS_MADE.getStatName());

    int mkdirRequestCount = 0;
    final Path dirPath = new Path("/DirPath_"
        + UUID.randomUUID().toString());

    // Case 1: Dir does not pre-exist
    fs.mkdirs(dirPath);

    // One request to server for dfs and 2 for blob because child calls mkdir for parent.
    if (!OperativeEndpoint.isMkdirEnabledOnDFS(getAbfsStore(fs).getAbfsConfiguration())) {
      mkdirRequestCount += 2;
    } else {
      mkdirRequestCount++;
    }

    assertAbfsStatistics(
        CONNECTIONS_MADE,
        totalConnectionMadeBeforeTest + mkdirRequestCount,
        fs.getInstrumentationMap());

    // Case 2: Dir pre-exists
    // Mkdir on existing Dir path will not lead to failure
    fs.mkdirs(dirPath);

    // One request to server for dfs and 3 for blob because child calls mkdir for parent.
    if (!OperativeEndpoint.isMkdirEnabledOnDFS(getAbfsStore(fs).getAbfsConfiguration())) {
      mkdirRequestCount += 3;
    } else {
      mkdirRequestCount++;
    }

    assertAbfsStatistics(
        CONNECTIONS_MADE,
        totalConnectionMadeBeforeTest + mkdirRequestCount,
        fs.getInstrumentationMap());
  }

  @Test
  public void testVerifyGetBlobProperty() throws Exception {
    Assume.assumeTrue(getFileSystem().getAbfsStore().getPrefixMode() == PrefixMode.BLOB);
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();
    AbfsClient client = store.getClient();
    AbfsClient testClient = Mockito.spy(ITestAbfsClient.createTestClientFromCurrentContext(
            client,
            fs.getAbfsStore().getAbfsConfiguration()));
    store.setClient(testClient);

    createAzCopyDirectory(new Path("/src"));
    intercept(AbfsRestOperationException.class, () -> {
      store.getBlobProperty(new Path("/src"), getTestTracingContext(fs, true));
    });
    fs.mkdirs(new Path("/src/dir"));
    Mockito.verify(testClient, Mockito.times(0)).getPathStatus(Mockito.any(String.class),
            Mockito.anyBoolean(), Mockito.any(TracingContext.class));
    Mockito.verify(testClient, Mockito.atLeast(1)).getBlobProperty(Mockito.any(Path.class),
            Mockito.any(TracingContext.class));

  }

  @Test
  public void testGetPathPropertyCalledOnMkdirBlobEndpoint() throws Exception {
    Assume.assumeTrue(getFileSystem().getAbfsStore().getPrefixMode() == PrefixMode.BLOB);
    AbfsConfiguration abfsConfig = new AbfsConfiguration(getRawConfiguration(), getAccountName());
    abfsConfig.setBoolean(FS_AZURE_MKDIRS_FALLBACK_TO_DFS, false);
    AzureBlobFileSystem fs = Mockito.spy((AzureBlobFileSystem) FileSystem.newInstance(abfsConfig.getRawConfiguration()));
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();

    fs.mkdirs(new Path("/testPath"));
    Mockito.verify(store, Mockito.times(1)).tryGetPathProperty(Mockito.any(Path.class),
            Mockito.any(TracingContext.class), Mockito.any(Boolean.class));
    Mockito.verify(store, Mockito.times(1)).getPathProperty(Mockito.any(Path.class),
            Mockito.any(TracingContext.class), Mockito.any(Boolean.class));

  }

  @Test
  public void testGetPathPropertyCalledOnMkdirFallbackDFS() throws Exception {
    Assume.assumeTrue(getFileSystem().getAbfsStore().getPrefixMode() == PrefixMode.BLOB);
    AbfsConfiguration abfsConfig = new AbfsConfiguration(getRawConfiguration(), getAccountName());
    abfsConfig.setBoolean(FS_AZURE_MKDIRS_FALLBACK_TO_DFS, true);
    AzureBlobFileSystem fs = Mockito.spy((AzureBlobFileSystem) FileSystem.newInstance(abfsConfig.getRawConfiguration()));
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();


    fs.mkdirs(new Path("/testPath"));
    Mockito.verify(store, Mockito.times(0)).tryGetPathProperty(Mockito.any(Path.class),
            Mockito.any(TracingContext.class), Mockito.any(Boolean.class));
    Mockito.verify(store, Mockito.times(0)).getPathProperty(Mockito.any(Path.class),
            Mockito.any(TracingContext.class), Mockito.any(Boolean.class));

  }

  @Test
  public void testMkdirWithExistingFilenameBlobEndpoint() throws Exception {
    Assume.assumeTrue(getFileSystem().getAbfsStore().getPrefixMode() == PrefixMode.BLOB);
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();

    fs.create(new Path("/testFilePath"));
    intercept(FileAlreadyExistsException.class, () -> fs.mkdirs(new Path("/testFilePath")));
    intercept(FileAlreadyExistsException.class, () -> fs.mkdirs(new Path("/testFilePath/newDir")));
  }
}

