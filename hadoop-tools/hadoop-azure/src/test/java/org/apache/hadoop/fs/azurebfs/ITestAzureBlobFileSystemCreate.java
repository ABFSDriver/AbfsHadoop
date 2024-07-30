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

import java.io.FileNotFoundException;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientHandler;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.GenericTestUtils;

import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.ConcurrentWriteOperationDetectedException;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.ITestAbfsClient;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_MKDIR_OVERWRITE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_LEASE_CREATE_NON_RECURSIVE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsFile;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;

/**
 * Test create operation.
 */
public class ITestAzureBlobFileSystemCreate extends
    AbstractAbfsIntegrationTest {
  private static final Path TEST_FILE_PATH = new Path("testfile");
  private static final String TEST_FOLDER_PATH = "testFolder";
  private static final String TEST_CHILD_FILE = "childFile";

  public ITestAzureBlobFileSystemCreate() throws Exception {
    super();
  }

  @Test
  public void testEnsureFileCreatedImmediately() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    FSDataOutputStream out = fs.create(TEST_FILE_PATH);
    try {
      assertIsFile(fs, TEST_FILE_PATH);
    } finally {
      out.close();
    }
    assertIsFile(fs, TEST_FILE_PATH);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCreateNonRecursive() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFolderPath = path(TEST_FOLDER_PATH);
    Path testFile = new Path(testFolderPath, TEST_CHILD_FILE);
    try {
      fs.createNonRecursive(testFile, true, 1024, (short) 1, 1024, null);
      fail("Should've thrown");
    } catch (FileNotFoundException expected) {
    }
    fs.registerListener(new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.MKDIR, false, 0));
    fs.mkdirs(testFolderPath);
    fs.registerListener(null);

    fs.createNonRecursive(testFile, true, 1024, (short) 1, 1024, null)
        .close();
    assertIsFile(fs, testFile);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCreateNonRecursive1() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFolderPath = path(TEST_FOLDER_PATH);
    Path testFile = new Path(testFolderPath, TEST_CHILD_FILE);
    try {
      fs.createNonRecursive(testFile, FsPermission.getDefault(), EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE), 1024, (short) 1, 1024, null);
      fail("Should've thrown");
    } catch (FileNotFoundException expected) {
    }
    fs.mkdirs(testFolderPath);
    fs.createNonRecursive(testFile, true, 1024, (short) 1, 1024, null)
        .close();
    assertIsFile(fs, testFile);

  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCreateNonRecursive2() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();

    Path testFolderPath = path(TEST_FOLDER_PATH);
    Path testFile = new Path(testFolderPath, TEST_CHILD_FILE);
    try {
      fs.createNonRecursive(testFile, FsPermission.getDefault(), false, 1024, (short) 1, 1024, null);
      fail("Should've thrown");
    } catch (FileNotFoundException e) {
    }
    fs.mkdirs(testFolderPath);
    fs.createNonRecursive(testFile, true, 1024, (short) 1, 1024, null)
        .close();
    assertIsFile(fs, testFile);
  }

  @Test
  public void testCreateOnRoot() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFile = path(AbfsHttpConstants.ROOT_PATH);
    AbfsRestOperationException ex = intercept(AbfsRestOperationException.class, () ->
        fs.create(testFile, true));
    if (ex.getStatusCode() != HTTP_CONFLICT) {
      // Request should fail with 409.
      throw ex;
    }

    ex = intercept(AbfsRestOperationException.class, () ->
        fs.createNonRecursive(testFile, FsPermission.getDefault(),
            false, 1024, (short) 1, 1024, null));
    if (ex.getStatusCode() != HTTP_CONFLICT) {
      // Request should fail with 409.
      throw ex;
    }
  }

  /**
   * Attempts to use to the ABFS stream after it is closed.
   */
  @Test
  public void testWriteAfterClose() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFolderPath = path(TEST_FOLDER_PATH);
    Path testPath = new Path(testFolderPath, TEST_CHILD_FILE);
    FSDataOutputStream out = fs.create(testPath);
    out.close();
    intercept(IOException.class, () -> out.write('a'));
    intercept(IOException.class, () -> out.write(new byte[]{'a'}));
    // hsync is not ignored on a closed stream
    // out.hsync();
    out.flush();
    out.close();
  }

  /**
   * Attempts to double close an ABFS output stream from within a
   * FilterOutputStream.
   * That class handles a double failure on close badly if the second
   * exception rethrows the first.
   */
  @Test
  public void testTryWithResources() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFolderPath = path(TEST_FOLDER_PATH);
    Path testPath = new Path(testFolderPath, TEST_CHILD_FILE);
    try (FSDataOutputStream out = fs.create(testPath)) {
      out.write('1');
      out.hsync();
      // this will cause the next write to failAll
      fs.delete(testPath, false);
      out.write('2');
      out.hsync();
      fail("Expected a failure");
    } catch (IOException fnfe) {
      //appendblob outputStream does not generate suppressed exception on close as it is
      //single threaded code
      if (!fs.getAbfsStore().isAppendBlobKey(fs.makeQualified(testPath).toString())) {
        // the exception raised in close() must be in the caught exception's
        // suppressed list
        Throwable[] suppressed = fnfe.getSuppressed();
        Assertions.assertThat(suppressed.length)
            .describedAs("suppressed count should be 1").isEqualTo(1);
        Throwable inner = suppressed[0];
        if (!(inner instanceof IOException)) {
          throw inner;
        }
        GenericTestUtils.assertExceptionContains(fnfe.getMessage(), inner);
      }
    }
  }

  /**
   * Attempts to write to the azure stream after it is closed will raise
   * an IOException.
   */
  @Test
  public void testFilterFSWriteAfterClose() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testPath = new Path(TEST_FOLDER_PATH, TEST_CHILD_FILE);
    FSDataOutputStream out = fs.create(testPath);
    intercept(IOException.class,
        () -> {
          try (FilterOutputStream fos = new FilterOutputStream(out)) {
            fos.write('a');
            fos.flush();
            out.hsync();
            fs.delete(testPath, false);
            // trigger the first failure
            throw intercept(IOException.class,
                () -> {
              fos.write('b');
              out.hsync();
              return "hsync didn't raise an IOE";
            });
          }
        });
  }

  /**
   * Tests if the number of connections made for:
   * 1. create overwrite=false of a file that doesnt pre-exist
   * 2. create overwrite=false of a file that pre-exists
   * 3. create overwrite=true of a file that doesnt pre-exist
   * 4. create overwrite=true of a file that pre-exists
   * matches the expectation when run against both combinations of
   * fs.azure.enable.conditional.create.overwrite=true and
   * fs.azure.enable.conditional.create.overwrite=false
   * @throws Throwable
   */
  @Test
  public void testDefaultCreateOverwriteFileTest() throws Throwable {
    testCreateFileOverwrite(true);
    testCreateFileOverwrite(false);
  }

  public void testCreateFileOverwrite(boolean enableConditionalCreateOverwrite)
      throws Throwable {
    final AzureBlobFileSystem currentFs = getFileSystem();
    Configuration config = new Configuration(this.getRawConfiguration());
    config.set("fs.azure.enable.conditional.create.overwrite",
        Boolean.toString(enableConditionalCreateOverwrite));
    AzureBlobFileSystemStore store = currentFs.getAbfsStore();
    AbfsClient client = store.getClientHandler().getIngressClient();

    final AzureBlobFileSystem fs =
        (AzureBlobFileSystem) FileSystem.newInstance(currentFs.getUri(),
            config);

    long totalConnectionMadeBeforeTest = fs.getInstrumentationMap()
        .get(CONNECTIONS_MADE.getStatName());

    int createRequestCount = 0;
    final Path nonOverwriteFile = new Path("/NonOverwriteTest_FileName_"
        + UUID.randomUUID().toString());

    // Case 1: Not Overwrite - File does not pre-exist
    // create should be successful
    fs.create(nonOverwriteFile, false);

    // One request to server to create path should be issued
    // two calls added for -
    // 1. getFileStatus on DFS endpoint : 1
    //    getFileStatus on Blob endpoint: 2 (Additional List blob call)
    // 2. actual create call: 1
    createRequestCount += (client instanceof AbfsBlobClient && !getIsNamespaceEnabled(fs) ? 2: 1);

    assertAbfsStatistics(
        CONNECTIONS_MADE,
        totalConnectionMadeBeforeTest + createRequestCount,
        fs.getInstrumentationMap());


    // Case 2: Not Overwrite - File pre-exists
    fs.registerListener(new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.CREATE, false, 0));
    intercept(FileAlreadyExistsException.class,
        () -> fs.create(nonOverwriteFile, false));
    fs.registerListener(null);

    // One request to server to create path should be issued
    // Only single tryGetFileStatus should happen
    // 1. getFileStatus on DFS endpoint : 1
    //    getFileStatus on Blob endpoint: 1 (No Additional List blob call as file exists)

    createRequestCount += (client instanceof AbfsBlobClient && !getIsNamespaceEnabled(fs) ? 2: 1);

    assertAbfsStatistics(
        CONNECTIONS_MADE,
        totalConnectionMadeBeforeTest + createRequestCount,
        fs.getInstrumentationMap());

    final Path overwriteFilePath = new Path("/OverwriteTest_FileName_"
        + UUID.randomUUID().toString());

    // Case 3: Overwrite - File does not pre-exist
    // create should be successful
    fs.create(overwriteFilePath, true);

    /// One request to server to create path should be issued
    // two calls added for -
    // 1. getFileStatus on DFS endpoint : 1
    //    getFileStatus on Blob endpoint: 2 (Additional List blob call for non-existing path)
    // 2. actual create call: 1
    createRequestCount += (client instanceof AbfsBlobClient && !getIsNamespaceEnabled(fs) ? 2: 1);

    assertAbfsStatistics(
        CONNECTIONS_MADE,
        totalConnectionMadeBeforeTest + createRequestCount,
        fs.getInstrumentationMap());

    // Case 4: Overwrite - File pre-exists
    fs.registerListener(new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.CREATE, true, 0));
    fs.create(overwriteFilePath, true);
    fs.registerListener(null);

    createRequestCount += (client instanceof AbfsBlobClient && !getIsNamespaceEnabled(fs) ? 1: 0);

    // Second actual create call will hap
    if (enableConditionalCreateOverwrite) {
      // Three requests will be sent to server to create path,
      // 1. create without overwrite
      // 2. GetFileStatus to get eTag
      // 3. create with overwrite
      createRequestCount += (client instanceof AbfsBlobClient && !getIsNamespaceEnabled(fs) ? 4: 3);
    } else {
      createRequestCount++;
    }

    assertAbfsStatistics(
        CONNECTIONS_MADE,
        totalConnectionMadeBeforeTest + createRequestCount,
        fs.getInstrumentationMap());
  }

  /**
   * Test negative scenarios with Create overwrite=false as default
   * With create overwrite=true ending in 3 calls:
   * A. Create overwrite=false
   * B. GFS
   * C. Create overwrite=true
   *
   * Scn1: A fails with HTTP409, leading to B which fails with HTTP404,
   *        detect parallel access
   * Scn2: A fails with HTTP409, leading to B which fails with HTTP500,
   *        fail create with HTTP500
   * Scn3: A fails with HTTP409, leading to B and then C,
   *        which fails with HTTP412, detect parallel access
   * Scn4: A fails with HTTP409, leading to B and then C,
   *        which fails with HTTP500, fail create with HTTP500
   * Scn5: A fails with HTTP500, fail create with HTTP500
   */
  @Test
  public void testNegativeScenariosForCreateOverwriteDisabled()
      throws Throwable {

    final AzureBlobFileSystem currentFs = getFileSystem();
    Configuration config = new Configuration(this.getRawConfiguration());
    config.set("fs.azure.enable.conditional.create.overwrite",
        Boolean.toString(true));

    final AzureBlobFileSystem fs =
        (AzureBlobFileSystem) FileSystem.newInstance(currentFs.getUri(),
            config);

    // Get mock AbfsClient with current config
    AbfsClient
        mockClient
        = ITestAbfsClient.getMockAbfsClient(
        fs.getAbfsStore().getClient(),
        fs.getAbfsStore().getAbfsConfiguration());
    AbfsClientHandler clientHandler = Mockito.mock(AbfsClientHandler.class);
    when(clientHandler.getIngressClient()).thenReturn(mockClient);
    when(clientHandler.getClient(Mockito.any())).thenReturn(mockClient);

    AzureBlobFileSystemStore abfsStore = fs.getAbfsStore();
    abfsStore = setAzureBlobSystemStoreField(abfsStore, "clientHandler", clientHandler);
    abfsStore = setAzureBlobSystemStoreField(abfsStore, "client", mockClient);
    boolean isNamespaceEnabled = abfsStore
        .getIsNamespaceEnabled(getTestTracingContext(fs, false));

    AbfsRestOperation successOp = mock(
        AbfsRestOperation.class);
    AbfsHttpOperation http200Op = mock(
        AbfsHttpOperation.class);
    when(http200Op.getStatusCode()).thenReturn(HTTP_OK);
    when(successOp.getResult()).thenReturn(http200Op);

    AbfsRestOperationException conflictResponseEx
        = getMockAbfsRestOperationException(HTTP_CONFLICT);
    AbfsRestOperationException serverErrorResponseEx
        = getMockAbfsRestOperationException(HTTP_INTERNAL_ERROR);
    AbfsRestOperationException fileNotFoundResponseEx
        = getMockAbfsRestOperationException(HTTP_NOT_FOUND);
    AbfsRestOperationException preConditionResponseEx
        = getMockAbfsRestOperationException(HTTP_PRECON_FAILED);

    // mock for overwrite=false
    doThrow(conflictResponseEx) // Scn1: GFS fails with Http404
        .doThrow(conflictResponseEx) // Scn2: GFS fails with Http500
        .doThrow(
            conflictResponseEx) // Scn3: create overwrite=true fails with Http412
        .doThrow(
            conflictResponseEx) // Scn4: create overwrite=true fails with Http500
        .doThrow(
            serverErrorResponseEx) // Scn5: create overwrite=false fails with Http500
        .when(mockClient)
        .createPath(any(String.class), eq(true), eq(false),
            any(AzureBlobFileSystemStore.Permissions.class), any(boolean.class), eq(null), any(),
            any(TracingContext.class), any(boolean.class));

    doThrow(fileNotFoundResponseEx) // Scn1: GFS fails with Http404
        .doThrow(serverErrorResponseEx) // Scn2: GFS fails with Http500
        .doReturn(successOp) // Scn3: create overwrite=true fails with Http412
        .doReturn(successOp) // Scn4: create overwrite=true fails with Http500
        .when(mockClient)
        .getPathStatus(any(String.class), eq(false), any(TracingContext.class), nullable(
            ContextEncryptionAdapter.class));

    // mock for overwrite=true
    doThrow(
        preConditionResponseEx) // Scn3: create overwrite=true fails with Http412
        .doThrow(
            serverErrorResponseEx) // Scn4: create overwrite=true fails with Http500
        .when(mockClient)
        .createPath(any(String.class), eq(true), eq(true),
            any(AzureBlobFileSystemStore.Permissions.class), any(boolean.class), eq(null), any(),
            any(TracingContext.class), any(boolean.class));

    // Scn1: GFS fails with Http404
    // Sequence of events expected:
    // 1. create overwrite=false - fail with conflict
    // 2. GFS - fail with File Not found
    // Create will fail with ConcurrentWriteOperationDetectedException
    validateCreateFileException(ConcurrentWriteOperationDetectedException.class,
        abfsStore);

    // Scn2: GFS fails with Http500
    // Sequence of events expected:
    // 1. create overwrite=false - fail with conflict
    // 2. GFS - fail with Server error
    // Create will fail with 500
    validateCreateFileException(AbfsRestOperationException.class, abfsStore);

    // Scn3: create overwrite=true fails with Http412
    // Sequence of events expected:
    // 1. create overwrite=false - fail with conflict
    // 2. GFS - pass
    // 3. create overwrite=true - fail with Pre-Condition
    // Create will fail with ConcurrentWriteOperationDetectedException
    validateCreateFileException(ConcurrentWriteOperationDetectedException.class,
        abfsStore);

    // Scn4: create overwrite=true fails with Http500
    // Sequence of events expected:
    // 1. create overwrite=false - fail with conflict
    // 2. GFS - pass
    // 3. create overwrite=true - fail with Server error
    // Create will fail with 500
    validateCreateFileException(AbfsRestOperationException.class, abfsStore);

    // Scn5: create overwrite=false fails with Http500
    // Sequence of events expected:
    // 1. create overwrite=false - fail with server error
    // Create will fail with 500
    validateCreateFileException(AbfsRestOperationException.class, abfsStore);
  }

  private AzureBlobFileSystemStore setAzureBlobSystemStoreField(
      final AzureBlobFileSystemStore abfsStore,
      final String fieldName,
      Object fieldObject) throws Exception {

    Field abfsClientField = AzureBlobFileSystemStore.class.getDeclaredField(
        fieldName);
    abfsClientField.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(abfsClientField,
        abfsClientField.getModifiers() & ~java.lang.reflect.Modifier.FINAL);
    abfsClientField.set(abfsStore, fieldObject);
    return abfsStore;
  }

  private <E extends Throwable> void validateCreateFileException(final Class<E> exceptionClass, final AzureBlobFileSystemStore abfsStore)
      throws Exception {
    FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL,
        FsAction.ALL);
    FsPermission umask = new FsPermission(FsAction.NONE, FsAction.NONE,
        FsAction.NONE);
    Path testPath = new Path("/testFile");
    intercept(
        exceptionClass,
        () -> abfsStore.createFile(testPath, null, true, permission, umask,
            getTestTracingContext(getFileSystem(), true)));
  }

  private AbfsRestOperationException getMockAbfsRestOperationException(int status) {
    return new AbfsRestOperationException(status, "", "", new Exception());
  }

  /**
   * Creating subdirectory on existing file path should fail.
   * @throws Exception
   */
  @Test
  public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.create(new Path("a/b/c"));
    fs.mkdirs(new Path("a/b/d"));
    intercept(IOException.class, () -> fs.mkdirs(new Path("a/b/c/d/e")));

    assertTrue(fs.exists(new Path("a/b/c")));
    assertTrue(fs.exists(new Path("a/b/d")));
    // Asserting directory created still exists as explicit.
    FileStatus status = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path("a/b/d")),
        new TracingContext(getTestTracingContext(fs, true)));
    Assert.assertTrue("Path is not an explicit directory", status.isDirectory());
  }

  /**
   * Try creating file same as an existing directory.
   * @throws Exception
   */
  @Test
  public void testCreateDirectoryAndFile() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a/b/c"));
    assertTrue(fs.exists(new Path("a/b/c")));
    intercept(IOException.class, () -> fs.create(new Path("a/b/c")));
    // Asserting that directory still exists as explicit
    FileStatus status = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path("a/b/c")),
        new TracingContext(getTestTracingContext(fs, true)));
    Assert.assertTrue("Path is not an explicit directory", status.isDirectory());
  }

  /**
   * Creating same file without specifying overwrite.
   * @throws Exception
   */
  @Test
  public void testCreateSameFile() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.create(new Path("a/b/c"));
    fs.create(new Path("a/b/c"));
    assertTrue("File does not exist", fs.exists(new Path("a/b/c")));
  }

  /**
   * Creating same file with overwrite flag set to false.
   * @throws Exception
   */
  @Test
  public void testCreateSameFileWithOverwriteFalse() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.create(new Path("a/b/c"));
    assertTrue(fs.exists(new Path("a/b/c")));
    intercept(IOException.class, () -> fs.create(new Path("a/b/c"), false));
  }

  /**
   * Creation of already existing subpath should fail.
   * @throws Exception
   */
  @Test
  public void testCreateSubPath() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.create(new Path("a/b/c"));
    assertTrue(fs.exists(new Path("a/b/c")));
    intercept(IOException.class, () -> fs.create(new Path("a/b")));
  }

  /**
   * Creating path with parent explicit.
   */
  @Test
  public void testCreatePathParentExplicit() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a/b/c"));
    assertTrue(fs.exists(new Path("a/b/c")));
    fs.create(new Path("a/b/c/d"));
    assertTrue(fs.exists(new Path("a/b/c/d")));

    // asserting that parent stays explicit
    FileStatus status = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path("a/b/c")),
        new TracingContext(getTestTracingContext(fs, true)));
    Assert.assertTrue("Path is not an explicit directory", status.isDirectory());
  }

  /**
   * Tests create file when the parent is an existing file
   * should fail.
   * @throws Exception FileAlreadyExists for blob and IOException for dfs.
   */
  @Test
  public void testCreateFileParentFile() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final AzureBlobFileSystemStore store = fs.getAbfsStore();

    String parentName = "/testParentFile";
    Path parent = new Path(parentName);
    fs.create(parent);

    String childName = "/testParentFile/testChildFile";
    Path child = new Path(childName);
    IOException e = intercept(IOException.class, () ->
        fs.create(child, false));

    // asserting that parent stays explicit
    FileStatus status = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path(parentName)),
        new TracingContext(getTestTracingContext(fs, true)));
    Assert.assertFalse("Path is not a file", status.isDirectory());
  }

  /**
   * Creating directory on existing file path should fail.
   * @throws Exception
   */
  @Test
  public void testCreateMkdirs() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.create(new Path("a/b/c"));
    intercept(IOException.class, () -> fs.mkdirs(new Path("a/b/c/d")));
  }

  /**
   * Test mkdirs.
   * @throws Exception
   */
  @Test
  public void testMkdirs() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a/b"));
    fs.mkdirs(new Path("a/b/c/d"));
    fs.mkdirs(new Path("a/b/c/e"));

    assertTrue(fs.exists(new Path("a/b")));
    assertTrue(fs.exists(new Path("a/b/c/d")));
    assertTrue(fs.exists(new Path("a/b/c/e")));

    //Asserting that directories created as explicit
    FileStatus status = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path("a/b")),
        new TracingContext(getTestTracingContext(fs, true)));
    Assert.assertTrue("Path is not an explicit directory", status.isDirectory());
    FileStatus status1 = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path("a/b/c/d")),
        new TracingContext(getTestTracingContext(fs, true)));
    Assert.assertTrue("Path is not an explicit directory", status1.isDirectory());
    FileStatus status2 = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path("a/b/c/e")),
        new TracingContext(getTestTracingContext(fs, true)));
    Assert.assertTrue("Path is not an explicit directory", status2.isDirectory());
  }

  /**
   * Creating subpath of directory path should fail.
   * @throws Exception
   */
  @Test
  public void testMkdirsCreateSubPath() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a/b/c"));
    assertTrue(fs.exists(new Path("a/b/c")));
    intercept(IOException.class, () -> fs.create(new Path("a/b")));

    //Asserting that directories created as explicit
    FileStatus status2 = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path("a/b/c")),
        new TracingContext(getTestTracingContext(fs, true)));
    Assert.assertTrue("Path is not an explicit directory", status2.isDirectory());
  }

  /**
   * Test creation of directory by level.
   * @throws Exception
   */
  @Test
  public void testMkdirsByLevel() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a"));
    fs.mkdirs(new Path("a/b/c"));
    fs.mkdirs(new Path("a/b/c/d/e"));

    assertTrue(fs.exists(new Path("a")));
    assertTrue(fs.exists(new Path("a/b/c")));
    assertTrue(fs.exists(new Path("a/b/c/d/e")));

    //Asserting that directories created as explicit
    FileStatus status = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path("a/")),
        new TracingContext(getTestTracingContext(fs, true)));
    Assert.assertTrue("Path is not an explicit directory", status.isDirectory());
    FileStatus status1 = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path("a/b/c")),
        new TracingContext(getTestTracingContext(fs, true)));
    Assert.assertTrue("Path is not an explicit directory", status1.isDirectory());
    FileStatus status2 = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path("a/b/c/d/e")),
        new TracingContext(getTestTracingContext(fs, true)));
    Assert.assertTrue("Path is not an explicit directory", status2.isDirectory());
  }

  /*
    Delete part of a path and validate sub path exists.
   */
  @Test
  public void testMkdirsWithDelete() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a/b"));
    fs.mkdirs(new Path("a/b/c/d"));
    fs.delete(new Path("a/b/c/d"));
    fs.getFileStatus(new Path("a/b/c"));
    assertTrue(fs.exists(new Path("a/b/c")));
  }

  /**
   * Verify mkdir and rename of parent.
   */
  @Test
  public void testMkdirsWithRename() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a/b/c/d"));
    fs.create(new Path("e/file"));
    fs.delete(new Path("a/b/c/d"));
    assertTrue(fs.rename(new Path("e"), new Path("a/b/c/d")));
    assertTrue(fs.exists(new Path("a/b/c/d/file")));
  }

  /**
   * Create a file with name /dir1 and then mkdirs for /dir1/dir2 should fail.
   * @throws Exception
   */
  @Test
  public void testFileCreateMkdirsRoot() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.setWorkingDirectory(new Path("/"));
    final Path p1 = new Path("dir1");
    fs.create(p1);
    intercept(IOException.class, () -> fs.mkdirs(new Path("dir1/dir2")));
  }

  /**
   * Create a file with name /dir1 and then mkdirs for /dir1/dir2 should fail.
   * @throws Exception
   */
  @Test
  public void testFileCreateMkdirsNonRoot() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path p1 = new Path("dir1");
    fs.create(p1);
    intercept(IOException.class, () -> fs.mkdirs(new Path("dir1/dir2")));
  }

  /**
   * Creation of same directory without overwrite flag should pass.
   * @throws Exception
   */
  @Test
  public void testCreateSameDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a/b/c"));
    fs.mkdirs(new Path("a/b/c"));

    assertTrue(fs.exists(new Path("a/b/c")));
    //Asserting that directories created as explicit
    FileStatus status = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path("a/b/c")),
        new TracingContext(getTestTracingContext(fs, true)));
    Assert.assertTrue("Path is not an explicit directory", status.isDirectory());
  }

  /**
   * Creation of same directory without overwrite flag should pass.
   * @throws Exception
   */
  @Test
  public void testCreateSamePathDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.create(new Path("a"));
    intercept(IOException.class, () -> fs.mkdirs(new Path("a")));
  }

  /**
   * Creation of directory with root as parent
   */
  @Test
  public void testMkdirOnRootAsParent() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path path = new Path("a");
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(path);

    // Asserting that the directory created by mkdir exists as explicit.
    FileStatus status = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path("a")),
        new TracingContext(getTestTracingContext(fs, true)));
    Assert.assertTrue("Path is not an explicit directory", status.isDirectory());
  }

  /**
   * Creation of directory on root
   */
  @Test
  public void testMkdirOnRoot() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path path = new Path("/");
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(path);

    FileStatus status = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path("/")),
        new TracingContext(getTestTracingContext(fs, true)));
    Assert.assertTrue("Path is not an explicit directory", status.isDirectory());
  }

  /**
   * Creation of directory on path with unicode chars
   */
  @Test
  public void testMkdirUnicode() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path path = new Path("/dir\u0031");
    fs.mkdirs(path);

    // Asserting that the directory created by mkdir exists as explicit.
    FileStatus status = fs.getAbfsStore().getFileStatus(fs.makeQualified(path),
        new TracingContext(getTestTracingContext(fs, true)));
    Assert.assertTrue("Path is not an explicit directory", status.isDirectory());
  }

  /**
   * Creation of directory on same path with parallel threads.
   */
  @Test
  public void testMkdirParallelRequests() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path path = new Path("/dir1");

    ExecutorService es = Executors.newFixedThreadPool(3);

    List<CompletableFuture<Void>> tasks = new ArrayList<>();

    for (int i = 0; i < 3; i++) {
      CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
        try {
          fs.mkdirs(path);
        } catch (IOException e) {
          throw new CompletionException(e);
        }
      }, es);
      tasks.add(future);
    }

    // Wait for all the tasks to complete
    CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).join();

    // Assert that the directory created by mkdir exists as explicit
    FileStatus status = fs.getAbfsStore().getFileStatus(fs.makeQualified(path),
        new TracingContext(getTestTracingContext(fs, true)));
    Assert.assertTrue("Path is not an explicit directory", status.isDirectory());

  }


  /**
   * Creation of directory with overwrite set to false should not fail according to DFS code.
   * @throws Exception
   */
  @Test
  public void testCreateSameDirectoryOverwriteFalse() throws Exception {
    Configuration configuration = getRawConfiguration();
    configuration.setBoolean(FS_AZURE_ENABLE_MKDIR_OVERWRITE, false);
    AzureBlobFileSystem fs1 = (AzureBlobFileSystem) FileSystem.newInstance(configuration);
    fs1.mkdirs(new Path("a/b/c"));
    fs1.mkdirs(new Path("a/b/c"));

    //Asserting that directories created as explicit
    FileStatus status = fs1.getAbfsStore().getFileStatus(fs1.makeQualified(new Path("a/b/c")),
        new TracingContext(getTestTracingContext(fs1, true)));
    Assert.assertTrue("Path is not an explicit directory", status.isDirectory());
  }

  /**
   * Try creating directory same as an existing file.
   */
  @Test
  public void testCreateDirectoryAndFileRecreation() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a/b/c"));
    fs.create(new Path("a/b/c/d"));
    assertTrue(fs.exists(new Path("a/b/c")));
    assertTrue(fs.exists(new Path("a/b/c/d")));
    intercept(IOException.class, () -> fs.mkdirs(new Path("a/b/c/d")));
  }

  @Test
  public void testCreateNonRecursiveForAtomicDirectoryFile() throws Exception {
    AzureBlobFileSystem fileSystem = getFileSystem();
    fileSystem.setWorkingDirectory(new Path("/"));
    fileSystem.mkdirs(new Path("/hbase/dir"));
    fileSystem.createFile(new Path("/hbase/dir/file"))
        .overwrite(false)
        .replication((short) 1)
        .bufferSize(1024)
        .blockSize(1024)
        .build();
    Assert.assertTrue(fileSystem.exists(new Path("/hbase/dir/file")));
  }

  @Test
  public void testActiveCreateNonRecursiveDenyParallelReadOnAtomicDir()
      throws Exception {
    Assumptions.assumeThat(getFileSystem().getAbfsClient())
        .isInstanceOf(AbfsBlobClient.class);
    Assumptions.assumeThat(
            getAbfsStore(getFileSystem()).getClientHandler().getClient())
        .isInstanceOf(AbfsBlobClient.class);

    Configuration configuration = new Configuration(getRawConfiguration());
    configuration.set(FS_AZURE_LEASE_CREATE_NON_RECURSIVE, "true");
    try (AzureBlobFileSystem fs = Mockito.spy(
        (AzureBlobFileSystem) FileSystem.newInstance(configuration))) {

      fs.setWorkingDirectory(new Path("/"));
      AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
      Mockito.doReturn(store).when(fs).getAbfsStore();

      AbfsClientHandler clientHandler = Mockito.spy(
          fs.getAbfsStore().getClientHandler());
      Mockito.doReturn(clientHandler).when(store).getClientHandler();

      AbfsBlobClient client = Mockito.spy(
          (AbfsBlobClient) clientHandler.getIngressClient());
      Mockito.doReturn(client).when(clientHandler).getIngressClient();
      Mockito.doReturn(client).when(store).getClient();

      fs.mkdirs(new Path("/hbase/dir"));

      int[] renameErrorCounter = {0};

      Mockito.doAnswer(answer -> {
            Object obj = answer.callRealMethod();
            intercept(IOException.class, () -> {
              fs.rename(new Path("/hbase/dir"), new Path("/hbase/dir1"));
            });
            renameErrorCounter[0]++;
            return obj;
          })
          .when(client)
          .takeAbfsLease(Mockito.anyString(), Mockito.anyLong(),
              Mockito.any(TracingContext.class));

      fs.createFile(new Path("/hbase/dir/file"))
          .overwrite(false)
          .replication((short) 1)
          .bufferSize(1024)
          .blockSize(1024)
          .build();

      Assertions.assertThat(renameErrorCounter[0]).isEqualTo(1);
    }
  }

  @Test
  public void testNoLeaseTakenOnCreateNonRecursiveIfConfigIsOff()
      throws Exception {
    Assumptions.assumeThat(getFileSystem().getAbfsClient())
        .isInstanceOf(AbfsBlobClient.class);
    Assumptions.assumeThat(
            getAbfsStore(getFileSystem()).getClientHandler().getClient())
        .isInstanceOf(AbfsBlobClient.class);

    Configuration configuration = new Configuration(getRawConfiguration());
    configuration.set(FS_AZURE_LEASE_CREATE_NON_RECURSIVE, "false");
    try (AzureBlobFileSystem fs = Mockito.spy(
        (AzureBlobFileSystem) FileSystem.newInstance(configuration))) {

      fs.setWorkingDirectory(new Path("/"));
      AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
      Mockito.doReturn(store).when(fs).getAbfsStore();

      AbfsClientHandler clientHandler = Mockito.spy(
          fs.getAbfsStore().getClientHandler());
      Mockito.doReturn(clientHandler).when(store).getClientHandler();

      AbfsBlobClient client = Mockito.spy(
          (AbfsBlobClient) clientHandler.getIngressClient());
      Mockito.doReturn(client).when(clientHandler).getIngressClient();
      Mockito.doReturn(client).when(store).getClient();

      fs.mkdirs(new Path("/hbase/dir"));


      fs.createFile(new Path("/hbase/dir/file"))
          .overwrite(false)
          .replication((short) 1)
          .bufferSize(1024)
          .blockSize(1024)
          .build();

      Mockito.verify(client, Mockito.never())
          .takeAbfsLease(Mockito.anyString(), Mockito.anyLong(),
              Mockito.any(TracingContext.class));
    }
  }

  @Test
  public void testCreateNonRecursiveOnAtomicPathWithParentNotExists()
      throws Exception {
    Assumptions.assumeThat(getFileSystem().getAbfsClient())
        .isInstanceOf(AbfsBlobClient.class);
    Configuration configuration = new Configuration(getRawConfiguration());
    configuration.set(FS_AZURE_LEASE_CREATE_NON_RECURSIVE, "true");
    try (AzureBlobFileSystem fs = (AzureBlobFileSystem)FileSystem.newInstance(configuration)) {
      intercept(FileNotFoundException.class, () -> {
        fs.createNonRecursive(new Path("/hbase/dir/file"),
            FsPermission.getDefault(), false, 1024, (short) 1, 1024, null);
      });
    }
  }
}
