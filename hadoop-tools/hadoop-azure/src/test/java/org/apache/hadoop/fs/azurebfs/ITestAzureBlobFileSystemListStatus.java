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
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientTestUtil;
import org.apache.hadoop.fs.azurebfs.services.BlobList;
import org.apache.hadoop.fs.azurebfs.services.BlobProperty;
import org.apache.hadoop.fs.azurebfs.services.PrefixMode;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.apache.hadoop.fs.contract.ContractTestUtils;

import org.mockito.Mockito;
import org.mockito.stubbing.Stubber;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_LIST_MAX_RESULTS;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_DNS_PREFIX;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.WASB_DNS_PREFIX;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_TIMEOUT_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_TIMEOUT_JDK_MESSAGE;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertMkdirs;
import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists;
import static org.apache.hadoop.fs.contract.ContractTestUtils.rename;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * Test listStatus operation.
 */
public class ITestAzureBlobFileSystemListStatus extends
    AbstractAbfsIntegrationTest {
  private static final int TEST_FILES_NUMBER = 6000;

  public ITestAzureBlobFileSystemListStatus() throws Exception {
    super();
  }

  @Test
  public void testListPath() throws Exception {
    Configuration config = Mockito.spy(this.getRawConfiguration());
    config.set(AZURE_LIST_MAX_RESULTS, "5000");
    final AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem
        .newInstance(getFileSystem().getUri(), config);
    final List<Future<Void>> tasks = new ArrayList<>();

    ExecutorService es = Executors.newFixedThreadPool(10);
    for (int i = 0; i < TEST_FILES_NUMBER; i++) {
      final Path fileName = new Path("/test" + i);
      Callable<Void> callable = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          touch(fileName);
          return null;
        }
      };

      tasks.add(es.submit(callable));
    }

    for (Future<Void> task : tasks) {
      task.get();
    }

    es.shutdownNow();
    fs.registerListener(
        new TracingHeaderValidator(getConfiguration().getClientCorrelationId(),
            fs.getFileSystemId(), FSOperationType.LISTSTATUS, true, 0));
    FileStatus[] files = fs.listStatus(new Path("/"));
    assertEquals(TEST_FILES_NUMBER, files.length /* user directory */);
  }

  /**
   * Test to verify that each paginated call to ListBlobs uses a new tracing context.
   * @throws Exception
   */
  @Test
  public void testListPathTracingContext() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Assume.assumeTrue("To work on only on non-HNS Blob endpoint",
        fs.getAbfsStore().getAbfsConfiguration().getPrefixMode()
            == PrefixMode.BLOB);
    TracingHeaderValidator validator = new TracingHeaderValidator(getConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.LISTSTATUS, true, 0);
    validator.setDisableValidation(true);
    fs.registerListener(validator);

    final AzureBlobFileSystem spiedFs = Mockito.spy(fs);
    final AzureBlobFileSystemStore spiedStore = Mockito.spy(fs.getAbfsStore());
    final AbfsClient spiedClient = Mockito.spy(fs.getAbfsClient());
    final TracingContext spiedTracingContext = Mockito.spy(
        new TracingContext(
            fs.getClientCorrelationId(), fs.getFileSystemId(),
            FSOperationType.LISTSTATUS, true, TracingHeaderFormat.ALL_ID_FORMAT,validator));

    Mockito.doReturn(spiedStore).when(spiedFs).getAbfsStore();
    spiedStore.setClient(spiedClient);
    spiedFs.setWorkingDirectory(new Path("/"));

    AbfsClientTestUtil.setMockAbfsRestOperationForListBlobOperation(spiedClient,
        (httpOperation) -> {
          BlobProperty blob = new BlobProperty();
          blob.setPath(new Path("/abc.txt"));
          blob.setName("abc.txt");
          BlobList blobListWithNextMarker = new BlobList();
          AbfsClientTestUtil.populateBlobListHelper(blobListWithNextMarker, blob, "nextMarker");
          BlobList blobListWithoutNextMarker = new BlobList();
          AbfsClientTestUtil.populateBlobListHelper(blobListWithNextMarker, blob, AbfsHttpConstants.EMPTY_STRING);
          when(httpOperation.getBlobList()).thenReturn(blobListWithNextMarker)
              .thenReturn(blobListWithoutNextMarker);

          Stubber stubber = Mockito.doThrow(
              new SocketTimeoutException(CONNECTION_TIMEOUT_JDK_MESSAGE));
          stubber.doNothing().when(httpOperation).processResponse(
              nullable(byte[].class), nullable(int.class), nullable(int.class));

          when(httpOperation.getStatusCode()).thenReturn(-1).thenReturn(HTTP_OK);
          return httpOperation;
        });

    List<FileStatus> fileStatuses = new ArrayList<>();
    spiedStore.listStatus(new Path("/"), "", fileStatuses, true, null, spiedTracingContext);

    // Assert that the tracing context passed initially, was used only for the
    // first paginated call. That called failed with CT once and then passed
    Mockito.verify(spiedTracingContext, times(1)).constructHeader(any(), eq(null));
    Mockito.verify(spiedTracingContext, times(1)).constructHeader(any(), eq(CONNECTION_TIMEOUT_ABBREVIATION));
    Mockito.verify(spiedTracingContext, times(2)).constructHeader(any(), any());

    // Assert that there were more than one (2) paginated ListBlob calls made
    Mockito.verify(spiedClient, times(2)).getListBlobs(any(), any(), any(), any(), any());
  }

  /**
   * Creates a file, verifies that listStatus returns it,
   * even while the file is still open for writing.
   */
  @Test
  public void testListFileVsListDir() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("/testFile");
    try(FSDataOutputStream ignored = fs.create(path)) {
      FileStatus[] testFiles = fs.listStatus(path);
      assertEquals("length of test files", 1, testFiles.length);
      FileStatus status = testFiles[0];
      assertIsFileReference(status);
    }
  }

  @Test
  public void testListFileVsListDir2() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("/testFolder"));
    fs.mkdirs(new Path("/testFolder/testFolder2"));
    fs.mkdirs(new Path("/testFolder/testFolder2/testFolder3"));
    Path testFile0Path = new Path("/testFolder/testFolder2/testFolder3/testFile");
    ContractTestUtils.touch(fs, testFile0Path);

    FileStatus[] testFiles = fs.listStatus(testFile0Path);
    assertEquals("Wrong listing size of file " + testFile0Path,
        1, testFiles.length);
    FileStatus file0 = testFiles[0];
    assertEquals("Wrong path for " + file0,
        new Path(getTestUrl(), "/testFolder/testFolder2/testFolder3/testFile"),
        file0.getPath());
    assertIsFileReference(file0);
  }

  @Test(expected = FileNotFoundException.class)
  public void testListNonExistentDir() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.listStatus(new Path("/testFile/"));
  }

  @Test
  public void testListFiles() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testDir = new Path("/test");
    fs.mkdirs(testDir);

    FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
    assertEquals(1, fileStatuses.length);

    fs.mkdirs(new Path("/test/sub"));
    fileStatuses = fs.listStatus(testDir);
    assertEquals(1, fileStatuses.length);
    assertEquals("sub", fileStatuses[0].getPath().getName());
    assertIsDirectoryReference(fileStatuses[0]);
    Path childF = fs.makeQualified(new Path("/test/f"));
    touch(childF);
    fileStatuses = fs.listStatus(testDir);
    assertEquals(2, fileStatuses.length);
    final FileStatus childStatus = fileStatuses[0];
    assertEquals(childF, childStatus.getPath());
    assertEquals("f", childStatus.getPath().getName());
    assertIsFileReference(childStatus);
    assertEquals(0, childStatus.getLen());
    final FileStatus status1 = fileStatuses[1];
    assertEquals("sub", status1.getPath().getName());
    assertIsDirectoryReference(status1);
    // look at the child through getFileStatus
    LocatedFileStatus locatedChildStatus = fs.listFiles(childF, false).next();
    assertIsFileReference(locatedChildStatus);

    fs.delete(testDir, true);
    intercept(FileNotFoundException.class,
        () -> fs.listFiles(childF, false).next());

    // do some final checks on the status (failing due to version checks)
    assertEquals("Path mismatch of " + locatedChildStatus,
        childF, locatedChildStatus.getPath());
    assertEquals("locatedstatus.equals(status)",
        locatedChildStatus, childStatus);
    assertEquals("status.equals(locatedstatus)",
        childStatus, locatedChildStatus);
  }

  private void assertIsDirectoryReference(FileStatus status) {
    assertTrue("Not a directory: " + status, status.isDirectory());
    assertFalse("Not a directory: " + status, status.isFile());
    assertEquals(0, status.getLen());
  }

  private void assertIsFileReference(FileStatus status) {
    assertFalse("Not a file: " + status, status.isDirectory());
    assertTrue("Not a file: " + status, status.isFile());
  }

  @Test
  public void testMkdirTrailingPeriodDirName() throws IOException {
    boolean exceptionThrown = false;
    final AzureBlobFileSystem fs = getFileSystem();

    Path nontrailingPeriodDir = path("testTrailingDir/dir");
    Path trailingPeriodDir = path("testTrailingDir/dir.");

    assertMkdirs(fs, nontrailingPeriodDir);

    try {
      fs.mkdirs(trailingPeriodDir);
    }
    catch(IllegalArgumentException e) {
      exceptionThrown = true;
    }
    assertTrue("Attempt to create file that ended with a dot should"
        + " throw IllegalArgumentException", exceptionThrown);
  }

  @Test
  public void testCreateTrailingPeriodFileName() throws IOException {
    boolean exceptionThrown = false;
    final AzureBlobFileSystem fs = getFileSystem();

    Path trailingPeriodFile = path("testTrailingDir/file.");
    Path nontrailingPeriodFile = path("testTrailingDir/file");

    createFile(fs, nontrailingPeriodFile, false, new byte[0]);
    assertPathExists(fs, "Trailing period file does not exist",
        nontrailingPeriodFile);

    try {
      createFile(fs, trailingPeriodFile, false, new byte[0]);
    }
    catch(IllegalArgumentException e) {
      exceptionThrown = true;
    }
    assertTrue("Attempt to create file that ended with a dot should"
        + " throw IllegalArgumentException", exceptionThrown);
  }

  @Test
  public void testRenameTrailingPeriodFile() throws IOException {
    boolean exceptionThrown = false;
    final AzureBlobFileSystem fs = getFileSystem();

    Path nonTrailingPeriodFile = path("testTrailingDir/file");
    Path trailingPeriodFile = path("testTrailingDir/file.");

    createFile(fs, nonTrailingPeriodFile, false, new byte[0]);
    try {
    rename(fs, nonTrailingPeriodFile, trailingPeriodFile);
    }
    catch(IllegalArgumentException e) {
      exceptionThrown = true;
    }
    assertTrue("Attempt to create file that ended with a dot should"
        + " throw IllegalArgumentException", exceptionThrown);
  }

  @Test
  public void testListStatusImplicitExplicitChildren() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.setWorkingDirectory(new Path("/"));
    Path root = new Path("/");
    AzcopyHelper azcopyHelper = new AzcopyHelper(
        getAccountName(),
        getFileSystemName(),
        getRawConfiguration(),
        fs.getAbfsStore().getPrefixMode()
    );

    // Create an implicit directory under root
    Path dir1 = new Path("a");
    azcopyHelper.createFolderUsingAzcopy(fs.makeQualified(dir1).toUri().getPath().substring(1));
    assertTrue("Path is implicit.",
        BlobDirectoryStateHelper.isImplicitDirectory(dir1, fs));

    // Assert that implicit directory is returned
    FileStatus[] fileStatuses = fs.listStatus(root);
    Assertions.assertThat(fileStatuses.length).isEqualTo(1);
    assertImplicitDirectoryFileStatus(fileStatuses[0], fs.makeQualified(dir1));

    // Create a marker blob for the directory.
    fs.mkdirs(dir1);

    // Assert that only one entry of explicit directory is returned
    fileStatuses = fs.listStatus(root);
    Assertions.assertThat(fileStatuses.length).isEqualTo(1);
    assertExplicitDirectoryFileStatus(fileStatuses[0], fs.makeQualified(dir1));

    // Create a file under root
    Path file1 = new Path("b");
    fs.create(file1);

    // Assert that two entries are returned in alphabetic order.
    fileStatuses = fs.listStatus(root);
    Assertions.assertThat(fileStatuses.length).isEqualTo(2);
    assertExplicitDirectoryFileStatus(fileStatuses[0], fs.makeQualified(dir1));
    assertFileFileStatus(fileStatuses[1], fs.makeQualified(file1));

    // Create another implicit directory under root.
    Path dir2 = new Path("c");
    azcopyHelper.createFolderUsingAzcopy(fs.makeQualified(dir2).toUri().getPath().substring(1));
    assertTrue("Path is implicit.",
        BlobDirectoryStateHelper.isImplicitDirectory(dir2, fs));

    // Assert that three entries are returned in alphabetic order.
    fileStatuses = fs.listStatus(root);
    Assertions.assertThat(fileStatuses.length).isEqualTo(3);
    assertExplicitDirectoryFileStatus(fileStatuses[0], fs.makeQualified(dir1));
    assertFileFileStatus(fileStatuses[1], fs.makeQualified(file1));
    assertImplicitDirectoryFileStatus(fileStatuses[2], fs.makeQualified(dir2));
  }

  @Test
  public void testListStatusOnNonExistingPath() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testPath = new Path("a/b");

    intercept(FileNotFoundException.class,
        () -> fs.listFiles(testPath, false).next());
  }

  @Test
  public void testListStatusOnImplicitDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    AzcopyHelper azcopyHelper = new AzcopyHelper(
        getAccountName(),
        getFileSystemName(),
        getRawConfiguration(),
        fs.getAbfsStore().getPrefixMode()
    );

    // Create an implicit directory with another implicit directory inside
    Path testPath = new Path("testDir");
    Path childPath = new Path("testDir/azcopy");
    azcopyHelper.createFolderUsingAzcopy(
        fs.makeQualified(testPath).toUri().getPath().substring(1));
    assertTrue("Path is implicit.",
        BlobDirectoryStateHelper.isImplicitDirectory(testPath, fs));

    // Assert that one entry is returned as implicit child.
    FileStatus[] fileStatuses = fs.listStatus(testPath);
    Assertions.assertThat(fileStatuses.length).isEqualTo(1);
    assertImplicitDirectoryFileStatus(fileStatuses[0], fs.makeQualified(childPath));
  }

  @Test
  public void testListStatusOnExplicitDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    AzcopyHelper azcopyHelper = new AzcopyHelper(
        getAccountName(),
        getFileSystemName(),
        getRawConfiguration(),
        fs.getAbfsStore().getPrefixMode()
    );

    // Create an explicit directory with all kind of children.
    Path testPath = new Path("testDir");
    Path explicitChild = new Path ("testDir/a/subdir");
    Path fileChild = new Path ("testDir/b");
    Path implicitChild = new Path ("testDir/c");
    fs.mkdirs(explicitChild);
    fs.create(fileChild);
    azcopyHelper.createFolderUsingAzcopy(
        fs.makeQualified(implicitChild).toUri().getPath().substring(1));

    assertTrue("Test path is explicit",
        BlobDirectoryStateHelper.isExplicitDirectory(testPath, fs));
    assertTrue("explicitChild Path is explicit",
        BlobDirectoryStateHelper.isExplicitDirectory(explicitChild, fs));
    assertTrue("implicitChild Path is implicit.",
        BlobDirectoryStateHelper.isImplicitDirectory(implicitChild, fs));

    // Assert that three entry is returned.
    FileStatus[] fileStatuses = fs.listStatus(testPath);
    Assertions.assertThat(fileStatuses.length).isEqualTo(3);
    assertExplicitDirectoryFileStatus(fileStatuses[0], fs.makeQualified(explicitChild.getParent()));
    assertFileFileStatus(fileStatuses[1], fs.makeQualified(fileChild));
    assertImplicitDirectoryFileStatus(fileStatuses[2], fs.makeQualified(implicitChild));
  }

  @Test
  public void testListStatusImplicitExplicitWithDotInFolderName() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    AzcopyHelper azcopyHelper = new AzcopyHelper(
        getAccountName(),
        getFileSystemName(),
        getRawConfiguration(),
        fs.getAbfsStore().getPrefixMode()
    );

    // Create two implicit folder with same prefix one having dot.
    Path testPath = new Path("Try1/DirA");
    Path implicitChild1 = new Path ("Try1/DirA/DirB/file.txt");
    Path implicitChild2 = new Path ("Try1/DirA/DirB.bak/file.txt");
    Path explicitChild = new Path ("Try1/DirA/DirB");

    azcopyHelper.createFolderUsingAzcopy(
        fs.makeQualified(implicitChild1).toUri().getPath().substring(1));
    azcopyHelper.createFolderUsingAzcopy(
        fs.makeQualified(implicitChild2).toUri().getPath().substring(1));
    fs.mkdirs(explicitChild);

    assertTrue("Test path is explicit",
        BlobDirectoryStateHelper.isExplicitDirectory(testPath, fs));
    assertTrue("explicitChild Path is explicit",
        BlobDirectoryStateHelper.isExplicitDirectory(explicitChild, fs));
    assertTrue("implicitChild2 Path is implicit.",
        BlobDirectoryStateHelper.isImplicitDirectory(implicitChild2, fs));

    // Assert that only 2 entry is returned.
    FileStatus[] fileStatuses = fs.listStatus(testPath);
    Assertions.assertThat(fileStatuses.length).isEqualTo(2);
    assertExplicitDirectoryFileStatus(fileStatuses[0], fs.makeQualified(explicitChild));
    assertImplicitDirectoryFileStatus(fileStatuses[1], fs.makeQualified(implicitChild2.getParent()));
  }

  @Test
  public void testListStatusOnEmptyDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testPath = new Path("testPath");
    fs.mkdirs(testPath);
    FileStatus[] fileStatuses = fs.listStatus(testPath);
    Assertions.assertThat(fileStatuses.length).isEqualTo(0);
  }

  @Test
  public void testListStatusUsesGfs() throws Exception {
    Assume.assumeTrue(getFileSystem().getAbfsStore().getPrefixMode() == PrefixMode.BLOB);
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    AbfsClient client = Mockito.spy(store.getClient());
    Mockito.doReturn(store).when(fs).getAbfsStore();
    Mockito.doReturn(client).when(store).getClient();
    store.setClient(client);

    intercept(FileNotFoundException.class, () ->
            fs.listStatus(new Path("a/b")));

    Mockito.verify(store, times(1)).getPathProperty(Mockito.any(Path.class),
            Mockito.any(TracingContext.class), Mockito.any(boolean.class));

    // getListBlobs from within store should not be called - as this is invoked from getFileStatus
    Mockito.verify(store, times(0)).getListBlobs(Mockito.any(Path.class),
            Mockito.any(String.class), Mockito.any(String.class), Mockito.any(TracingContext.class),
            Mockito.any(Integer.class), Mockito.any(Boolean.class));

    // getListBlobs from client should be called once - the call for listStatus that would fail
    // as this blob is actually non-existent
    Mockito.verify(client, times(1)).getListBlobs(Mockito.nullable(String.class),
            Mockito.nullable(String.class), Mockito.nullable(String.class),
            Mockito.any(Integer.class), Mockito.any(TracingContext.class));
  }

  private void assertFileFileStatus(final FileStatus fileStatus,
      final Path qualifiedPath) {
    Assertions.assertThat(fileStatus.getPath()).isEqualTo(qualifiedPath);
    Assertions.assertThat(fileStatus.isDirectory()).isEqualTo(false);
    Assertions.assertThat(fileStatus.isFile()).isEqualTo(true);
    Assertions.assertThat(fileStatus.getModificationTime()).isNotEqualTo(0);
  }

  private void assertImplicitDirectoryFileStatus(final FileStatus fileStatus,
      final Path qualifiedPath) {
    assertDirectoryFileStatus(fileStatus, qualifiedPath);
    Assertions.assertThat(fileStatus.getModificationTime()).isEqualTo(0);
  }

  private void assertExplicitDirectoryFileStatus(final FileStatus fileStatus,
      final Path qualifiedPath) {
    assertDirectoryFileStatus(fileStatus, qualifiedPath);
    Assertions.assertThat(fileStatus.getModificationTime()).isNotEqualTo(0);
  }

  private void assertDirectoryFileStatus(final FileStatus fileStatus,
      final Path qualifiedPath) {
    Assertions.assertThat(fileStatus.getPath()).isEqualTo(qualifiedPath);
    Assertions.assertThat(fileStatus.isDirectory()).isEqualTo(true);
    Assertions.assertThat(fileStatus.isFile()).isEqualTo(false);
    Assertions.assertThat(fileStatus.getLen()).isEqualTo(0);
  }

  @Test
  public void testListStatusNotTriesToRenameResumeForNonAtomicDir()
      throws Exception {
    Assume.assumeTrue(getPrefixMode(getFileSystem()) == PrefixMode.BLOB);
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();
    Mockito.doReturn(new FileStatus[1])
        .when(store)
        .listStatus(Mockito.any(Path.class), Mockito.any(
            TracingContext.class));
    fs.listStatus(new Path("/testDir/"));
    Mockito.verify(store, Mockito.times(0))
        .getRenamePendingFileStatus(Mockito.any(FileStatus[].class));
  }

  @Test
  public void testListStatusTriesToRenameResumeForAtomicDir() throws Exception {
    Assume.assumeTrue(getPrefixMode(getFileSystem()) == PrefixMode.BLOB);
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();
    Mockito.doReturn(new FileStatus[0])
        .when(store)
        .listStatus(Mockito.any(Path.class), Mockito.any(
            TracingContext.class));
    fs.listStatus(new Path("/hbase/"));
    Mockito.verify(store, Mockito.times(1))
        .getRenamePendingFileStatus(Mockito.any(FileStatus[].class));
  }

  @Test
  public void testListStatusTriesToRenameResumeForAbsoluteAtomicDir()
      throws Exception {
    Assume.assumeTrue(getPrefixMode(getFileSystem()) == PrefixMode.BLOB);
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();
    Mockito.doReturn(new FileStatus[0])
        .when(store)
        .listStatus(Mockito.any(Path.class), Mockito.any(
            TracingContext.class));
    fs.listStatus(new Path("/hbase"));
    Mockito.verify(store, Mockito.times(1))
        .getRenamePendingFileStatus(Mockito.any(FileStatus[].class));
  }

  @Test
  public void testListStatusReturnStatusWithPathWithSameUriGivenInConfig()
      throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    String accountName = getAccountName();
    Boolean isAccountNameInDfs = accountName.contains(ABFS_DNS_PREFIX);
    String dnsAssertion;
    if (isAccountNameInDfs) {
      dnsAssertion = ABFS_DNS_PREFIX;
    } else {
      dnsAssertion = WASB_DNS_PREFIX;
    }

    final Path path = new Path("/testDir/file");
    fs.create(path);
    assertListStatusPath(fs, accountName, dnsAssertion, path);

    final Configuration configuration;
    if (isAccountNameInDfs) {
      configuration = new Configuration(getRawConfiguration());
      configuration.set(FS_DEFAULT_NAME_KEY,
          configuration.get(FS_DEFAULT_NAME_KEY)
              .replace(ABFS_DNS_PREFIX, WASB_DNS_PREFIX));
      dnsAssertion = WASB_DNS_PREFIX;

    } else {
      configuration = new Configuration(getRawConfiguration());
      configuration.set(FS_DEFAULT_NAME_KEY,
          configuration.get(FS_DEFAULT_NAME_KEY)
              .replace(WASB_DNS_PREFIX, ABFS_DNS_PREFIX));
      dnsAssertion = ABFS_DNS_PREFIX;
    }
    fs = (AzureBlobFileSystem) FileSystem.newInstance(configuration);
    assertListStatusPath(fs, accountName, dnsAssertion, path);
  }

  private void assertListStatusPath(final AzureBlobFileSystem fs,
      final String accountName,
      final String dnsAssertion,
      final Path path) throws IOException {
    FileStatus[] fileStatuses = fs.listStatus(new Path(
        "abfs://" + fs.getAbfsClient().getFileSystem() + "@" + accountName
            + path.toUri().getPath()));
    Assertions.assertThat(fileStatuses[0].getPath().toString())
        .contains(dnsAssertion);

    fileStatuses = fs.listStatus(new Path(
        "abfs://" + fs.getAbfsClient().getFileSystem() + "@" + accountName
            + path.getParent().toUri().getPath()));
    Assertions.assertThat(fileStatuses[0].getPath().toString())
        .contains(dnsAssertion);
  }
}
