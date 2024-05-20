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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.runner.Version;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientTestUtil;
import org.apache.hadoop.fs.azurebfs.services.AbfsDfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsLease;
import org.apache.hadoop.fs.azurebfs.services.BlobRenameHandler;
import org.apache.hadoop.fs.azurebfs.services.PathInformation;
import org.apache.hadoop.fs.azurebfs.services.RenameAtomicity;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.statistics.IOStatisticAssertions;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.functional.FunctionRaisingIOE;

import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.RENAME_PATH_ATTEMPTS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;
import static org.apache.hadoop.fs.azurebfs.services.RenameAtomicity.SUFFIX;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertMkdirs;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathDoesNotExist;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertRenameOutcome;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;

/**
 * Test rename operation.
 */
public class ITestAzureBlobFileSystemRename extends
    AbstractAbfsIntegrationTest {

  public ITestAzureBlobFileSystemRename() throws Exception {
    super();
  }

  @Test
  public void testEnsureFileIsRenamed() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path src = path("testEnsureFileIsRenamed-src");
    touch(src);
    Path dest = path("testEnsureFileIsRenamed-dest");
    fs.delete(dest, true);
    assertRenameOutcome(fs, src, dest, true);

    assertIsFile(fs, dest);
    assertPathDoesNotExist(fs, "expected renamed", src);
  }

  @Test
  public void testRenameWithPreExistingDestination() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path src = path("renameSrc");
    touch(src);
    Path dest = path("renameDest");
    touch(dest);
    assertRenameOutcome(fs, src, dest, false);
  }

  @Test
  public void testRenameFileUnderDir() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path sourceDir = path("/testSrc");
    assertMkdirs(fs, sourceDir);
    String filename = "file1";
    Path file1 = new Path(sourceDir, filename);
    touch(file1);

    Path destDir = path("/testDst");
    assertRenameOutcome(fs, sourceDir, destDir, true);
    FileStatus[] fileStatus = fs.listStatus(destDir);
    assertNotNull("Null file status", fileStatus);
    FileStatus status = fileStatus[0];
    assertEquals("Wrong filename in " + status,
        filename, status.getPath().getName());
  }

  @Test
  public void testRenameDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testDir = path("testDir");
    fs.mkdirs(testDir);
    Path test1 = new Path(testDir + "/test1");
    fs.mkdirs(test1);
    fs.mkdirs(new Path(testDir + "/test1/test2"));
    fs.mkdirs(new Path(testDir + "/test1/test2/test3"));

    assertRenameOutcome(fs, test1,
        new Path(testDir + "/test10"), true);
    assertPathDoesNotExist(fs, "rename source dir", test1);
  }

  @Test
  public void testRenameFirstLevelDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final List<Future<Void>> tasks = new ArrayList<>();

    ExecutorService es = Executors.newFixedThreadPool(10);
    Path source = path("/test");
    for (int i = 0; i < 1000; i++) {
      final Path fileName = new Path(source + "/" + i);
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
    Path dest = path("/renamedDir");
    assertRenameOutcome(fs, source, dest, true);

    FileStatus[] files = fs.listStatus(dest);
    assertEquals("Wrong number of files in listing", 1000, files.length);
    assertPathDoesNotExist(fs, "rename source dir", source);
  }

  @Test
  public void testRenameRoot() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assertRenameOutcome(fs,
        new Path("/"),
        new Path("/testRenameRoot"),
        false);
    assertRenameOutcome(fs,
        new Path(fs.getUri().toString() + "/"),
        new Path(fs.getUri().toString() + "/s"),
        false);
  }

  @Test
  public void testPosixRenameDirectory() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path testDir2 = path("testDir2");
    fs.mkdirs(new Path(testDir2 + "/test1/test2/test3"));
    fs.mkdirs(new Path(testDir2 + "/test4"));
    Assert.assertTrue(fs.rename(new Path(testDir2 + "/test1/test2/test3"), new Path(testDir2 + "/test4")));
    assertPathExists(fs, "This path should exist", testDir2);
    assertPathExists(fs, "This path should exist",
        new Path(testDir2 + "/test1/test2"));
    assertPathExists(fs, "This path should exist",
        new Path(testDir2 + "/test4"));
    assertPathExists(fs, "This path should exist",
        new Path(testDir2 + "/test4/test3"));
    assertPathDoesNotExist(fs, "This path should not exist",
        new Path(testDir2 + "/test1/test2/test3"));
  }

  @Test
  public void testRenameWithNoDestinationParentDir() throws Exception {
    describe("Verifying the expected behaviour of ABFS rename when "
        + "destination parent Dir doesn't exist.");

    final AzureBlobFileSystem fs = getFileSystem();
    Path sourcePath = path(getMethodName());
    Path destPath = new Path("falseParent", "someChildFile");

    byte[] data = dataset(1024, 'a', 'z');
    writeDataset(fs, sourcePath, data, data.length, 1024, true);

    // Verify that renaming on a destination with no parent dir wasn't
    // successful.
    assertFalse("Rename result expected to be false with no Parent dir",
        fs.rename(sourcePath, destPath));

    // Verify that metadata was in an incomplete state after the rename
    // failure, and we retired the rename once more.
    IOStatistics ioStatistics = fs.getIOStatistics();
    AbfsClient client = fs.getAbfsStore().getClient();
    IOStatisticAssertions.assertThatStatisticCounter(ioStatistics,
        RENAME_PATH_ATTEMPTS.getStatName())
        .describedAs("For Dfs endpoint: There should be 2 rename "
            + "attempts if metadata incomplete state failure is hit."
            + "For Blob endpoint: There would be only one rename attempt which "
            + "would have a failed precheck.")
        .isEqualTo(client instanceof AbfsDfsClient ? 2 : 1);
  }

  @Test
  public void testRenameToRoot() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("/src1/src2"));
    Assert.assertTrue(fs.rename(new Path("/src1/src2"), new Path("/")));
    Assert.assertTrue(fs.exists(new Path("/src2")));
  }

  @Test
  public void testRenameNotFoundBlobToEmptyRoot() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Assert.assertFalse(fs.rename(new Path("/file"), new Path("/")));
  }

  private void assumeNonHnsAccountBlobEndpoint(final AzureBlobFileSystem fs) {
    Assumptions.assumeThat(fs.getAbfsStore().getClient())
        .describedAs("Client has to be of type AbfsBlobClient")
        .isInstanceOf(AbfsBlobClient.class);
  }

  @Test(expected = IOException.class)
  public void testRenameBlobToDstWithColonInPath() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    fs.create(new Path("/src"));
    fs.rename(new Path("/src"), new Path("/dst:file"));
  }

  @Test
  public void testRenameBlobInSameDirectoryWithNoMarker() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    AbfsBlobClient client = (AbfsBlobClient) fs.getAbfsStore().getClient();
    fs.create(new Path("/srcDir/dir/file"));
    client.deleteBlobPath(new Path("/srcDir/dir"), null,
        getTestTracingContext(fs, true));
    Assert.assertTrue(fs.rename(new Path("/srcDir/dir"), new Path("/srcDir")));
  }

  /**
   * <pre>
   * Test to check behaviour of rename API if the destination directory is already
   * there. The HNS call and the one for Blob endpoint should have same behaviour.
   *
   * /testDir2/test1/test2/test3 contains (/file)
   * There is another path that exists: /testDir2/test4/test3
   * On rename(/testDir2/test1/test2/test3, /testDir2/test4).
   * </pre>
   *
   * Expectation for HNS / Blob endpoint:<ol>
   * <li>Rename should fail</li>
   * <li>No file should be transferred to destination directory</li>
   * </ol>
   */
  @Test
  public void testPosixRenameDirectoryWhereDirectoryAlreadyThereOnDestination()
      throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.mkdirs(new Path("testDir2/test1/test2/test3"));
    fs.create(new Path("testDir2/test1/test2/test3/file"));
    fs.mkdirs(new Path("testDir2/test4/test3"));
    assertTrue(fs.exists(new Path("testDir2/test1/test2/test3/file")));
    Assert.assertFalse(fs.rename(new Path("testDir2/test1/test2/test3"),
        new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2")));
    assertTrue(fs.exists(new Path("testDir2/test1/test2")));
    assertTrue(fs.exists(new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2/test1/test2/test3")));
    if (getIsNamespaceEnabled(fs)
        || fs.getAbfsClient() instanceof AbfsBlobClient) {
      assertFalse(fs.exists(new Path("testDir2/test4/test3/file")));
      assertTrue(fs.exists(new Path("testDir2/test1/test2/test3/file")));
    } else {
      assertTrue(fs.exists(new Path("testDir2/test4/test3/file")));
      assertFalse(fs.exists(new Path("testDir2/test1/test2/test3/file")));
    }
  }

  @Test
  public void testPosixRenameDirectoryWherePartAlreadyThereOnDestination()
      throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.mkdirs(new Path("testDir2/test1/test2/test3"));
    fs.create(new Path("testDir2/test1/test2/test3/file"));
    fs.create(new Path("testDir2/test1/test2/test3/file1"));
    fs.mkdirs(new Path("testDir2/test4/"));
    fs.create(new Path("testDir2/test4/file1"));
    byte[] etag = fs.getXAttr(new Path("testDir2/test4/file1"), "ETag");
    assertTrue(fs.exists(new Path("testDir2/test1/test2/test3/file")));
    assertTrue(fs.exists(new Path("testDir2/test1/test2/test3/file1")));
    Assert.assertTrue(fs.rename(new Path("testDir2/test1/test2/test3"),
        new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2")));
    assertTrue(fs.exists(new Path("testDir2/test1/test2")));
    assertTrue(fs.exists(new Path("testDir2/test4")));
    assertFalse(fs.exists(new Path("testDir2/test1/test2/test3")));


    assertFalse(fs.exists(new Path("testDir2/test4/file")));
    assertTrue(fs.exists(new Path("testDir2/test4/file1")));
    assertTrue(fs.exists(new Path("testDir2/test4/test3/file")));
    assertTrue(fs.exists(new Path("testDir2/test4/test3/file1")));
    assertTrue(fs.exists(new Path("testDir2/test4/file1")));
    assertFalse(fs.exists(new Path("testDir2/test1/test2/test3/file")));
    assertFalse(fs.exists(new Path("testDir2/test1/test2/test3/file1")));
  }

  /**
   * Test that after completing rename for a directory which is enabled for
   * AtomicRename, the RenamePending JSON file is deleted.
   */
  @Test
  public void testRenamePendingJsonIsRemovedPostSuccessfulRename()
      throws Exception {
    final AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fs);
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);

    assumeNonHnsAccountBlobEndpoint(fs);
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path("hbase/test1/test2/test3"));
    fs.create(new Path("hbase/test1/test2/test3/file"));
    fs.create(new Path("hbase/test1/test2/test3/file1"));
    fs.mkdirs(new Path("hbase/test4/"));
    fs.create(new Path("hbase/test4/file1"));
    final Integer[] correctDeletePathCount = new Integer[1];
    correctDeletePathCount[0] = 0;

    Mockito.doAnswer(answer -> {
          final String correctDeletePath = "/hbase/test1/test2/test3" + SUFFIX;
          if (correctDeletePath.equals(
              ((Path) answer.getArgument(0)).toUri().getPath())) {
            correctDeletePathCount[0] = 1;
          }
          return null;
        })
        .when(client)
        .deleteBlobPath(Mockito.any(Path.class), Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));
    assertTrue(fs.rename(new Path("hbase/test1/test2/test3"),
        new Path("hbase/test4")));
    assertTrue("RenamePendingJson should be deleted",
        correctDeletePathCount[0] == 1);
  }

  private AbfsClient addSpyHooksOnClient(final AzureBlobFileSystem fs) {
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();
    AbfsClient client = Mockito.spy(store.getClient());
    Mockito.doReturn(client).when(store).getClient();
    return client;
  }

  /**
   * Test for a directory in /hbase directory. To simulate the crash of process,
   * test will throw an exception with 403 on a copy of one of the blob.<br>
   * ListStatus API will be called on the directory. Expectation is that the ListStatus
   * API of {@link AzureBlobFileSystem} should recover the paused rename.
   */
  @Test
  public void testHBaseHandlingForFailedRenameWithListRecovery()
      throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fs);
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);

    String srcPath = "hbase/test1/test2";
    final String failedCopyPath = srcPath + "/test3/file1";
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path(srcPath));
    fs.mkdirs(new Path(srcPath, "test3"));
    fs.create(new Path(srcPath + "/test3/file"));
    fs.create(new Path(failedCopyPath));
    fs.mkdirs(new Path("hbase/test4/"));
    fs.create(new Path("hbase/test4/file1"));

    crashRenameAndRecover(fs, client, srcPath, failedCopyPath, (abfsFs) -> {
      abfsFs.listStatus(new Path(srcPath).getParent());
      return null;
    });
  }

  /**
   * Test for a directory in /hbase directory. To simulate the crash of process,
   * test will throw an exception with 403 on a copy of one of the blob. The
   * source directory is a nested directory.<br>
   * GetFileStatus API will be called on the directory. Expectation is that the
   * GetFileStatus API of {@link AzureBlobFileSystem} should recover the paused
   * rename.
   */
  @Test
  public void testHBaseHandlingForFailedRenameWithGetFileStatusRecovery()
      throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fs);
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);

    String srcPath = "hbase/test1/test2";
    final String failedCopyPath = srcPath + "/test3/file1";
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path(srcPath));
    fs.mkdirs(new Path(srcPath, "test3"));
    fs.create(new Path(srcPath + "/test3/file"));
    fs.create(new Path(failedCopyPath));
    fs.mkdirs(new Path("hbase/test4/"));
    fs.create(new Path("hbase/test4/file1"));

    crashRenameAndRecover(fs, client, srcPath, failedCopyPath, (abfsFs) -> {
      abfsFs.getFileStatus(new Path(srcPath));
      return null;
    });
  }

  private void crashRenameAndRecover(final AzureBlobFileSystem fs,
      AbfsBlobClient client,
      final String srcPath,
      final String failedCopyPath,
      final FunctionRaisingIOE<AzureBlobFileSystem, Void> recoveryCallable)
      throws Exception {
    BlobRenameHandler[] blobRenameHandlers = new BlobRenameHandler[1];
    AbfsClientTestUtil.mockGetRenameBlobHandler(client,
        blobRenameHandler -> {
          blobRenameHandlers[0] = blobRenameHandler;
          return null;
        });

    //Fail rename orchestration on path hbase/test1/test2/test3/file1
    Mockito.doAnswer(answer -> {
      Path path = answer.getArgument(0);
      if ((ROOT_PATH + failedCopyPath).equalsIgnoreCase(
          path.toUri().getPath())) {
        throw new AbfsRestOperationException(HTTP_FORBIDDEN, "", "",
            new Exception());
      }
      return answer.callRealMethod();
    }).when(client).copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
        Mockito.nullable(String.class),
        Mockito.any(TracingContext.class));

    LambdaTestUtils.intercept(AccessDeniedException.class, () -> {
      fs.rename(new Path(srcPath),
          new Path("hbase/test4"));
    });

    Assert.assertTrue(fs.exists(new Path(failedCopyPath)));
    Assert.assertFalse(fs.exists(new Path(
        failedCopyPath.replace("test1/test2/", "test4/test3/"))));

    //Release all the leases taken by atomic rename orchestration
    List<AbfsLease> leases = blobRenameHandlers[0].getLeases();
    for (AbfsLease lease : leases) {
      lease.free();
    }

    AzureBlobFileSystem fs2 = Mockito.spy(getFileSystem());
    fs2.setWorkingDirectory(new Path(ROOT_PATH));
    client = (AbfsBlobClient) addSpyHooksOnClient(fs2);
    int[] renameJsonDeleteCounter = new int[1];
    renameJsonDeleteCounter[0] = 0;
    Mockito.doAnswer(answer -> {
          if ((ROOT_PATH + srcPath + SUFFIX)
              .equalsIgnoreCase(((Path) answer.getArgument(0)).toUri().getPath())) {
            renameJsonDeleteCounter[0] = 1;
          }
          return answer.callRealMethod();
        })
        .when(client)
        .deleteBlobPath(Mockito.any(Path.class), Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));

    recoveryCallable.apply(fs2);
    Assertions.assertThat(renameJsonDeleteCounter[0])
        .describedAs("RenamePendingJson should be deleted")
        .isEqualTo(1);

    //List would complete the rename orchestration.
    assertFalse(fs2.exists(new Path("hbase/test1/test2")));
    assertFalse(fs2.exists(new Path("hbase/test1/test2/test3")));
    assertTrue(fs2.exists(new Path("hbase/test4/test2/test3")));
    assertFalse(fs2.exists(new Path("hbase/test1/test2/test3/file")));
    assertTrue(fs2.exists(new Path("hbase/test4/test2/test3/file")));
    assertFalse(fs2.exists(new Path("hbase/test1/test2/test3/file1")));
    assertTrue(fs2.exists(new Path("hbase/test4/test2/test3/file1")));
  }

  /**
   * Simulates a scenario where HMaster in Hbase starts up and executes listStatus
   * API on the directory that has to be renamed by some other executor-machine.
   * The scenario is that RenamePending JSON is created but before it could be
   * appended, it has been opened by the HMaster. The HMaster will delete it. The
   * machine doing rename would have to recreate the JSON file.
   * ref: <a href="https://issues.apache.org/jira/browse/HADOOP-12678">issue</a>
   */
  @Test
  public void testHbaseListStatusBeforeRenamePendingFileAppendedWithIngressOnBlob()
      throws Exception {
    final AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fs);
    fs.setWorkingDirectory(new Path(ROOT_PATH));
    testRenamePreRenameFailureResolution(fs);
    testAtomicityRedoInvalidFile(fs);
  }

  private void testRenamePreRenameFailureResolution(final AzureBlobFileSystem fs)
      throws Exception {
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);

    Path src = new Path("hbase/test1/test2");
    Path dest = new Path("hbase/test4");
    fs.mkdirs(src);
    fs.mkdirs(new Path(src, "test3"));

    Mockito.doAnswer(createFsCallbackAnswer -> {
      AzureBlobFileSystem.GetCreateCallback createCallback = Mockito.spy(
          (AzureBlobFileSystem.GetCreateCallback) createFsCallbackAnswer.callRealMethod());
      int[] counter = new int[1];
      counter[0] = 0;
      Mockito.doAnswer(createAnswer -> {
        Path path = createAnswer.getArgument(0);
        FSDataOutputStream stream = Mockito.spy(
            (FSDataOutputStream) createAnswer.callRealMethod());
        if (counter[0]++ == 0) {
          Mockito.doAnswer(closeAnswer -> {
            fs.delete(path, true);
            return closeAnswer.callRealMethod();
          }).when(stream).close();
        }
        return stream;
      }).when(createCallback).get(Mockito.any(Path.class));
      return createCallback;
    }).when(client).getCreateCallback();

    RenameAtomicity[] renameAtomicity = new RenameAtomicity[1];
    AbfsClientTestUtil.mockGetRenameBlobHandler(client, blobRenameHandler -> {
      Mockito.doAnswer(getRenameAtomicity -> {
            renameAtomicity[0] = Mockito.spy(
                (RenameAtomicity) getRenameAtomicity.callRealMethod());
            return renameAtomicity[0];
          })
          .when(blobRenameHandler)
          .getRenameAtomicity(Mockito.any(PathInformation.class));
      return null;
    });

    fs.rename(src, dest);
    Mockito.verify(renameAtomicity[0], Mockito.times(2)).preRename();
  }

  private void testAtomicityRedoInvalidFile(final AzureBlobFileSystem fs)
      throws Exception {
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);

    Path path = new Path("/hbase/test1/test2");
    fs.mkdirs(new Path(path, "test3"));
    Path renameJson = new Path(path.getParent(), path.getName() + SUFFIX);
    OutputStream os = fs.create(renameJson);
    os.write("{".getBytes(StandardCharsets.UTF_8));
    os.close();

    int[] renameJsonDeleteCounter = new int[1];
    renameJsonDeleteCounter[0] = 0;
    Mockito.doAnswer(deleteAnswer -> {
          Path ansPath = deleteAnswer.getArgument(0);
          if (renameJson.toUri()
              .getPath()
              .equalsIgnoreCase(ansPath.toUri().getPath())) {
            renameJsonDeleteCounter[0]++;
          }
          return deleteAnswer.callRealMethod();
        })
        .when(client)
        .deleteBlobPath(Mockito.any(Path.class), Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));

    RenameAtomicity renameAtomicity = Mockito.spy(
        new RenameAtomicity(renameJson,
            client.getCreateCallback(), client.getReadCallback(),
            getTestTracingContext(fs, true), true, null, client));
    renameAtomicity.redo();

    Assertions.assertThat(renameJsonDeleteCounter[0])
        .describedAs("RenamePendingJson should be deleted")
        .isEqualTo(1);
    Mockito.verify(client, Mockito.times(0)).copyBlob(Mockito.any(Path.class),
        Mockito.any(Path.class), Mockito.nullable(String.class),
        Mockito.any(TracingContext.class));
  }

  @Test
  public void testRenameJsonDeletedBeforeRenameAtomicityCanDelete()
      throws Exception {
    final AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fs);
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);

    fs.setWorkingDirectory(new Path(ROOT_PATH));

    Path path = new Path("/hbase/test1/test2");
    fs.mkdirs(new Path(path, "test3"));
    Path renameJson = new Path(path.getParent(), path.getName() + SUFFIX);
    OutputStream os = fs.create(renameJson);
    os.write("{}".getBytes(StandardCharsets.UTF_8));
    os.close();

    int[] renameJsonDeleteCounter = new int[1];
    renameJsonDeleteCounter[0] = 0;
    Mockito.doAnswer(deleteAnswer -> {
          Path ansPath = deleteAnswer.getArgument(0);
          if (renameJson.toUri()
              .getPath()
              .equalsIgnoreCase(ansPath.toUri().getPath())) {
            renameJsonDeleteCounter[0]++;
          }
          getFileSystem().delete(ansPath, true);
          return deleteAnswer.callRealMethod();
        })
        .when(client)
        .deleteBlobPath(Mockito.any(Path.class), Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));

    RenameAtomicity renameRedoAtomicity =
        new RenameAtomicity(renameJson,
            client.getCreateCallback(), client.getReadCallback(),
            getTestTracingContext(fs, true), true, null, client);
    renameRedoAtomicity.redo();
  }

  @Test
  public void testRenameCompleteBeforeRenameAtomicityRedo() throws Exception {
    final AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fs);
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);

    fs.setWorkingDirectory(new Path(ROOT_PATH));

    Path path = new Path("/hbase/test1/test2");
    fs.mkdirs(new Path(path, "test3"));
    Path renameJson = new Path(path.getParent(), path.getName() + SUFFIX);

    Mockito.doAnswer(readAnswer -> {
      AzureBlobFileSystem.GetReadCallback readCallback = Mockito.spy(
          (AzureBlobFileSystem.GetReadCallback) readAnswer.callRealMethod());

      Mockito.doAnswer(readCallbackAnswer -> {
        FSDataInputStream is = Mockito.spy(
            (FSDataInputStream) readCallbackAnswer.callRealMethod());
        Mockito.doAnswer(readFullyAnswer -> {
          readFullyAnswer.callRealMethod();
          fs.delete(path, true);
          return null;
        }).when(is).readFully(Mockito.anyLong(), Mockito.any(byte[].class));
        return is;
      }).when(readCallback).get(Mockito.any(Path.class));

      return readCallback;
    }).when(client).getReadCallback();

    /*
     * Create renameJson file.
     */
    AzureBlobFileSystemStore.VersionedFileStatus fileStatus
        = (AzureBlobFileSystemStore.VersionedFileStatus) fs.getFileStatus(path);
    RenameAtomicity renamePreAtomicity = new RenameAtomicity(path,
        new Path("/hbase/test4"), renameJson,
        client.getCreateCallback(), client.getReadCallback(),
        getTestTracingContext(fs, true), true, fileStatus.getEtag(), client);
    renamePreAtomicity.preRename();

    RenameAtomicity redoAtomicity =
        new RenameAtomicity(renameJson,
            client.getCreateCallback(), client.getReadCallback(),
            getTestTracingContext(fs, true), true, null, client);
    redoAtomicity.redo();
  }

  @Test
  public void testCopyBlobIdempotency() throws Exception {
    final AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fs);
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);

    fs.setWorkingDirectory(new Path(ROOT_PATH));

  }
}
