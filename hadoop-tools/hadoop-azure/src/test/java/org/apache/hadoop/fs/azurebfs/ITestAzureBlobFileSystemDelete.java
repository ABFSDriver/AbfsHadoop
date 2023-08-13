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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientTestUtil;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.ListBlobProducer;
import org.apache.hadoop.fs.azurebfs.services.ListBlobQueue;
import org.apache.hadoop.fs.azurebfs.services.PrefixMode;
import org.apache.hadoop.fs.azurebfs.services.ITestAbfsClient;
import org.apache.hadoop.fs.azurebfs.services.TestAbfsPerfTracker;
import org.apache.hadoop.fs.azurebfs.utils.TestMockHelpers;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.LambdaTestUtils;

import org.mockito.Mockito;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_MKDIRS_FALLBACK_TO_DFS;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_PRODUCER_QUEUE_MAX_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_REDIRECT_DELETE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_REDIRECT_RENAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_DELETE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_DELETE_CONSIDERED_IDEMPOTENT;
import static org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType.DeletePath;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertDeleted;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathDoesNotExist;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;


/**
 * Test delete operation.
 */
public class ITestAzureBlobFileSystemDelete extends
    AbstractAbfsIntegrationTest {

  private static final int REDUCED_RETRY_COUNT = 1;
  private static final int REDUCED_MAX_BACKOFF_INTERVALS_MS = 5000;

  public ITestAzureBlobFileSystemDelete() throws Exception {
    super();
  }

  @Test
  public void testDeleteRoot() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();

    fs.mkdirs(new Path("/testFolder0"));
    fs.mkdirs(new Path("/testFolder1"));
    fs.mkdirs(new Path("/testFolder2"));
    touch(new Path("/testFolder1/testfile"));
    touch(new Path("/testFolder1/testfile2"));
    touch(new Path("/testFolder1/testfile3"));

    Path root = new Path("/");
    FileStatus[] ls = fs.listStatus(root);
    assertEquals(3, ls.length);

    fs.delete(root, true);
    ls = fs.listStatus(root);
    assertEquals("listing size", 0, ls.length);
  }

  @Test()
  public void testOpenFileAfterDelete() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testfile = new Path("/testFile");
    touch(testfile);
    assertDeleted(fs, testfile, false);

    intercept(FileNotFoundException.class,
        () -> fs.open(testfile));
  }

  @Test
  public void testEnsureFileIsDeleted() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testfile = new Path("testfile");
    touch(testfile);
    assertDeleted(fs, testfile, false);
    assertPathDoesNotExist(fs, "deleted", testfile);
  }

  @Test
  public void testEnsureFileIsDeletedWithRedirection() throws Exception {

    // Set redirect to wasb delete as true and assert deletion.
    Configuration configuration = Mockito.spy(getRawConfiguration());
    configuration.setBoolean(FS_AZURE_REDIRECT_DELETE, true);
    AzureBlobFileSystem fs1 = (AzureBlobFileSystem) FileSystem.newInstance(configuration);

    Path testfile = makeQualified(new Path("testfile"));
    touch(testfile);
    assertDeleted(fs1, testfile, false);
    assertPathDoesNotExist(fs1, "deleted", testfile);
  }

  @Test
  public void testDeleteDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path dir = new Path("testfile");
    fs.mkdirs(dir);
    fs.mkdirs(new Path("testfile/test1"));
    fs.mkdirs(new Path("testfile/test1/test2"));

    assertDeleted(fs, dir, true);
    assertPathDoesNotExist(fs, "deleted", dir);
  }

  @Ignore
  @Test
  public void testDeleteFirstLevelDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final List<Future<Void>> tasks = new ArrayList<>();

    ExecutorService es = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 1000; i++) {
      final Path fileName = new Path("/test/" + i);
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
    Path dir = new Path("/test");
    fs.registerListener(new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.DELETE, false, 0));
    // first try a non-recursive delete, expect failure
    intercept(IOException.class,
        () -> fs.delete(dir, false));
    fs.registerListener(null);
    assertDeleted(fs, dir, true);
    assertPathDoesNotExist(fs, "deleted", dir);

  }

  @Test
  public void testDeleteIdempotency() throws Exception {
    Assume.assumeTrue(DEFAULT_DELETE_CONSIDERED_IDEMPOTENT);
    // Config to reduce the retry and maxBackoff time for test run
    AbfsConfiguration abfsConfig
        = TestAbfsConfigurationFieldsValidation.updateRetryConfigs(
        getConfiguration(),
        REDUCED_RETRY_COUNT, REDUCED_MAX_BACKOFF_INTERVALS_MS);

    final AzureBlobFileSystem fs = getFileSystem();
    AbfsClient abfsClient = fs.getAbfsStore().getClient();
    AbfsClient testClient = ITestAbfsClient.createTestClientFromCurrentContext(
        abfsClient,
        abfsConfig);

    // Mock instance of AbfsRestOperation
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    // Set retryCount to non-zero
    when(op.isARetriedRequest()).thenReturn(true);

    // Case 1: Mock instance of Http Operation response. This will return
    // HTTP:Not Found
    AbfsHttpOperation http404Op = mock(AbfsHttpOperation.class);
    when(http404Op.getStatusCode()).thenReturn(HTTP_NOT_FOUND);

    // Mock delete response to 404
    when(op.getResult()).thenReturn(http404Op);
    when(op.hasResult()).thenReturn(true);

    Assertions.assertThat(testClient.deleteIdempotencyCheckOp(op)
        .getResult()
        .getStatusCode())
        .describedAs(
            "Delete is considered idempotent by default and should return success.")
        .isEqualTo(HTTP_OK);

    // Case 2: Mock instance of Http Operation response. This will return
    // HTTP:Bad Request
    AbfsHttpOperation http400Op = mock(AbfsHttpOperation.class);
    when(http400Op.getStatusCode()).thenReturn(HTTP_BAD_REQUEST);

    // Mock delete response to 400
    when(op.getResult()).thenReturn(http400Op);
    when(op.hasResult()).thenReturn(true);

    Assertions.assertThat(testClient.deleteIdempotencyCheckOp(op)
        .getResult()
        .getStatusCode())
        .describedAs(
            "Idempotency check to happen only for HTTP 404 response.")
        .isEqualTo(HTTP_BAD_REQUEST);

  }

  @Test
  public void testDeleteIdempotencyTriggerHttp404() throws Exception {

    final AzureBlobFileSystem fs = getFileSystem();
    AbfsClient client = ITestAbfsClient.createTestClientFromCurrentContext(
        fs.getAbfsStore().getClient(),
        this.getConfiguration());

    // Case 1: Not a retried case should throw error back
    // Add asserts at AzureBlobFileSystemStore and AbfsClient levels
    intercept(AbfsRestOperationException.class,
        () -> fs.getAbfsStore().delete(
            new Path("/NonExistingPath"),
            false, getTestTracingContext(fs, false)));

    intercept(AbfsRestOperationException.class,
        () -> client.deletePath(
        "/NonExistingPath",
        false,
        null,
        getTestTracingContext(fs, true)));

    // mock idempotency check to mimic retried case
    AbfsClient mockClient = ITestAbfsClient.getMockAbfsClient(
        fs.getAbfsStore().getClient(),
        this.getConfiguration());
    AzureBlobFileSystemStore mockStore = mock(AzureBlobFileSystemStore.class);
    mockStore = TestMockHelpers.setClassField(AzureBlobFileSystemStore.class, mockStore,
        "client", mockClient);
    mockStore = TestMockHelpers.setClassField(AzureBlobFileSystemStore.class,
        mockStore,
        "abfsPerfTracker",
        TestAbfsPerfTracker.getAPerfTrackerInstance(this.getConfiguration()));
    doCallRealMethod().when(mockStore).delete(new Path("/NonExistingPath"),
        false, getTestTracingContext(fs, false));

    // Case 2: Mimic retried case
    // Idempotency check on Delete always returns success
    AbfsRestOperation idempotencyRetOp = ITestAbfsClient.getRestOp(
        DeletePath, mockClient, HTTP_METHOD_DELETE,
        ITestAbfsClient.getTestUrl(mockClient, "/NonExistingPath"),
        ITestAbfsClient.getTestRequestHeaders(mockClient));
    idempotencyRetOp.hardSetResult(HTTP_OK);

    doReturn(idempotencyRetOp).when(mockClient).deleteIdempotencyCheckOp(any());
    TracingContext tracingContext = getTestTracingContext(fs, false);
    when(mockClient.deletePath("/NonExistingPath", false, null, tracingContext))
        .thenCallRealMethod();

    Assertions.assertThat(mockClient.deletePath(
        "/NonExistingPath",
        false,
        null,
        tracingContext)
        .getResult()
        .getStatusCode())
        .describedAs("Idempotency check reports successful "
            + "delete. 200OK should be returned")
        .isEqualTo(idempotencyRetOp.getResult().getStatusCode());

    // Call from AzureBlobFileSystemStore should not fail either
    mockStore.delete(new Path("/NonExistingPath"), false, getTestTracingContext(fs, false));
  }

  @Test
  public void testDeleteImplicitDir() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Assume.assumeTrue(fs.getAbfsStore().getPrefixMode() == PrefixMode.BLOB);
    fs.mkdirs(new Path("/testDir/dir1"));
    fs.create(new Path("/testDir/dir1/file1"));
    fs.getAbfsClient().deleteBlobPath(new Path("/testDir/dir1"),
        null, getTestTracingContext(fs, true));

    fs.delete(new Path("/testDir/dir1"), true);

    Assert.assertTrue(!fs.exists(new Path("/testDir/dir1")));
    Assert.assertTrue(!fs.exists(new Path("/testDir/dir1/file1")));
  }

  @Test
  public void testDeleteImplicitDirWithSingleListResults() throws Exception {
    AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(
        getRawConfiguration());
    Assume.assumeTrue(fs.getAbfsStore().getPrefixMode() == PrefixMode.BLOB);
    AbfsClient client = fs.getAbfsClient();
    AbfsClient spiedClient = Mockito.spy(client);
    fs.getAbfsStore().setClient(spiedClient);
    fs.mkdirs(new Path("/testDir/dir1"));
    for (int i = 0; i < 10; i++) {
      fs.create(new Path("/testDir/dir1/file" + i));
    }
    Mockito.doAnswer(answer -> {
          String marker = answer.getArgument(0);
          String prefix = answer.getArgument(1);
          TracingContext context = answer.getArgument(4);
          return client.getListBlobs(marker, prefix, null, 1, context);
        })
        .when(spiedClient)
        .getListBlobs(Mockito.nullable(String.class), Mockito.anyString(),
            Mockito.nullable(String.class), Mockito.nullable(Integer.class),
            Mockito.any(TracingContext.class));
    fs.getAbfsClient().deleteBlobPath(new Path("/testDir/dir1"),
        null, getTestTracingContext(fs, true));

    fs.delete(new Path("/testDir/dir1"), true);

    Assert.assertTrue(!fs.exists(new Path("/testDir/dir1")));
  }

  @Test
  public void testDeleteExplicitDirInImplicitParentDir() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Assume.assumeTrue(fs.getAbfsStore().getPrefixMode() == PrefixMode.BLOB);
    fs.mkdirs(new Path("/testDir/dir1"));
    fs.create(new Path("/testDir/dir1/file1"));
    fs.getAbfsClient().deleteBlobPath(new Path("/testDir/"),
        null, getTestTracingContext(fs, true));

    fs.delete(new Path("/testDir/dir1"), true);

    Assert.assertTrue(!fs.exists(new Path("/testDir/dir1")));
    Assert.assertTrue(!fs.exists(new Path("/testDir/dir1/file1")));
    Assert.assertTrue(fs.exists(new Path("/testDir/")));
  }

  @Test
  public void testDeleteParallelBlobFailure() throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    Assume.assumeTrue(fs.getAbfsStore().getPrefixMode() == PrefixMode.BLOB);
    AbfsClient client = Mockito.spy(fs.getAbfsClient());
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    store.setClient(client);
    Mockito.doReturn(store).when(fs).getAbfsStore();

    fs.mkdirs(new Path("/testDir"));
    fs.create(new Path("/testDir/file1"));
    fs.create(new Path("/testDir/file2"));
    fs.create(new Path("/testDir/file3"));

    Mockito.doThrow(
            new AbfsRestOperationException(HTTP_FORBIDDEN, "", "", new Exception()))
        .when(client)
        .deleteBlobPath(Mockito.any(Path.class), Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));

    LambdaTestUtils.intercept(RuntimeException.class, () -> {
      fs.delete(new Path("/testDir"), true);
    });
  }

  @Test
  public void testDeleteRootWithNonRecursion() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("/testDir"));
    Assertions.assertThat(fs.delete(new Path("/"), false)).isFalse();
  }

  @Test
  public void testDeleteCheckIfParentLMTChange() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    AbfsConfiguration conf = getConfiguration();
    /*
     * LMT of parent directory doesn't change when delete directory triggered with
     * DFS endpoint (both hns and non-hns account). In Blob endpoint, if there
     * is no redirect for ingress / mkdirs, the LMT doesn't change. But in case
     * of ingress redirection, the directory creation of parent overrides the
     * path which changes the LMT. Hence, for tests running with redirect
     * configuration, this test is ignored.
     */
    Assume.assumeFalse(
        getPrefixMode(fs) == PrefixMode.BLOB && (conf.shouldMkdirFallbackToDfs()
            || conf.shouldIngressFallbackToDfs()));
    fs.mkdirs(new Path("/dir1/dir2"));
    fs.create(new Path("/dir1/dir2/file"));
    FileStatus status = fs.getFileStatus(new Path("/dir1"));
    Long lmt = status.getModificationTime();

    fs.delete(new Path("/dir1/dir2"), true);
    Long newLmt = fs.getFileStatus(new Path("/dir1")).getModificationTime();
    Assertions.assertThat(lmt).isEqualTo(newLmt);
  }

  /**
   * Test to assert that the CID in src marker delete contains the
   * total number of blobs operated in the delete directory.
   * Also, to assert that all operations in the delete-directory flow have same
   * primaryId and opType.
   */
  @Test
  public void testDeleteEmitDeletionCountInClientRequestId() throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    Assume.assumeTrue(getPrefixMode(fs) == PrefixMode.BLOB);
    String dirPathStr = "/testDir/dir1";
    fs.mkdirs(new Path(dirPathStr));
    ExecutorService executorService = Executors.newFixedThreadPool(5);
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      final int iter = i;
      Future future = executorService.submit(() -> {
        return fs.create(new Path("/testDir/dir1/file" + iter));
      });
      futures.add(future);
    }

    for (Future future : futures) {
      future.get();
    }
    executorService.shutdown();


    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();
    AbfsClient client = Mockito.spy(store.getClient());
    store.setClient(client);

    final TracingHeaderValidator tracingHeaderValidator
        = new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.DELETE, true, 0);
    fs.registerListener(tracingHeaderValidator);

    Mockito.doAnswer(answer -> {
          Mockito.doAnswer(deleteAnswer -> {
                if (dirPathStr.equalsIgnoreCase(
                    ((Path) deleteAnswer.getArgument(0)).toUri().getPath())) {
                  tracingHeaderValidator.setOperatedBlobCount(11);
                  Object result = deleteAnswer.callRealMethod();
                  tracingHeaderValidator.setOperatedBlobCount(null);
                  return result;
                }
                return deleteAnswer.callRealMethod();
              })
              .when(client)
              .deleteBlobPath(Mockito.any(Path.class),
                  Mockito.nullable(String.class),
                  Mockito.any(TracingContext.class));

          return answer.callRealMethod();
        })
        .when(store)
        .delete(Mockito.any(Path.class), Mockito.anyBoolean(),
            Mockito.any(TracingContext.class));

    fs.delete(new Path(dirPathStr), true);
  }

  @Test
  public void testProducerStopOnDeleteFailure() throws Exception {
    Assume.assumeTrue(getPrefixMode(getFileSystem()) == PrefixMode.BLOB);
    Configuration configuration = Mockito.spy(getRawConfiguration());
    AzureBlobFileSystem fs = Mockito.spy(
        (AzureBlobFileSystem) FileSystem.get(configuration));

    fs.mkdirs(new Path("/src"));
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    List<Future> futureList = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      int iter = i;
      Future future = executorService.submit(() -> {
        try {
          fs.create(new Path("/src/file" + iter));
        } catch (IOException ex) {}
      });
      futureList.add(future);
    }

    for (Future future : futureList) {
      future.get();
    }

    AbfsClient client = fs.getAbfsClient();
    AbfsClient spiedClient = Mockito.spy(client);
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    store.setClient(spiedClient);
    Mockito.doReturn(store).when(fs).getAbfsStore();

    ListBlobProducer[] producers = new ListBlobProducer[1];
    Mockito.doAnswer(answer -> {
      producers[0] = (ListBlobProducer) answer.callRealMethod();
      return producers[0];
    }).when(store).getListBlobProducer(Mockito.anyString(), Mockito.any(
            ListBlobQueue.class), Mockito.nullable(String.class),
        Mockito.any(TracingContext.class));

    AtomicInteger listCounter = new AtomicInteger(0);
    AtomicBoolean hasConsumerStarted = new AtomicBoolean(false);

    Mockito.doAnswer(answer -> {
          String marker = answer.getArgument(0);
          String prefix = answer.getArgument(1);
          String delimiter = answer.getArgument(2);
          TracingContext tracingContext = answer.getArgument(4);
          int counter = listCounter.incrementAndGet();
          if (counter > 1) {
            while (!hasConsumerStarted.get()) {
              Thread.sleep(1_000L);
            }
          }
          Object result = client.getListBlobs(marker, prefix, delimiter, 1,
              tracingContext);
          return result;
        })
        .when(spiedClient)
        .getListBlobs(Mockito.nullable(String.class),
            Mockito.nullable(String.class), Mockito.nullable(String.class),
            Mockito.nullable(Integer.class), Mockito.any(TracingContext.class));

    Mockito.doAnswer(answer -> {
          spiedClient.acquireBlobLease(
              ((Path) answer.getArgument(0)).toUri().getPath(), -1,
              answer.getArgument(2));
          hasConsumerStarted.set(true);
          return answer.callRealMethod();
        })
        .when(spiedClient)
        .deleteBlobPath(Mockito.any(Path.class), Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));

    intercept(Exception.class, () -> {
      fs.delete(new Path("/src"), true);
    });

    producers[0].waitForProcessCompletion();

    Mockito.verify(spiedClient, Mockito.atMost(3))
        .getListBlobs(Mockito.nullable(String.class),
            Mockito.nullable(String.class), Mockito.nullable(String.class),
            Mockito.nullable(Integer.class), Mockito.any(TracingContext.class));
  }

  @Test
  public void deleteBlobDirParallelThreadToDeleteOnDifferentTracingContext()
      throws Exception {
    Assume.assumeTrue(getPrefixMode(getFileSystem()) == PrefixMode.BLOB);
    Configuration configuration = getRawConfiguration();
    AzureBlobFileSystem fs = Mockito.spy(
        (AzureBlobFileSystem) FileSystem.newInstance(configuration));
    AzureBlobFileSystemStore spiedStore = Mockito.spy(fs.getAbfsStore());
    AbfsClient spiedClient = Mockito.spy(fs.getAbfsClient());

    Mockito.doReturn(spiedStore).when(fs).getAbfsStore();
    spiedStore.setClient(spiedClient);

    fs.mkdirs(new Path("/testDir"));
    fs.create(new Path("/testDir/file1"));
    fs.create(new Path("/testDir/file2"));

    AbfsClientTestUtil.hookOnRestOpsForTracingContextSingularity(spiedClient);

    fs.delete(new Path("/testDir"), true);
  }
}
