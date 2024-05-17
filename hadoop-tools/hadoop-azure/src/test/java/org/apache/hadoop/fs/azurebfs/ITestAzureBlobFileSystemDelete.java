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
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientTestUtil;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.ITestAbfsClient;
import org.apache.hadoop.fs.azurebfs.services.ListBlobQueue;
import org.apache.hadoop.fs.azurebfs.services.TestAbfsPerfTracker;
import org.apache.hadoop.fs.azurebfs.utils.TestMockHelpers;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.LambdaTestUtils;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;
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

    Path testPath = path("/testFolder");
    fs.mkdirs(new Path(testPath + "_0"));
    fs.mkdirs(new Path(testPath + "_1"));
    fs.mkdirs(new Path(testPath + "_2"));
    touch(new Path(testPath + "_1/testfile"));
    touch(new Path(testPath + "_1/testfile2"));
    touch(new Path(testPath + "_1/testfile3"));

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
    Path testfile = path("/testFile");
    touch(testfile);
    assertDeleted(fs, testfile, false);

    intercept(FileNotFoundException.class,
        () -> fs.open(testfile));
  }

  @Test
  public void testEnsureFileIsDeleted() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testfile = path("testfile");
    touch(testfile);
    assertDeleted(fs, testfile, false);
    assertPathDoesNotExist(fs, "deleted", testfile);
  }

  @Test
  public void testDeleteDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path dir = path("testfile");
    fs.mkdirs(dir);
    fs.mkdirs(new Path(dir + "/test1"));
    fs.mkdirs(new Path(dir + "/test1/test2"));

    assertDeleted(fs, dir, true);
    assertPathDoesNotExist(fs, "deleted", dir);
  }

  @Test
  public void testDeleteFirstLevelDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final List<Future<Void>> tasks = new ArrayList<>();

    ExecutorService es = Executors.newFixedThreadPool(10);
    Path dir = path("/test");
    for (int i = 0; i < 1000; i++) {
      final Path fileName = new Path(dir + "/" + i);
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
        getTestTracingContext(fs, true),
        fs.getIsNamespaceEnabled(getTestTracingContext(fs, true))));

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
    AbfsRestOperation idempotencyRetOp = Mockito.spy(ITestAbfsClient.getRestOp(
        DeletePath, mockClient, HTTP_METHOD_DELETE,
        ITestAbfsClient.getTestUrl(mockClient, "/NonExistingPath"),
        ITestAbfsClient.getTestRequestHeaders(mockClient)));
    idempotencyRetOp.hardSetResult(HTTP_OK);

    doReturn(idempotencyRetOp).when(mockClient).deleteIdempotencyCheckOp(any());
    TracingContext tracingContext = getTestTracingContext(fs, false);
    doReturn(tracingContext).when(idempotencyRetOp).createNewTracingContext(any());
    when(mockClient.deletePath("/NonExistingPath", false, null,
        tracingContext, fs.getIsNamespaceEnabled(tracingContext)))
        .thenCallRealMethod();

    Assertions.assertThat(mockClient.deletePath(
        "/NonExistingPath",
        false,
        null,
        tracingContext, fs.getIsNamespaceEnabled(tracingContext))
        .getResult()
        .getStatusCode())
        .describedAs("Idempotency check reports successful "
            + "delete. 200OK should be returned")
        .isEqualTo(idempotencyRetOp.getResult().getStatusCode());

    // Call from AzureBlobFileSystemStore should not fail either
    mockStore.delete(new Path("/NonExistingPath"), false, getTestTracingContext(fs, false));
  }

  @Test
  public void deleteBlobDirParallelThreadToDeleteOnDifferentTracingContext()
      throws Exception {
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
    fs.close();
  }

  private void assumeBlobClient() throws IOException {
    Assumptions.assumeThat(getFileSystem().getAbfsClient())
        .isInstanceOf(AbfsBlobClient.class);
  }

  /**
   * Assert that deleting an implicit directory delete all the children of the
   * folder.
   */
  @Test
  public void testDeleteImplicitDir() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeBlobClient();
    fs.mkdirs(new Path("/testDir/dir1"));
    fs.create(new Path("/testDir/dir1/file1"));
    AbfsBlobClient client = (AbfsBlobClient) fs.getAbfsClient();
    client.deleteBlobPath(new Path("/testDir/dir1"),
        null, getTestTracingContext(fs, true));
    fs.delete(new Path("/testDir/dir1"), true);

    Assertions.assertThat(!fs.exists(new Path("/testDir/dir1")))
        .describedAs("FileStatus of the deleted directory should not exist")
        .isTrue();
    Assertions.assertThat(!fs.exists(new Path("/testDir/dir1/file1")))
        .describedAs("Child of a deleted directory should not be present");
  }

  /**
   * Assert deleting an implicit directory, for which paginated list is required.
   */
  @Test
  public void testDeleteImplicitDirWithSingleListResults() throws Exception {
    AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(
        getRawConfiguration());
    assumeBlobClient();
    AbfsBlobClient client = (AbfsBlobClient) fs.getAbfsClient();
    AbfsBlobClient spiedClient = Mockito.spy(client);
    fs.getAbfsStore().setClient(spiedClient);
    fs.mkdirs(new Path("/testDir/dir1"));
    for (int i = 0; i < 10; i++) {
      fs.create(new Path("/testDir/dir1/file" + i));
    }

    Mockito.doAnswer(answer -> {
          String path = answer.getArgument(0);
          boolean recursive = answer.getArgument(1);
          String continuation = answer.getArgument(3);
          TracingContext context = answer.getArgument(4);

          return client.listPath(path, recursive, 1, continuation, context);
        })
        .when(spiedClient)
        .listPath(Mockito.anyString(), Mockito.anyBoolean(), Mockito.anyInt(),
            Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));

    client.deleteBlobPath(new Path("/testDir/dir1"),
        null, getTestTracingContext(fs, true));

    fs.delete(new Path("/testDir/dir1"), true);

    Assertions.assertThat(fs.exists(new Path("/testDir/dir1")))
        .describedAs("FileStatus of the deleted directory should not exist")
        .isFalse();
  }

  /**
   * Assert deleting of the only child of an implicit directory ensures that the
   * parent directory's marker is present.
   */
  @Test
  public void testDeleteExplicitDirInImplicitParentDir() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeBlobClient();
    AbfsBlobClient client = (AbfsBlobClient) fs.getAbfsClient();
    fs.mkdirs(new Path("/testDir/dir1"));
    fs.create(new Path("/testDir/dir1/file1"));
    client.deleteBlobPath(new Path("/testDir/"),
        null, getTestTracingContext(fs, true));

    fs.delete(new Path("/testDir/dir1"), true);

    Assertions.assertThat(fs.exists(new Path("/testDir/dir1")))
        .describedAs("Deleted directory should not exist")
        .isFalse();
    Assertions.assertThat(fs.exists(new Path("/testDir/dir1/file1")))
        .describedAs("Child of a deleted directory should not be present")
        .isFalse();
    Assertions.assertThat(fs.exists(new Path("/testDir")))
        .describedAs("Parent Implicit directory should exist")
        .isTrue();
  }

  @Test
  public void testDeleteParallelBlobFailure() throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    assumeBlobClient();
    AbfsBlobClient client = Mockito.spy((AbfsBlobClient) fs.getAbfsClient());
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

        LambdaTestUtils.intercept(
        AccessDeniedException.class,
        () -> {
          fs.delete(new Path("/testDir"), true);
        });
  }

  @Test
  public void testDeleteRootWithNonRecursion() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("/testDir"));
    Assertions.assertThat(fs.delete(new Path(ROOT_PATH), false)).isFalse();
  }

  /**
   * Assert that delete operation failure should stop List producer.
   */
  @Test
  public void testProducerStopOnDeleteFailure() throws Exception {
    assumeBlobClient();
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

    AbfsBlobClient client = (AbfsBlobClient) fs.getAbfsClient();
    AbfsBlobClient spiedClient = Mockito.spy(client);
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    store.setClient(spiedClient);
    Mockito.doReturn(store).when(fs).getAbfsStore();

    final int[] deleteCallInvocation = new int[1];
    deleteCallInvocation[0] = 0;
    Mockito.doAnswer(answer -> {
          throw new AbfsRestOperationException(HTTP_FORBIDDEN, "", "",
              new Exception());
        }).when(spiedClient)
        .deleteBlobPath(Mockito.any(Path.class), Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));

    AbfsClientTestUtil.mockGetDeleteBlobHandler(spiedClient,
        (blobDeleteHandler) -> {
          Mockito.doAnswer(answer -> {
                try {
                  answer.callRealMethod();
                } catch (AbfsRestOperationException ex) {
                  if (ex.getStatusCode() == HTTP_FORBIDDEN) {
                    deleteCallInvocation[0]++;
                  }
                  throw ex;
                }
                throw new AssertionError("List Consumption should have failed");
              })
              .when(blobDeleteHandler).listRecursiveAndTakeAction();
          return null;
        });

    final int[] listCallInvocation = new int[1];
    listCallInvocation[0] = 0;
    Mockito.doAnswer(answer -> {
          if (listCallInvocation[0] == 1) {
            while (deleteCallInvocation[0] == 0) ;
          }
          listCallInvocation[0]++;
          return answer.callRealMethod();
        })
        .when(spiedClient)
        .listPath(Mockito.anyString(), Mockito.anyBoolean(), Mockito.anyInt(),
            Mockito.nullable(String.class), Mockito.any(TracingContext.class));

    intercept(AccessDeniedException.class,
        () -> {
          fs.delete(new Path("/src"), true);
        });

    Mockito.verify(spiedClient, Mockito.times(1))
        .listPath(Mockito.anyString(), Mockito.anyBoolean(), Mockito.anyInt(),
            Mockito.nullable(String.class), Mockito.any(TracingContext.class));
  }
}
