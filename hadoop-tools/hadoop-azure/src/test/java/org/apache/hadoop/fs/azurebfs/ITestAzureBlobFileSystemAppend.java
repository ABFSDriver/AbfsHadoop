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
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsDfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.services.AzureBlobIngressHandler;
import org.apache.hadoop.fs.azurebfs.services.AzureDFSIngressHandler;
import org.apache.hadoop.fs.azurebfs.services.AzureIngressHandler;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.store.BlockUploadStatistics;
import org.apache.hadoop.fs.store.DataBlocks;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.DATA_BLOCKS_BUFFER;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_CONDITIONAL_CREATE_OVERWRITE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_INFINITE_LEASE_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_INGRESS_SERVICE_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_LEASE_THREADS;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_BLOB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_DFS;
import static org.apache.hadoop.fs.store.DataBlocks.DATA_BLOCKS_BUFFER_ARRAY;
import static org.apache.hadoop.fs.store.DataBlocks.DATA_BLOCKS_BUFFER_DISK;
import static org.apache.hadoop.fs.store.DataBlocks.DATA_BLOCKS_BYTEBUFFER;
import static org.apache.hadoop.fs.store.DataBlocks.DataBlock.DestState.Closed;
import static org.apache.hadoop.fs.store.DataBlocks.DataBlock.DestState.Writing;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test append operations.
 */
public class ITestAzureBlobFileSystemAppend extends
    AbstractAbfsIntegrationTest {
  private static final String TEST_FILE_PATH = "testfile";
  private static final String TEST_FOLDER_PATH = "testFolder";

  public ITestAzureBlobFileSystemAppend() throws Exception {
    super();
  }

  @Test(expected = FileNotFoundException.class)
  public void testAppendDirShouldFail() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path filePath = path(TEST_FILE_PATH);
    fs.mkdirs(filePath);
    fs.append(filePath, 0).close();
  }

  @Test
  public void testAppendWithLength0() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    try(FSDataOutputStream stream = fs.create(path(TEST_FILE_PATH))) {
      final byte[] b = new byte[1024];
      new Random().nextBytes(b);
      stream.write(b, 1000, 0);
      assertEquals(0, stream.getPos());
    }
  }


  @Test(expected = FileNotFoundException.class)
  public void testAppendFileAfterDelete() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path filePath = path(TEST_FILE_PATH);
    ContractTestUtils.touch(fs, filePath);
    fs.delete(filePath, false);

    fs.append(filePath).close();
  }

  @Test(expected = FileNotFoundException.class)
  public void testAppendDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path folderPath = path(TEST_FOLDER_PATH);
    fs.mkdirs(folderPath);
    fs.append(folderPath).close();
  }

  @Test
  public void testTracingForAppend() throws IOException {
    AzureBlobFileSystem fs = getFileSystem();
    Path testPath = path(TEST_FILE_PATH);
    fs.create(testPath).close();
    fs.registerListener(new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.APPEND, false, 0));
    fs.append(testPath, 10);
  }

  @Test
  public void testCloseOfDataBlockOnAppendComplete() throws Exception {
    Set<String> blockBufferTypes = new HashSet<>();
    blockBufferTypes.add(DATA_BLOCKS_BUFFER_DISK);
    blockBufferTypes.add(DATA_BLOCKS_BYTEBUFFER);
    blockBufferTypes.add(DATA_BLOCKS_BUFFER_ARRAY);
    for (String blockBufferType : blockBufferTypes) {
      Configuration configuration = new Configuration(getRawConfiguration());
      configuration.set(DATA_BLOCKS_BUFFER, blockBufferType);
      AzureBlobFileSystem fs = Mockito.spy(
          (AzureBlobFileSystem) FileSystem.newInstance(configuration));
      AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
      Mockito.doReturn(store).when(fs).getAbfsStore();
      DataBlocks.DataBlock[] dataBlock = new DataBlocks.DataBlock[1];
      Mockito.doAnswer(getBlobFactoryInvocation -> {
        DataBlocks.BlockFactory factory = Mockito.spy(
            (DataBlocks.BlockFactory) getBlobFactoryInvocation.callRealMethod());
        Mockito.doAnswer(factoryCreateInvocation -> {
              dataBlock[0] = Mockito.spy(
                  (DataBlocks.DataBlock) factoryCreateInvocation.callRealMethod());
              return dataBlock[0];
            })
            .when(factory)
            .create(Mockito.anyLong(), Mockito.anyInt(), Mockito.any(
                BlockUploadStatistics.class));
        return factory;
      }).when(store).getBlockFactory();
      try (OutputStream os = fs.create(
          new Path(getMethodName() + "_" + blockBufferType))) {
        os.write(new byte[1]);
        Assertions.assertThat(dataBlock[0].getState())
            .describedAs(
                "On write of data in outputStream, state should become Writing")
            .isEqualTo(Writing);
        os.close();
        Mockito.verify(dataBlock[0], Mockito.times(1)).close();
        Assertions.assertThat(dataBlock[0].getState())
            .describedAs("On close of outputStream, state should become Closed")
            .isEqualTo(Closed);
      }
    }
  }

  /**
   * Creates a file over DFS and attempts to append over Blob.
   * It should fallback to DFS when appending to the file fails.
   *
   * @throws IOException if an I/O error occurs.
   */
  @Test
  public void testCreateOverDfsAppendOverBlob() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    Path TEST_FILE_PATH = new Path("testFile");
    AzureBlobFileSystemStore.Permissions permissions
        = new AzureBlobFileSystemStore.Permissions(false,
        FsPermission.getDefault(), FsPermission.getUMask(fs.getConf()));
    fs.getAbfsStore().getClientHandler().getDfsClient().
        createPath(makeQualified(TEST_FILE_PATH).toUri().getPath(), true, false,
            permissions, false, null,
            null, getTestTracingContext(fs, true));
    fs.getAbfsStore()
        .getAbfsConfiguration()
        .set(FS_AZURE_INGRESS_SERVICE_TYPE, "BLOB");
    FSDataOutputStream outputStream = fs.append(TEST_FILE_PATH);
    AzureIngressHandler ingressHandler
        = ((AbfsOutputStream) outputStream.getWrappedStream()).getIngressHandler();
    AbfsClient client = ingressHandler.getClient();
    Assert.assertTrue("Blob client was not used before fallback",
        client instanceof AbfsBlobClient);
    outputStream.write(10);
    outputStream.hsync();
    outputStream.write(20);
    outputStream.hsync();
    outputStream.write(30);
    outputStream.hsync();
    AzureIngressHandler ingressHandlerFallback
        = ((AbfsOutputStream) outputStream.getWrappedStream()).getIngressHandler();
    AbfsClient clientFallback = ingressHandlerFallback.getClient();
    Assert.assertTrue("DFS client was not used after fallback",
        clientFallback instanceof AbfsDfsClient);
    }

  /**
   * Tests the correct retrieval of the AzureIngressHandler based on the configured ingress service type.
   *
   * @throws IOException if an I/O error occurs
   */
  @Test
  public void testValidateIngressHandler() throws IOException {
    Configuration configuration = getRawConfiguration();
    configuration.set(FS_AZURE_INGRESS_SERVICE_TYPE, ABFS_BLOB);
    AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(configuration);
    Path TEST_FILE_PATH = new Path("testFile");
    AzureBlobFileSystemStore.Permissions permissions
        = new AzureBlobFileSystemStore.Permissions(false,
        FsPermission.getDefault(), FsPermission.getUMask(fs.getConf()));
      fs.getAbfsStore().getClientHandler().getBlobClient().
          createPath(makeQualified(TEST_FILE_PATH).toUri().getPath(), true,
              false,
              permissions, false, null,
              null, getTestTracingContext(fs, true));
      FSDataOutputStream outputStream = fs.append(TEST_FILE_PATH);
      AzureIngressHandler ingressHandler
          = ((AbfsOutputStream) outputStream.getWrappedStream()).getIngressHandler();
      Assert.assertTrue("Ingress handler instance is not correct",
          ingressHandler instanceof AzureBlobIngressHandler);
      AbfsClient client = ingressHandler.getClient();
      Assert.assertTrue("Blob client was not used correctly",
          client instanceof AbfsBlobClient);

      Path TEST_FILE_PATH_1 = new Path("testFile1");
      fs.getAbfsStore().getClientHandler().getBlobClient().
          createPath(makeQualified(TEST_FILE_PATH_1).toUri().getPath(), true,
              false,
              permissions, false, null,
              null, getTestTracingContext(fs, true));
      fs.getAbfsStore()
          .getAbfsConfiguration()
          .set(FS_AZURE_INGRESS_SERVICE_TYPE, ABFS_DFS);
      FSDataOutputStream outputStream1 = fs.append(TEST_FILE_PATH_1);
      AzureIngressHandler ingressHandler1
          = ((AbfsOutputStream) outputStream1.getWrappedStream()).getIngressHandler();
      Assert.assertTrue("Ingress handler instance is not correct",
          ingressHandler1 instanceof AzureDFSIngressHandler);
      AbfsClient client1 = ingressHandler1.getClient();
      Assert.assertTrue("DFS client was not used correctly",
          client1 instanceof AbfsDfsClient);
    }

  @Test(expected = FileNotFoundException.class)
  public void testAppendImplicitDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path folderPath = new Path(TEST_FOLDER_PATH);
    fs.mkdirs(folderPath);
    fs.append(folderPath.getParent());
  }

  @Test(expected = FileNotFoundException.class)
  public void testAppendFileNotExists() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path folderPath = new Path(TEST_FOLDER_PATH);
    fs.append(folderPath);
  }

  /**
   * Create directory over dfs endpoint and append over blob endpoint.
   * Should return error as append is not supported for directory.
   * **/
  @Test(expected = IOException.class)
  public void testCreateExplicitDirectoryOverDfsAppendOverBlob() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path folderPath = path(TEST_FOLDER_PATH);
    AzureBlobFileSystemStore.Permissions permissions
        = new AzureBlobFileSystemStore.Permissions(false,
        FsPermission.getDefault(), FsPermission.getUMask(fs.getConf()));
    fs.getAbfsStore().getClientHandler().getDfsClient().
        createPath(makeQualified(folderPath).toUri().getPath(), false, false,
            permissions, false, null,
            null, getTestTracingContext(fs, true));
    FSDataOutputStream outputStream = fs.append(folderPath);
    outputStream.write(10);
    outputStream.hsync();
  }

  /**
   * Recreate file between append and flush. Etag mismatch happens.
   **/
  @Test(expected = IOException.class)
  public void testRecreateAppendAndFlush() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path filePath = path(TEST_FILE_PATH);
    fs.create(filePath);
    FSDataOutputStream outputStream = fs.append(filePath);
    outputStream.write(10);
    final AzureBlobFileSystem fs1 = (AzureBlobFileSystem) FileSystem.newInstance(getRawConfiguration());
    FSDataOutputStream outputStream1 = fs1.create(filePath);
    outputStream.hsync();
  }

  /**
   * Recreate directory between append and flush. Etag mismatch happens.
   **/
  @Test(expected = IOException.class)
  public void testRecreateDirectoryAppendAndFlush() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path filePath = path(TEST_FILE_PATH);
    fs.create(filePath);
    FSDataOutputStream outputStream = fs.append(filePath);
    outputStream.write(10);
    final AzureBlobFileSystem fs1 = (AzureBlobFileSystem) FileSystem.newInstance(getRawConfiguration());
    fs1.mkdirs(filePath);
    outputStream.hsync();
  }

  /**
   * Verify that parallel write with same offset from different output streams will not throw exception.
   **/
  @Test
  public void testParallelWriteSameOffsetDifferentOutputStreams() throws Exception {
    Configuration configuration = getRawConfiguration();
    configuration.set(FS_AZURE_ENABLE_CONDITIONAL_CREATE_OVERWRITE, "false");
    AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(configuration);
    ExecutorService executorService = Executors.newFixedThreadPool(5);
    List<Future<?>> futures = new ArrayList<>();

    final byte[] b = new byte[8 * ONE_MB];
    new Random().nextBytes(b);
    final Path filePath = path(TEST_FILE_PATH);
    // Create three output streams
    FSDataOutputStream out1 = fs.create(filePath);
    FSDataOutputStream out2 = fs.append(filePath);
    FSDataOutputStream out3 = fs.append(filePath);

    // Submit tasks to write to each output stream with the same offset
    futures.add(executorService.submit(() -> {
      try {
        out1.write(b, 10, 200);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }));

    futures.add(executorService.submit(() -> {
      try {
        out2.write(b, 10, 200);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }));

    futures.add(executorService.submit(() -> {
      try {
        out3.write(b, 10, 200);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }));

    int exceptionCaught = 0;
    for (Future<?> future : futures) {
      try {
        future.get(); // wait for the task to complete and handle any exceptions thrown by the lambda expression
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException) {
          exceptionCaught++;
        } else {
          System.err.println("Unexpected exception caught: " + cause);
        }
      } catch (InterruptedException e) {
        // handle interruption
      }
    }
    assertEquals(exceptionCaught, 0);
  }

  /**
   * Verify that parallel write for different content length will not throw exception.
   **/
  @Test
  public void testParallelWriteDifferentContentLength() throws Exception {
    Configuration configuration = getRawConfiguration();
    configuration.set(FS_AZURE_ENABLE_CONDITIONAL_CREATE_OVERWRITE, "false");
    FileSystem fs = FileSystem.newInstance(configuration);
    ExecutorService executorService = Executors.newFixedThreadPool(5);
    List<Future<?>> futures = new ArrayList<>();

    final Path filePath = path(TEST_FILE_PATH);
    // Create three output streams with different content length
    FSDataOutputStream out1 = fs.create(filePath);
    final byte[] b1 = new byte[8 * ONE_MB];
    new Random().nextBytes(b1);

    FSDataOutputStream out2 = fs.append(filePath);
    FSDataOutputStream out3 = fs.append(filePath);

    // Submit tasks to write to each output stream
    futures.add(executorService.submit(() -> {
      try {
        out1.write(b1, 10, 200);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }));

    futures.add(executorService.submit(() -> {
      try {
        out2.write(b1, 20, 300);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }));

    futures.add(executorService.submit(() -> {
      try {
        out3.write(b1, 30, 400);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }));

    int exceptionCaught = 0;
    for (Future<?> future : futures) {
      try {
        future.get(); // wait for the task to complete and handle any exceptions thrown by the lambda expression
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException) {
          exceptionCaught++;
        } else {
          System.err.println("Unexpected exception caught: " + cause);
        }
      } catch (InterruptedException e) {
        // handle interruption
      }
    }
    assertEquals(exceptionCaught, 0);
  }

  /**
   * Verify that parallel write for different content length will not throw exception.
   **/
  @Test
  public void testParallelWriteOutputStreamClose() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    final Path SECONDARY_FILE_PATH = new Path("secondarytestfile");
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    List<Future<?>> futures = new ArrayList<>();

    FSDataOutputStream out1 = fs.create(SECONDARY_FILE_PATH);
    AbfsOutputStream outputStream1 = (AbfsOutputStream) out1.getWrappedStream();
    String fileETag = outputStream1.getIngressHandler().getETag();
    final byte[] b1 = new byte[8 * ONE_MB];
    new Random().nextBytes(b1);
    final byte[] b2 = new byte[8 * ONE_MB];
    new Random().nextBytes(b2);

    FSDataOutputStream out2 = fs.append(SECONDARY_FILE_PATH);

    // Submit tasks to write to each output stream
    futures.add(executorService.submit(() -> {
      try {
        out1.write(b1, 0, 200);
        out1.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }));

    futures.add(executorService.submit(() -> {
      try {
        out2.write(b2, 0, 400);
        out2.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }));

    int exceptionCaught = 0;

    for (Future<?> future : futures) {
      try {
        future.get(); // wait for the task to complete and handle any exceptions thrown by the lambda expression
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException) {
          exceptionCaught++;
        } else {
          System.err.println("Unexpected exception caught: " + cause);
        }
      } catch (InterruptedException e) {
        // handle interruption
      }
    }

    assertEquals(exceptionCaught, 1);
    // Validate that the data written in the buffer is the same as what was read
    final byte[] readBuffer = new byte[8 * ONE_MB];
    int result;
    FSDataInputStream inputStream = fs.open(SECONDARY_FILE_PATH);
    inputStream.seek(0);

    AbfsOutputStream outputStream2 = (AbfsOutputStream) out1.getWrappedStream();
    String out1Etag = outputStream2.getIngressHandler().getETag();

    AbfsOutputStream outputStream3 = (AbfsOutputStream) out2.getWrappedStream();
    String out2Etag = outputStream3.getIngressHandler().getETag();

    if (!fileETag.equals(out1Etag)) {
      result = inputStream.read(readBuffer, 0, 4 * ONE_MB);
      assertEquals(result, 200); // Verify that the number of bytes read matches the number of bytes written
      assertArrayEquals(
          Arrays.copyOfRange(readBuffer, 0, result), Arrays.copyOfRange(b1, 0, result)); // Verify that the data read matches the original data written
    } else if (!fileETag.equals(out2Etag)) {
      result = inputStream.read(readBuffer, 0, 4 * ONE_MB);
      assertEquals(result, 400); // Verify that the number of bytes read matches the number of bytes written
      assertArrayEquals(Arrays.copyOfRange(readBuffer, 0, result), Arrays.copyOfRange(b2, 0, result)); // Verify that the data read matches the original data written
    } else {
      fail("Neither out1 nor out2 was flushed successfully.");
    }
  }

  /**
   * Verify that once flushed etag changes.
   **/
  @Test
  public void testEtagMismatch() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    final Path filePath = path(TEST_FILE_PATH);
    FSDataOutputStream out1 = fs.create(filePath);
    FSDataOutputStream out2 = fs.create(filePath);

    out2.write(10);
    out2.hsync();
    out1.write(10);
    intercept(IOException.class, () -> out1.hsync());
  }

//  @Test
//  public void testAppendWithLease() throws Exception {
//    final Path testFilePath = new Path("/testfile");
//    final AzureBlobFileSystem fs = Mockito.spy(getCustomFileSystem(testFilePath.getParent(), 1));
//    FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL,
//        FsAction.ALL);
//    FsPermission umask = new FsPermission(FsAction.NONE, FsAction.NONE,
//        FsAction.NONE);
//    AbfsOutputStream outputStream = (AbfsOutputStream) fs.getAbfsStore().createFile(testFilePath, null, true,
//        permission, umask, getTestTracingContext(fs, true));
//    outputStream.write(10);
//    outputStream.close();
//    assertNotNull(outputStream.getLeaseId());
//  }
//
//  private AzureBlobFileSystem getCustomFileSystem(Path infiniteLeaseDirs, int numLeaseThreads) throws Exception {
//    Configuration conf = getRawConfiguration();
//    conf.setBoolean(String.format("fs.%s.impl.disable.cache", getAbfsScheme()), true);
//    conf.set(FS_AZURE_INFINITE_LEASE_KEY, infiniteLeaseDirs.toUri().getPath());
//    conf.setInt(FS_AZURE_LEASE_THREADS, numLeaseThreads);
//    FileSystem fileSystem = FileSystem.newInstance(conf);
//    return (AzureBlobFileSystem) fileSystem;
//  }
}
