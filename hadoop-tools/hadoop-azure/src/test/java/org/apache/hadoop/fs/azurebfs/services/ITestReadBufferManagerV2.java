package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

public class ITestReadBufferManagerV2 extends AbstractAbfsIntegrationTest {

  protected ITestReadBufferManagerV2() throws Exception {
    super();
  }

  @Test
  public void testReadBufferManagerV2() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    int fileSize = 30 * ONE_MB;
    Path[] testPaths = createFilesWithContent(fs, "testFile", 5, fileSize);
    ExecutorService executorService = Executors.newFixedThreadPool(5);
    int[] fileIdx = new int[1];
    try {
      for (int i = 0; i < 5; i++) {
        executorService.submit((Callable<Void>) () -> {
          try (FSDataInputStream iStream = fs.open(testPaths[fileIdx[0]++])) {
            int bytesRead = iStream.read(new byte[fileSize], 0, fileSize);
            Assertions.assertEquals(fileSize, bytesRead,
                "Read size should match file size");
          }
          return null;
        });
      }
    } catch(Exception e) {
      System.out.println("Exception occurred during file read: " + e.getMessage());
    } finally {
      executorService.shutdown();
      // wait for all tasks to finish
      executorService.awaitTermination(1, TimeUnit.MINUTES);
    }
  }

  private byte[] getRandomBytesArray(int length) {
    final byte[] b = new byte[length];
    new Random().nextBytes(b);
    return b;
  }

  private Path createFileWithContent(FileSystem fs, String fileName,
      byte[] fileContent) throws
      IOException {
    Path testFilePath = path(fileName);
    try (FSDataOutputStream oStream = fs.create(testFilePath)) {
      oStream.write(fileContent);
      oStream.flush();
    }
    return testFilePath;
  }

  private Path[] createFilesWithContent(FileSystem fs, String fileNamePrefix,
      int numFiles, int fileSize) throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(numFiles);
    Path[] tesFilePaths = new Path[numFiles];
    int[] fileIdx = new int[1];
    try {
      for (int i = 0; i < 5; i++) {
        final String fileName = fileNamePrefix + i;
        executorService.submit((Callable<Void>) () -> {
          byte[] fileContent = getRandomBytesArray(fileSize);
          tesFilePaths[fileIdx[0]++] = createFileWithContent(fs, fileName, fileContent);
          return null;
        });
      }
    } finally {
      executorService.shutdown();
      // wait for all tasks to finish
      executorService.awaitTermination(1, TimeUnit.MINUTES);
    }
  return tesFilePaths;
  }
}
