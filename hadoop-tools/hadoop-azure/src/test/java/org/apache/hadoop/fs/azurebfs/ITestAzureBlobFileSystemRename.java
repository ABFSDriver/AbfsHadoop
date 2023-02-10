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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.rules.ExpectedException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertMkdirs;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathDoesNotExist;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertRenameOutcome;

/**
 * Test rename operation.
 */
public class ITestAzureBlobFileSystemRename extends
        AbstractAbfsIntegrationTest {

  public ITestAzureBlobFileSystemRename() throws Exception {
//    super();
  }

  @Override
  public void setup() throws Exception {
    loadConfiguredFileSystem();
    super.setup();
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
  public void testFNSDestnParentDoesNotExist() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();

    Path src = path("/srcDir/srcF.txt");
    assertTrue(fs.getFileStatus(src).isFile());

    // Non existing parent
    Path dest = path("/destNoParentDir/destFile.txt");
    System.out.println("Sneha : calling rename - start");
    boolean result = fs.rename(src, dest);
    System.out.println("Sneha : test - testFNSDestnParentDoesNotExist: rename success ? " + (result == true));
    if (true != result) {
      fail(String.format("Expected rename(%s, %s) to return %b, but result was %b", src, dest, false, result));
    }
  }

  @Test
  public void testFNSImplicitDestnParent() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();

    Path src = path("/srcDir/srcF.txt");
    assertTrue(fs.getFileStatus(src).isFile());

    Path implicitDummyFile = path("/implicitDir/dummyFile.txt");
    assertTrue(fs.getFileStatus(implicitDummyFile).isFile());

    Path implicitDir = path("/implicitDir");
    assertTrue(fs.getFileStatus(implicitDir).isDirectory());

    // Implicit directory path
    Path dest = path("/implicitDir/destFile.txt");

    System.out.println("Sneha : calling rename - start");
    boolean result = fs.rename(src, dest);
    System.out.println("Sneha : test - testFNSImplicitDestnParent: rename success ? " + (result == true));
    if (true != result) {
      fail(String.format("Expected rename(%s, %s) to return %b, but result was %b", src, dest, false, result));
    }
  }

  @Test
  public void testFNSCleanUpOnSuccess() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();

    fs.delete(path("/implicitDir/"), true);
    try {
      fs.getFileStatus(path("/implicitDir/"));
    } catch (FileNotFoundException e) {
    }

    fs.delete(path("/destExplicitParent"), true);

    try {
      fs.getFileStatus(path("/destExplicitParent/"));
    } catch (FileNotFoundException e) {
    }
    fs.getFileStatus(path("/destExplicitParent/destFile.txt"));
  }

  @Test
  public void testGenInteropTest() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path fileForWasbTest = path ("interopTesting/fileInDFS.txt");
    try (FSDataOutputStream outputStm = fs.create(fileForWasbTest, true)) {
      byte[] b = new byte[8 * 1024 * 1024];
      new Random().nextBytes(b);
      outputStm.write(b);
      outputStm.hflush();
    }
    FileStatus status = fs.getFileStatus(fileForWasbTest);

    System.out.print("Size of " + status.getPath() + " = " + status.getLen());

  }

  @Test
  public void testFNSExplicitDestnParent() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path fileForWasbTest = path ("interopTesting/fileInDFS.txt");
    try (FSDataOutputStream outputStm = fs.create(fileForWasbTest)) {
      byte[] b = new byte[8 * 1024 * 1024];
      new Random().nextBytes(b);
      outputStm.write(b);
      outputStm.hflush();
    }
    FileStatus status = fs.getFileStatus(fileForWasbTest);

    System.out.print("Size of " + status.getPath() + " = " + status.getLen());

    Path src = path("/srcDir/srcF.txt");
    assertTrue(fs.getFileStatus(src).isFile());

    try {
      fs.getFileStatus(path("/destExplicitParent/destFile.txt"));
    } catch (FileNotFoundException e) {
      System.out.println("Sneha - unexpected dest present - 1");
    }

    Path dummyDest = path("/destExplicitParent/dummyDest.txt");
    touch(dummyDest);

    try {
      fs.getFileStatus(path("/destExplicitParent/destFile.txt"));
    } catch (FileNotFoundException e) {
      System.out.println("Sneha - unexpected dest present - 2");
    }

    // Implicit directory path
    Path dest = path("/destExplicitParent/destFile.txt");
    System.out.println("Sneha : calling rename - start");
    boolean result = fs.rename(src, dest);
    System.out.println("Sneha : test - testFNSExplicitDestnParent: rename success ? " + (result == true));
    if (true != result) {
      fail(String.format("Expected rename(%s, %s) to return %b, but result was %b", src, dest, false, result));
    }
  }

  @Test
  public void testFNSImplicitDirListing() throws Exception {
    // https://snvijayanonhnstest.blob.core.windows.net/testfnsrename/implicitListingDir/a/b/c/d/file.txt

    final AzureBlobFileSystem fs = getFileSystem();

    Path implicitListingDir = path("/implicitListingDir");
    for (FileStatus item : fs.listStatus(implicitListingDir)) {
      System.out.println("ListOutput - implicitListingDir: " + item.getPath() + " isFile=" + item.isFile());
    }

    Path a = path("/implicitListingDir/a");
    for (FileStatus item : fs.listStatus(a)) {
      System.out.println("ListOutput - /implicitListingDir/a: " + item.getPath() + " isFile=" + item.isFile());
    }

    Path b = path("/implicitListingDir/a/b");
    for (FileStatus item : fs.listStatus(b)) {
      System.out.println("ListOutput - /implicitListingDir/a/b: " + item.getPath() + " isFile=" + item.isFile());
    }

    Path c = path("/implicitListingDir/a/b/c");
    for (FileStatus item : fs.listStatus(c)) {
      System.out.println("ListOutput - /implicitListingDir/a/b/c: " + item.getPath() + " isFile=" + item.isFile());
    }

    Path d = path("/implicitListingDir/a/b/c/d");
    for (FileStatus item : fs.listStatus(d)) {
      System.out.println("ListOutput - /implicitListingDir/a/b/c/d: " + item.getPath() + " isFile=" + item.isFile());
    }
  }

  @Test
  public void testRenameFileUnderDir() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path sourceDir = new Path("/testSrc");
    assertMkdirs(fs, sourceDir);
    String filename = "file1";
    Path file1 = new Path(sourceDir, filename);
    touch(file1);

    Path destDir = new Path("/testDst");
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
    fs.mkdirs(new Path("testDir"));
    Path test1 = new Path("testDir/test1");
    fs.mkdirs(test1);
    fs.mkdirs(new Path("testDir/test1/test2"));
    fs.mkdirs(new Path("testDir/test1/test2/test3"));

    assertRenameOutcome(fs, test1,
            new Path("testDir/test10"), true);
    assertPathDoesNotExist(fs, "rename source dir", test1);
  }

  @Test
  public void testRenameFirstLevelDirectory() throws Exception {
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
    Path source = new Path("/test");
    Path dest = new Path("/renamedDir");
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
    fs.mkdirs(new Path("testDir2/test1/test2/test3"));
    fs.mkdirs(new Path("testDir2/test4"));
    Assert.assertTrue(fs.rename(new Path("testDir2/test1/test2/test3"), new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2")));
    assertTrue(fs.exists(new Path("testDir2/test1/test2")));
    assertTrue(fs.exists(new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2/test4/test3")));
    assertFalse(fs.exists(new Path("testDir2/test1/test2/test3")));
  }

}
