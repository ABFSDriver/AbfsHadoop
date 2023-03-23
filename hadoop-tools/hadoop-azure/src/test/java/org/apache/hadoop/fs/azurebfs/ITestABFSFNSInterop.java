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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.azure.ITestNativeAzureFileSystemFNSInterop;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;

public class ITestABFSFNSInterop extends
    AbstractAbfsIntegrationTest {

  String log_prefix = "\nSneha:IOP:";
  String parentTestFolder = "/testRoot";
  int defaultTestFileSize = 8 * 1024 * 1024;

  public ITestABFSFNSInterop() throws Exception {
    super();

  }

  @Override
  public void setup() throws Exception {
    String timestampFolder =  Instant.now().toString().replace(":", "_");
    parentTestFolder = "/testRootDir_" + timestampFolder + "/interopTesting/";
    loadConfiguredFileSystem();
    super.setup();
  }

  @After
  public void teardown() throws Exception {
    //super.teardown();
  }

  public Path getWasbBlobPath(String blobName) {
    return new Path("wasb://" + getFileSystemName() + "@" + getAccountName().replace(".dfs.", ".blob.") + "/" + parentTestFolder + blobName);
  }

  public Path getRelativeDFSPath(String blobName) {
    return new Path(parentTestFolder + blobName);
  }

  private void println(String msg) {
    System.out.print(log_prefix + msg + "\n");
  }

  protected void assertContentReadCorrectly(byte[] actualFileContent, int from,
                                            int len, byte[] contentRead) {
    for (int i = 0; i < len; i++) {
      assertEquals(contentRead[i], actualFileContent[i + from]);
    }
  }

  protected void assertContentReadCorrectly(byte[] actualFileContent, byte[] contentRead) {
    assertEquals(actualFileContent.length, contentRead.length);

    for (int i = 0; i < actualFileContent.length; i++) {
      assertEquals(contentRead[i], actualFileContent[i]);
    }
  }

  public void testUtilityCleanUpOnSuccess() throws Exception {
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

  public void testUtilityGenInteropTest() throws Exception {
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

  // will create a 8 MB blob
  public Path testUtilityCreateBlob(String blobName) throws Throwable {
    ITestNativeAzureFileSystemFNSInterop wasb = new ITestNativeAzureFileSystemFNSInterop();
    Path wasbPath = getWasbBlobPath(blobName);
    wasb.createBlobFile(getFileSystemName(), getWasbBlobPath(blobName), defaultTestFileSize);
    println("Created file on BlobEndpoint : " + wasbPath.toString());
    return wasbPath;
  }

  public Path testUtilityCreateBlob(String blobName, byte[] data) throws Throwable {
    ITestNativeAzureFileSystemFNSInterop wasb = new ITestNativeAzureFileSystemFNSInterop();
    Path wasbPath = getWasbBlobPath(blobName);
    wasb.createBlobFile(getFileSystemName(), getWasbBlobPath(blobName), data);
    println("Created file on BlobEndpoint : " + wasbPath.toString());
    return wasbPath;
  }

  public Path testUtilityCreateDFSFile(AzureBlobFileSystem fs, String fileName) throws Throwable {
    Path dfsFilePath = getRelativeDFSPath(fileName);
    try(FSDataOutputStream outStream = fs.create(dfsFilePath)) {
      byte[] fileData = null;
        fileData = getTestData(defaultTestFileSize);
        outStream.write(fileData);
        outStream.hflush();
    }
    println("Created file on DFSEndpoint : " + dfsFilePath);
    return dfsFilePath;
  }

  private static byte[] getTestData(int size) {
    byte[] testData = new byte[size];
    System.arraycopy(RandomStringUtils.randomAlphabetic(size).getBytes(), 0, testData, 0, size);
    return testData;
  }

  public void createFileUsingAzcopy(String pathFromContainerRoot) throws IOException, InterruptedException {
    String shellcmd = "/home/snvijaya/Documents/AbfsHadoop/hadoop-tools/hadoop-azure/azcopy/createFile.sh " + pathFromContainerRoot;
    String[] cmd = { "bash", "-c", shellcmd };
    Process p = Runtime.getRuntime().exec(cmd);
    p.waitFor();
  }

  @Test
  public void testDFSGetFileStatusOnBlobEndPtBlob() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    String testFileName = getMethodName();

    // Save status for assert when created on DFS
    Path testDFSPath = testUtilityCreateDFSFile(fs, testFileName);
    FileStatus dfsPathFileStatus = fs.getFileStatus(testDFSPath);
    // delete the file
    fs.delete(testDFSPath, false);

    // Recreate on Blob endpoint
    Path testWASBPath = testUtilityCreateBlob(testFileName);

    // Validate
    FileStatus status = fs.getFileStatus(testDFSPath);
    assertEquals(dfsPathFileStatus.getPath(), status.getPath());
    assertEquals(dfsPathFileStatus.isFile(), status.isFile());
    assertEquals(dfsPathFileStatus.getLen(), status.getLen());

    println(status.getPath().toString() + " " + status.getLen() + " " + status.isFile());
  }

  @Test (expected = IOException.class)
  public void testDFSAppendOnBlobEndPtBlob() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    String testFileName = getMethodName();
    Path testDFSPath = getRelativeDFSPath(testFileName);

    // Recreate on Blob endpoint
    Path testWASBPath = testUtilityCreateBlob(testFileName);

    /* Caused by: Operation failed: "The resource was created or modified
    by the Azure Blob Service API and cannot be appended to by the Azure
    Data Lake Storage Service API.", 409, PUT,
    https://snvijayanonhnstest.dfs.core.windows.net/testfnsrename/
    testRootDir_2023-02-11T13_04_50.169Z/interopTesting/testBlobAppend
    ?action=append&position=8388608&timeout=90, InvalidAppendOperation,
    "The resource was created or modified by the Azure Blob Service API and
    cannot be appended to by the Azure Data Lake Storage Service API.
    RequestId:6e349201-a01f-002e-6f19-3eadec000000 Time:2023-02-11T13:05:38.1071981Z" */

    // Validate
    try (FSDataOutputStream outStream = fs.append(testDFSPath)) {
      outStream.write(getTestData(defaultTestFileSize));
    }
  }

  @Test (expected = FileAlreadyExistsException.class)
  public void testDFSCreateOvwFalseOnBlobEndPtBlob() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    String testFileName = getMethodName();
    Path testDFSPath = getRelativeDFSPath(testFileName);

    // Recreate on Blob endpoint
    Path testWASBPath = testUtilityCreateBlob(testFileName);

    // Validate
    try (FSDataOutputStream outStream = fs.create(testDFSPath, false)) {
    }
  }

  @Test
  public void testDFSCreateOvwTrueOnBlobEndPtBlob() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    String testFileName = getMethodName();
    Path testDFSPath = getRelativeDFSPath(testFileName);

    // Recreate on Blob endpoint
    Path testWASBPath = testUtilityCreateBlob(testFileName);

    // Validate
    try (FSDataOutputStream outStream = fs.create(testDFSPath, true)) {
    }
  }

  @Test
  public void testDFSReadOnBlobEndPtBlob() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    String testFileName = getMethodName();
    Path testDFSPath = getRelativeDFSPath(testFileName);

    byte[] data = getTestData(defaultTestFileSize);
    Path testWASBPath = testUtilityCreateBlob(testFileName, data);
    assertTrue(fs.exists(testDFSPath));

    // Validate
    try (FSDataInputStream inStream = fs.open(testDFSPath)) {
      int fileLen = (int) fs.getFileStatus(testDFSPath).getLen();
      byte[] readFile = new byte[fileLen];
      inStream.read(readFile, 0, fileLen);
      assertContentReadCorrectly(data, readFile);
    }
  }

  @Test
  public void testBlobDeleteOnBlobEndPtBlob() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    String testFileName = getMethodName();
    Path testDFSPath = getRelativeDFSPath(testFileName);
    Path testWASBPath = testUtilityCreateBlob(testFileName);

    fs.delete(testDFSPath, false);

    assertFalse(fs.exists(testDFSPath));
  }

  @Test
  public void testDFSDeleteOnBlobEndPtBlob() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    getAbfsStore(fs).getAbfsConfiguration().setRedirectRename(false);
    String testFileName = getMethodName();
    Path testDFSPath = getRelativeDFSPath(testFileName);
    Path testWASBPath = testUtilityCreateBlob(testFileName);

    fs.delete(testDFSPath, false);

    assertFalse(fs.exists(testDFSPath));
  }

  @Test
  public void testBlobDeleteOnDFSBlob() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    String testFileName = getMethodName();
    Path testDFSPath = testUtilityCreateDFSFile(fs, testFileName);

    fs.delete(testDFSPath, false);

    assertFalse(fs.exists(testDFSPath));
  }

  @Test
  public void testDFSListStatusOnBlobEndPtBlob() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    String testFileName = getMethodName();

    // Save status for assert when created on DFS
    Path testDFSPath = testUtilityCreateDFSFile(fs, testFileName);
    FileStatus dfsPathFileStatus = (fs.listStatus(testDFSPath))[0];
    // delete the file
    fs.delete(testDFSPath, false);

    // Recreate on Blob endpoint
    Path testWASBPath = testUtilityCreateBlob(testFileName);

    // Validate
    FileStatus status = (fs.listStatus(testDFSPath))[0];
    assertEquals(dfsPathFileStatus.getPath(), status.getPath());
    assertEquals(dfsPathFileStatus.isFile(), status.isFile());
    assertEquals(dfsPathFileStatus.getLen(), status.getLen());

    println(status.getPath().toString() + " " + status.getLen() + " " + status.isFile());
  }

  @Test
  public void testRenameBlobEndPtSrcToDestnWithNoParent() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    String srcParent = "Implicit_Parent_Of_Src_In_testRenameBlobEndPtSrcToDestnWithNoParent/";
    String destParent = "Implicit_Parent_Of_Dest_In_testRenameBlobEndPtSrcToDestnWithNoParent/";

    String srcFile = srcParent + "Src_" + getMethodName();
    Path srcTestDFSPath = getRelativeDFSPath(srcFile);
    Path srcTestWASBPath = testUtilityCreateBlob(srcFile);

    String destFile = destParent + "Dest_" + getMethodName();
    Path destTestDFSPath = getRelativeDFSPath(destFile);
    Path destTestWASBPath = getWasbBlobPath(destFile);

    assertTrue(fs.exists(srcTestDFSPath));
    assertFalse(fs.exists(destTestDFSPath));
    assertFalse(fs.rename(srcTestDFSPath, destTestDFSPath));
  }

  @Test
  public void testDFSRenameToDestnWithImplicitParent() throws Throwable {
    testRenameBlobEndPtSrcToDestnWithImplicitParent(false);
  }

  @Test
  public void testBlobRenameToDestnWithImplicitParent() throws Throwable {
    testRenameBlobEndPtSrcToDestnWithImplicitParent(true);
  }

  public void testRenameBlobEndPtSrcToDestnWithImplicitParent(boolean redirect) throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    getAbfsStore(fs).getAbfsConfiguration().setRedirectRename(redirect);

    String redirectStr = (redirect ? "Blob_Rename_" : "DFS_Rename_");
    String srcParent = redirectStr + "Implicit_Parent_Of_Src_In_testRenameBlobEndPtSrcToDestnWithImplicitParent/";
    String destParent = redirectStr + "Implicit_Parent_Of_Dest_In_testRenameBlobEndPtSrcToDestnWithImplicitParent/";

    String srcFile = srcParent + "Src_" + getMethodName();
    Path srcTestDFSPath = getRelativeDFSPath(srcFile);
    Path srcTestWASBPath = testUtilityCreateBlob(srcFile);

    // create implicit dir
    String dummyFileAtDestParent = destParent + "dummyFileAtDestParent";
    Path dummyFileAtDestParentPath = getRelativeDFSPath(dummyFileAtDestParent);
    createFileUsingAzcopy(dummyFileAtDestParentPath.toString());
    assertTrue(fs.exists(dummyFileAtDestParentPath));

    String destFile = destParent + "Dest_" + getMethodName();
    Path destTestDFSPath = getRelativeDFSPath(destFile);

    assertTrue(fs.exists(srcTestDFSPath));
    assertFalse(fs.exists(destTestDFSPath));

    if (redirect) {
      assertTrue(fs.rename(srcTestDFSPath, destTestDFSPath));
      assertFalse(fs.exists(srcTestDFSPath));
      assertTrue(fs.exists(destTestDFSPath));
    } else {
      assertFalse(fs.rename(srcTestDFSPath, destTestDFSPath));
    }
  }

  @Test
  public void testImplicitDirListing() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    String parentDir = "Implicit_Parent_Of_testImplicitDirListing";
    String implicitChildDir_of_parentDir = parentDir + "/Implicit_A";
    String implicitChildDir_of_A = implicitChildDir_of_parentDir + "/Implicit_B";
    String implicitChildDir_of_B = implicitChildDir_of_A + "/Implicit_C";
    String implicitChildDir_of_C = implicitChildDir_of_B + "/Implicit_D";
    String childFile_of_D = implicitChildDir_of_C + "/" + getMethodName();
    Path fileDFSPath = getRelativeDFSPath(childFile_of_D);
    createFileUsingAzcopy(fileDFSPath.toString());

    Path parentDirPath = getRelativeDFSPath(parentDir);
    Path implicitChildDir_of_parentDir_Path = getRelativeDFSPath(implicitChildDir_of_parentDir);
    Path implicitChildDir_of_A_Path = getRelativeDFSPath(implicitChildDir_of_A);
    Path implicitChildDir_of_B_Path = getRelativeDFSPath(implicitChildDir_of_B);
    Path implicitChildDir_of_C_Path = getRelativeDFSPath(implicitChildDir_of_C);

    FileStatus status = (fs.listStatus(parentDirPath))[0];
    assertTrue(status.getPath().toString().endsWith(implicitChildDir_of_parentDir));

    status = (fs.listStatus(implicitChildDir_of_parentDir_Path))[0];
    assertTrue(status.getPath().toString().endsWith(implicitChildDir_of_A));

    status = (fs.listStatus(implicitChildDir_of_A_Path))[0];
    assertTrue(status.getPath().toString().endsWith(implicitChildDir_of_B));

    status = (fs.listStatus(implicitChildDir_of_B_Path))[0];
    println(" status=> " + status.getPath().toString() + " expected=>" + implicitChildDir_of_C);
    assertTrue(status.getPath().toString().endsWith(implicitChildDir_of_C));

    status = (fs.listStatus(implicitChildDir_of_C_Path))[0];
    assertTrue(status.getPath().toString().endsWith(childFile_of_D));
  }

}
