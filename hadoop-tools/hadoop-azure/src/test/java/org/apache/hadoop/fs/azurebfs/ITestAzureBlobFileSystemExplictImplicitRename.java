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
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient;

import static org.apache.hadoop.fs.azurebfs.ITestAzureBlobFileSystemRename.addSpyHooksOnClient;
import static org.apache.hadoop.fs.azurebfs.ITestAzureBlobFileSystemRename.assumeNonHnsAccountBlobEndpoint;
import static org.apache.hadoop.fs.azurebfs.ITestAzureBlobFileSystemRename.crashRenameAndRecover;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class ITestAzureBlobFileSystemExplictImplicitRename
    extends AbstractAbfsIntegrationTest {

  public ITestAzureBlobFileSystemExplictImplicitRename() throws Exception {
    super();
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    Assume.assumeTrue(
        getFileSystem().getAbfsClient() instanceof AbfsBlobClient);
  }

  @Test
  public void testRenameSrcFileInImplicitParentDirectory() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    List<Path> dirPaths = new ArrayList<>();
    dirPaths.add(new Path("/src"));
    List<Path> blobPath = new ArrayList<>();
    blobPath.add(new Path("/src/file"));

    createMultiplePath(dirPaths, blobPath);

    AbfsBlobClient client = getAbfsBlobClient(fs);

    Assert.assertTrue(fs.rename(new Path("/src/file"), new Path("/dstFile")));
    Assert.assertNotNull(client.getPathStatus("/dstFile",
        getTestTracingContext(fs, true), null, false));
    intercept(AbfsRestOperationException.class, () -> {
      client.getPathStatus("/src/file",
          getTestTracingContext(fs, true), null, false);
    });

    Assert.assertFalse(fs.rename(new Path("/src/file"), new Path("/dstFile2")));
  }

  private AbfsBlobClient getAbfsBlobClient(final AzureBlobFileSystem fs) {
    return fs.getAbfsStore().getClientHandler().getBlobClient();
  }

  @Test
  public void testRenameNonExistentFileInImplicitParent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    createAzCopyFolder(new Path("/src"));

    Assert.assertFalse(fs.rename(new Path("/src/file"), new Path("/dstFile2")));
  }

  @Test
  public void testRenameFileToNonExistingDstInImplicitParent()
      throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    List<Path> dirPaths = new ArrayList<>();
    dirPaths.add(new Path("/dstDir"));

    List<Path> filePaths = new ArrayList<>();
    filePaths.add(new Path("/file"));
    filePaths.add(new Path("/dstDir/file2"));

    createMultiplePath(dirPaths, filePaths);

    Assert.assertTrue(fs.rename(new Path("/file"), new Path("/dstDir")));
    Assert.assertTrue(fs.exists(new Path("/dstDir/file")));
  }

  @Test
  public void testRenameFileAsExistingExplicitDirectoryInImplicitDirectory()
      throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    createAzCopyFile(new Path("/file"));
    fs.mkdirs(new Path("/dst/dir"));
    deleteBlobPath(fs, new Path("/dst"));
    Assert.assertTrue(fs.rename(new Path("/file"), new Path("/dst/dir")));
    Assert.assertTrue(fs.exists(new Path("/dst/dir/file")));
    intercept(AbfsRestOperationException.class, () -> {
      getAbfsBlobClient(fs).getPathStatus("/file",
          getTestTracingContext(fs, true), null, false);
    });
  }

  @Test
  public void testRenameFileAsExistingImplicitDirectoryInExplicitDirectory()
      throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("/dst"));

    List<Path> dirPaths = new ArrayList<>();
    dirPaths.add(new Path("/dst/dir"));

    List<Path> filePaths = new ArrayList<>();
    filePaths.add(new Path("/file"));
    filePaths.add(new Path("/dst/dir/file2"));

    createMultiplePath(dirPaths, filePaths);

    Assert.assertTrue(fs.rename(new Path("/file"), new Path("/dst/dir")));
    Assert.assertTrue(fs.exists(new Path("/dst/dir/file")));
    intercept(AbfsRestOperationException.class, () -> {
      getAbfsBlobClient(fs).getPathStatus("/file",
          getTestTracingContext(fs, true), null, false);
    });
  }

  @Test
  public void testRenameFileAsExistingImplicitDirectoryInImplicitDirectory()
      throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    List<Path> dirPaths = new ArrayList<>();
    dirPaths.add(new Path("/dst"));
    dirPaths.add(new Path("/dst/dir"));

    List<Path> filePaths = new ArrayList<>();
    filePaths.add(new Path("/file"));
    filePaths.add(new Path("/dst/dir/file2"));

    createMultiplePath(dirPaths, filePaths);

    Assert.assertTrue(fs.rename(new Path("/file"), new Path("/dst/dir")));
    Assert.assertTrue(fs.exists(new Path("/dst/dir/file")));
    intercept(AbfsRestOperationException.class, () -> {
      getAbfsBlobClient(fs).getPathStatus("/file",
          getTestTracingContext(fs, true), null, false);
    });
  }

  @Test
  public void testRenameDirectoryContainingImplicitDirectory()
      throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("/src"));
    fs.mkdirs(new Path("/dst"));
    List<Path> dirPaths = new ArrayList<>();
    dirPaths.add(new Path("/src/subDir"));

    List<Path> filePaths = new ArrayList<>();
    filePaths.add(new Path("/src/subFile"));
    filePaths.add(new Path("/src/subDir/subFile"));

    createMultiplePath(dirPaths, filePaths);

    Assert.assertTrue(fs.rename(new Path("/src"), new Path("/dst/dir")));
    Assert.assertTrue(fs.exists(new Path("/dst/dir/subFile")));
    Assert.assertTrue(fs.exists(new Path("/dst/dir/subDir/subFile")));
  }

  @Test
  public void testRenameImplicitDirectoryContainingExplicitDirectory()
      throws Exception {
    explicitImplicitDirectoryRenameTest(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/false,
        /*isDstFile*/false,
        /*shouldRenamePass*/true
    );
  }

  @Test
  public void testRenameImplicitDirectoryContainingImplicitDirectory()
      throws Exception {
    explicitImplicitDirectoryRenameTest(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/false,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/false,
        /*isDstFile*/false,
        /*shouldRenamePass*/true
    );
  }

  @Test
  public void testRenameExplicitDirectoryContainingExplicitDirectoryInImplicitSrcParent()
      throws Exception {
    explicitImplicitDirectoryRenameTest(
        /*srcParentExplicit*/false,
        /*srcExplicit*/true,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/false,
        /*isDstFile*/false,
        /*shouldRenamePass*/true
    );
  }

  @Test
  public void testRenameExplicitDirectoryContainingImplicitDirectoryInImplicitSrcParent()
      throws Exception {
    explicitImplicitDirectoryRenameTest(
        /*srcParentExplicit*/false,
        /*srcExplicit*/true,
        /*srcSubDirExplicit*/false,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/false,
        /*isDstFile*/false,
        /*shouldRenamePass*/true
    );
  }

  @Test
  public void testRenameImplicitDirectoryContainingExplicitDirectoryInImplicitSrcParent()
      throws Exception {
    explicitImplicitDirectoryRenameTest(
        /*srcParentExplicit*/false,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/false,
        /*isDstFile*/false,
        /*shouldRenamePass*/true
    );
  }

  @Test
  public void testRenameImplicitDirectoryContainingImplicitDirectoryInImplicitSrcParent()
      throws Exception {
    explicitImplicitDirectoryRenameTest(
        /*srcParentExplicit*/false,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/false,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/false,
        /*isDstFile*/false,
        /*shouldRenamePass*/true
    );
  }

  @Test
  public void testRenameDirectoryWhereDstParentDoesntExist() throws Exception {
    explicitImplicitDirectoryRenameTest(
        /*srcParentExplicit*/true,
        /*srcExplicit*/true,
        /*srcSubDirExplicit*/false,
        /*dstParentExplicit*/false,
        /*dstExplicit*/false,
        /*dstParentExists*/false,
        /*isDstParentFile*/false,
        /*dstExist*/false,
        /*isDstFile*/false,
        /*shouldRenamePass*/false
    );
  }

  @Test
  public void testRenameImplicitDirectoryWhereDstParentDoesntExist()
      throws Exception {
    explicitImplicitDirectoryRenameTest(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/false,
        /*dstExplicit*/false,
        /*dstParentExists*/false,
        /*isDstParentFile*/false,
        /*dstExist*/false,
        /*isDstFile*/false,
        /*shouldRenamePass*/false
    );
  }

  @Test
  public void testRenameImplicitDirectoryToNonExistentDstWithImplicitParent()
      throws Exception {
    explicitImplicitDirectoryRenameTest(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/false,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/false,
        /*isDstFile*/false,
        /*shouldRenamePass*/true
    );
  }

  @Test
  public void testRenameImplicitDirectoryToNonExistentDstWithParentIsFile()
      throws Exception {
    explicitImplicitDirectoryRenameTest(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/false,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/true,
        /*dstExist*/false,
        /*isDstFile*/false,
        /*shouldRenamePass*/false
    );
  }

  @Test
  public void testRenameExplicitDirectoryToFileDst() throws Exception {
    explicitImplicitDirectoryRenameTest(
        /*srcParentExplicit*/true,
        /*srcExplicit*/true,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/true,
        /*shouldRenamePass*/false
    );
  }

  @Test
  public void testRenameimplicitDirectoryToFileDst() throws Exception {
    explicitImplicitDirectoryRenameTest(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/true,
        /*shouldRenamePass*/false
    );
  }

  @Test
  public void testDirectoryIntoSameNameDestination() throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/true,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/true,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/"src",
        /*dstSubFileName*/null,
        /*dstSubDirName*/null,
        /*isSubDirExplicit*/true, /*shouldRenamePass*/true
    );
  }

  @Test
  public void testRenameDirectoryToSameNameImplicitDirectoryDestination()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/true,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/"src",
        /*dstSubFileName*/null,
        /*dstSubDirName*/null,
        /*isSubDirExplicit*/true, /*shouldRenamePass*/true
    );
  }

  @Test
  public void testRenameDirectoryToImplicitDirectoryDestinationHavingSameNameSubDir()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/true,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/null,
        /*dstSubDirName*/"src",
        /*isSubDirExplicit*/true, /*shouldRenamePass*/false
    );
  }

  @Test
  public void testRenameDirectoryToImplicitDirectoryDestinationHavingSameNameSubFile()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/true,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/"src",
        /*dstSubDirName*/null,
        /*isSubDirExplicit*/true, /*shouldRenamePass*/false
    );
  }

  @Test
  public void testRenameDirectoryToImplicitDirectoryDestinationHavingSameNameImplicitSubDir()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/true,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/null,
        /*dstSubDirName*/"src",
        /*isSubDirExplicit*/false, /*shouldRenamePass*/false
    );
  }

  @Test
  public void testImplicitDirectoryIntoSameNameDestination() throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/true,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/"src",
        /*dstSubFileName*/null,
        /*dstSubDirName*/null,
        /*isSubDirExplicit*/true, /*shouldRenamePass*/true
    );
  }

  @Test
  public void testImplicitDirectoryIntoExplicitDestination() throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/true,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/null,
        /*dstSubDirName*/null,
        /*isSubDirExplicit*/true, /*shouldRenamePass*/true
    );
  }

  @Test
  public void testRenameImplicitDirectoryToExplicitDirectoryDestinationHavingSameNameSubDir()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/true,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/null,
        /*dstSubDirName*/"src",
        /*isSubDirExplicit*/true, /*shouldRenamePass*/false
    );
  }

  @Test
  public void testRenameImplicitDirectoryToExplicitDirectoryDestinationHavingSameNameSubFile()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/true,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/"src",
        /*dstSubDirName*/null,
        /*isSubDirExplicit*/true, /*shouldRenamePass*/false
    );
  }

  @Test
  public void testRenameImplicitDirectoryToExplicitDirectoryDestinationHavingSameNameImplicitSubDir()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/true,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/null,
        /*dstSubDirName*/"src",
        /*isSubDirExplicit*/false, /*shouldRenamePass*/false
    );
  }

  @Test
  public void testImplicitDirectoryIntoSameNameImplicitDestination()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/"src",
        /*dstSubFileName*/null,
        /*dstSubDirName*/null,
        /*isSubDirExplicit*/true, /*shouldRenamePass*/true
    );
  }

  @Test
  public void testImplicitDirectoryIntoImplicitDestination() throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/null,
        /*dstSubDirName*/null,
        /*isSubDirExplicit*/true, /*shouldRenamePass*/true
    );
  }

  @Test
  public void testRenameImplicitDirectoryToImplicitDirectoryDestinationHavingSameNameSubDir()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/null,
        /*dstSubDirName*/"src",
        /*isSubDirExplicit*/true, /*shouldRenamePass*/false
    );
  }

  @Test
  public void testRenameImplicitDirectoryToImplicitDirectoryDestinationHavingSameNameSubFile()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/"src",
        /*dstSubDirName*/null,
        /*isSubDirExplicit*/true, /*shouldRenamePass*/false
    );
  }

  @Test
  public void testRenameImplicitDirectoryToImplicitDirectoryDestinationHavingSameNameImplicitSubDir()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/null,
        /*dstSubDirName*/"src",
        /*isSubDirExplicit*/false, /*shouldRenamePass*/false
    );
  }

  @Test
  public void testRenameExplicitSrcWithImplicitSubDirToImplicitDstWithExplicitSubDir()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/true,
        /*srcSubDirExplicit*/false,
        /*dstParentExplicit*/false,
        /*dstExplicit*/true,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/null,
        /*dstSubDirName*/null,
        /*isSubDirExplicit*/true, /*shouldRenamePass*/true
    );
  }

  @Test
  public void testRenameExplicitSrcWithImplicitSubDirToImplicitDstWithImplicitSubDir()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/true,
        /*srcSubDirExplicit*/false,
        /*dstParentExplicit*/false,
        /*dstExplicit*/true,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/null,
        /*dstSubDirName*/null,
        /*isSubDirExplicit*/false, /*shouldRenamePass*/true
    );
  }


  private void explicitImplicitDirectoryRenameTest(Boolean srcParentExplicit,
      Boolean srcExplicit,
      Boolean srcSubDirExplicit,
      Boolean dstParentExplicit,
      Boolean dstExplicit,
      Boolean dstParentExists,
      Boolean isDstParentFile,
      Boolean dstExist,
      Boolean isDstFile,
      Boolean shouldRenamePass) throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path srcParent = new Path("/srcParent");
    Path src = new Path(srcParent, "src");
    Pair<List<Path>, List<Path>> srcPair = createSourcePaths(srcParentExplicit,
        srcExplicit, srcSubDirExplicit, fs,
        srcParent,
        src);

    Path dstParent = new Path("/dstParent");
    Path dst = new Path(dstParent, "dst");
    Pair<List<Path>, List<Path>> dstPair = createDestinationPaths(dstParentExplicit, dstExplicit, dstParentExists,
        isDstParentFile,
        dstExist, isDstFile, fs, dstParent, dst, null, null, true);

    List<Path> dirPath = new ArrayList<>();
    dirPath.addAll(srcPair.getKey());
    dirPath.addAll(dstPair.getKey());

    List<Path> filePath = new ArrayList<>();
    filePath.addAll(srcPair.getValue());
    filePath.addAll(dstPair.getValue());

    createMultiplePath(dirPath, filePath);

    explicitImplicitCaseRenameAssert(dstExist, shouldRenamePass, fs, src, dst);
  }

  private void explicitImplicitDirectoryRenameTestWithDestPathNames(Boolean srcParentExplicit,
      Boolean srcExplicit,
      Boolean srcSubDirExplicit,
      Boolean dstParentExplicit,
      Boolean dstExplicit,
      Boolean dstParentExists,
      Boolean isDstParentFile,
      Boolean dstExist,
      Boolean isDstFile,
      String srcName,
      String dstName,
      String dstSubFileName,
      String dstSubDirName,
      final Boolean isSubDirExplicit, Boolean shouldRenamePass)
      throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path srcParent = new Path("/srcParent");
    Path src = new Path(srcParent, srcName != null ? srcName : "src");
    Pair<List<Path>, List<Path>> srcPathPair = createSourcePaths(srcParentExplicit, srcExplicit, srcSubDirExplicit, fs,
        srcParent,
        src);

    Path dstParent = new Path("/dstParent");
    Path dst = new Path(dstParent, dstName != null ? dstName : "dst");
    Pair<List<Path>, List<Path>> dstPathPair = createDestinationPaths(dstParentExplicit, dstExplicit, dstParentExists,
        isDstParentFile,
        dstExist, isDstFile, fs, dstParent, dst, dstSubFileName, dstSubDirName,
        isSubDirExplicit);

    List<Path> dirPath = new ArrayList<>();
    dirPath.addAll(srcPathPair.getKey());
    dirPath.addAll(dstPathPair.getKey());

    List<Path> filePath = new ArrayList<>();
    filePath.addAll(srcPathPair.getValue());
    filePath.addAll(dstPathPair.getValue());

    createMultiplePath(dirPath, filePath);

    explicitImplicitCaseRenameAssert(dstExist, shouldRenamePass, fs, src, dst);
  }

  private Pair<List<Path>, List<Path>> createSourcePaths(final Boolean srcParentExplicit,
      final Boolean srcExplicit,
      final Boolean srcSubDirExplicit,
      final AzureBlobFileSystem fs,
      final Path srcParent,
      final Path src) throws Exception {
    List<Path> dirPaths = new ArrayList<>();
    List<Path> filePaths = new ArrayList<>();

    if (srcParentExplicit) {
      fs.mkdirs(srcParent);
    } else {
      dirPaths.add(srcParent);
    }

    if (srcExplicit) {
      fs.mkdirs(src);
      if (!srcParentExplicit) {
        deleteBlobPath(fs, srcParent);
      }
    } else {
      dirPaths.add(src);
    }
    filePaths.add(new Path(src, "subFile"));
    if (srcSubDirExplicit) {
      fs.mkdirs(new Path(src, "subDir"));
      if (!srcParentExplicit) {
        deleteBlobPath(fs, srcParent);
      }
      if (!srcExplicit) {
        deleteBlobPath(fs, src);
      }
    } else {
      Path srcSubDir = new Path(src, "subDir");
      dirPaths.add(srcSubDir);
      filePaths.add(new Path(srcSubDir, "subFile"));
    }

    return Pair.of(dirPaths, filePaths);
  }

  private void deleteBlobPath(final AzureBlobFileSystem fs,
      final Path srcParent)
      throws AzureBlobFileSystemException {
    try {
      getAbfsBlobClient(fs).deleteBlobPath(srcParent, null,
          getTestTracingContext(fs, true));
    } catch (AbfsRestOperationException ex) {
      if (ex.getStatusCode() != HttpURLConnection.HTTP_NOT_FOUND) {
        throw ex;
      }
    }
  }

  private Pair<List<Path>, List<Path>> createDestinationPaths(final Boolean dstParentExplicit,
      final Boolean dstExplicit,
      final Boolean dstParentExists,
      final Boolean isDstParentFile,
      final Boolean dstExist,
      final Boolean isDstFile,
      final AzureBlobFileSystem fs,
      final Path dstParent,
      final Path dst, final String subFileName, final String subDirName,
      final Boolean isSubDirExplicit) throws Exception {
    List<Path> dirPaths = new ArrayList<>();
    List<Path> filePaths = new ArrayList<>();
    if (dstParentExists) {
      if (!isDstParentFile) {
        if (dstParentExplicit) {
          fs.mkdirs(dstParent);
        } else {
          dirPaths.add(dstParent);
        }
      } else {
        filePaths.add(dstParent);
      }
    }

    if (dstExist) {
      if (!isDstFile) {
        if (dstExplicit) {
          fs.mkdirs(dst);
          if (!dstParentExplicit) {
            deleteBlobPath(fs, dstParent);
          }
        } else {
          dirPaths.add(dst);
        }
        if (subFileName != null) {
          filePaths.add(new Path(dst, subFileName));
        }
        if (subDirName != null) {
          if (isSubDirExplicit) {
            fs.mkdirs(new Path(dst, subDirName));
            if (!dstParentExplicit) {
              deleteBlobPath(fs, dstParent);
            }
            if (!dstExplicit) {
              deleteBlobPath(fs, dst);
            }
          } else {
            dirPaths.add(new Path(dst, subDirName));
          }
        }
      } else {
        filePaths.add(dst);
      }
    }
    return Pair.of(dirPaths, filePaths);
  }

  private void explicitImplicitCaseRenameAssert(final Boolean dstExist,
      final Boolean shouldRenamePass,
      final AzureBlobFileSystem fs,
      final Path src,
      final Path dst) throws IOException {
    AbfsBlobClient blobClient = getAbfsBlobClient(fs);
    if (shouldRenamePass) {
      Assert.assertTrue(fs.rename(src, dst));
      if (dstExist) {
        Assert.assertTrue(blobClient.checkIsDir(blobClient.getPathStatus(
            new Path(dst, src.getName()).toUri().getPath(),
            getTestTracingContext(fs, true), null, false).getResult()));
      } else {
        Assert.assertTrue(blobClient.checkIsDir(
            blobClient.getPathStatus(dst.toUri().getPath(),
                getTestTracingContext(fs, true), null, false).getResult()));
      }
    } else {
      Assert.assertFalse(fs.rename(src, dst));
      Assert.assertTrue(
          blobClient.listPath(src.toUri().getPath(), false, 5000, null,
                  getTestTracingContext(fs, true), false)
              .getResult()
              .getListResultSchema()
              .paths()
              .size() > 0);
      if (dstExist) {
        Assert.assertTrue(
            blobClient.listPath(src.toUri().getPath(), false, 5000, null,
                    getTestTracingContext(fs, true), false)
                .getResult()
                .getListResultSchema()
                .paths()
                .size() > 0);
      }
    }
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
    getFileSystem().setWorkingDirectory(new Path("/"));
    AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fs);
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);

    String srcPath = "hbase/test1/test2";
    final String failedCopyPath = srcPath + "/test3/file1";
    List<Path> dirPathList = new ArrayList<>();
    dirPathList.add(new Path(srcPath));
    dirPathList.add(new Path(srcPath, "test3"));
    dirPathList.add(new Path("hbase/test4/"));

    List<Path> blobPathList = new ArrayList<>();
    blobPathList.add(new Path(srcPath, "test3/file"));
    blobPathList.add(new Path(failedCopyPath));
    blobPathList.add(new Path("hbase/test4/file1"));

    createMultiplePath(dirPathList, blobPathList);

    crashRenameAndRecover(fs, client, srcPath, (abfsFs) -> {
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
    getFileSystem().setWorkingDirectory(new Path("/"));
    AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fs);
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);

    String srcPath = "hbase/test1/test2";
    final String failedCopyPath = srcPath + "/test3/file1";
    List<Path> dirPathList = new ArrayList<>();

    dirPathList.add(new Path(srcPath));
    dirPathList.add(new Path(srcPath, "test3"));
    dirPathList.add(new Path("hbase/test4/"));

    List<Path> blobPathList = new ArrayList<>();
    blobPathList.add(new Path(srcPath, "test3/file"));
    blobPathList.add(new Path(failedCopyPath));
    blobPathList.add(new Path("hbase/test4/file1"));

    createMultiplePath(dirPathList, blobPathList);

    crashRenameAndRecover(fs, client, srcPath, (abfsFs) -> {
      abfsFs.exists(new Path(srcPath));
      return null;
    });
  }
}
