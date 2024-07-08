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

import org.assertj.core.api.Assertions;
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

  /**
   * Rename a blob whose parent is an implicit directory. Assert renameOperation
   * succeeds.
   */
  @Test
  public void testRenameSrcFileInImplicitParentDirectory() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    List<Path> dirPaths = new ArrayList<>();
    dirPaths.add(new Path("/src"));
    List<Path> blobPath = new ArrayList<>();
    blobPath.add(new Path("/src/file"));

    createMultiplePath(dirPaths, blobPath);

    AbfsBlobClient client = getAbfsBlobClient(fs);

    Assertions.assertThat(fs.rename(new Path("/src/file"), new Path("/dstFile")))
            .describedAs("Rename of implicit parent directory should succeed")
            .isTrue();
    Assertions.assertThat(client.getPathStatus("/dstFile",
        getTestTracingContext(fs, true), null, false))
            .describedAs("Destination should exist after rename")
            .isNotNull();
    assertPathNotExist(client, "/src/file", fs);

    Assertions.assertThat(fs.rename(new Path("/src/file"), new Path("/dstFile2")))
        .describedAs("Renamed source can not renamed again")
        .isFalse();
  }

  private void assertPathNotExist(final AbfsBlobClient client,
      final String path,
      final AzureBlobFileSystem fs) throws Exception {
    intercept(AbfsRestOperationException.class, () -> {
      client.getPathStatus(path,
          getTestTracingContext(fs, true), null, false);
    });
  }

  private AbfsBlobClient getAbfsBlobClient(final AzureBlobFileSystem fs) {
    return fs.getAbfsStore().getClientHandler().getBlobClient();
  }

  /**
   * Rename a non-existent blob whose parent is an implicit directory. Assert 
   * rename operation failure.
   */
  @Test
  public void testRenameNonExistentFileInImplicitParent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    createAzCopyFolder(new Path("/src"));

    Assertions
        .assertThat(fs.rename(new Path("/src/file"), new Path("/dstFile2")))
        .describedAs("Rename of non-existent file should fail")
        .isFalse();
  }

  /**
   * Rename a blob to an implicit directory. Assert that the rename operation succeeds.
   */
  @Test
  public void testRenameFileToInImplicitDirectory()
      throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    List<Path> dirPaths = new ArrayList<>();
    dirPaths.add(new Path("/dstDir"));

    List<Path> filePaths = new ArrayList<>();
    filePaths.add(new Path("/file"));
    filePaths.add(new Path("/dstDir/file2"));

    createMultiplePath(dirPaths, filePaths);

    Assertions.assertThat(fs.rename(new Path("/file"), new Path("/dstDir")))
        .describedAs("Rename of file to implicit directory should succeed")
        .isTrue();
    Assertions.assertThat(fs.exists(new Path("/dstDir/file")))
        .describedAs("Destination path should exist after rename")
        .isTrue();
  }

  /**
   * Test rename of a file to an explicit directory whose parent is implicit.
   * Assert that the rename operation succeeds.
   */
  @Test
  public void testRenameFileToExistingExplicitDirectoryInImplicitDirectory()
      throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    createAzCopyFile(new Path("/file"));
    fs.mkdirs(new Path("/dst/dir"));
    deleteBlobPath(fs, new Path("/dst"));
    Assertions.assertThat(fs.rename(new Path("/file"), new Path("/dst/dir")))
            .describedAs("Rename of file to explicit directory in implicit parent should succeed")
            .isTrue();
    Assertions.assertThat(fs.exists(new Path("/dst/dir/file")))
            .describedAs("Destination path should exist after rename")
            .isTrue();
    Assertions.assertThat(fs.exists(new Path("/file")))
            .describedAs("Source path should not exist after rename")
            .isFalse();
  }

  /**
   * Test rename of a file to an implicit directory whose parent is explicit. Assert
   * that the rename operation succeeds.
   */
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

    Assertions.assertThat(fs.rename(new Path("/file"), new Path("/dst/dir")))
            .describedAs("Rename of file to implicit directory in explicit parent should succeed")
            .isTrue();
    Assertions.assertThat(fs.exists(new Path("/dst/dir/file")))
            .describedAs("Destination path should exist after rename")
            .isTrue();
    Assertions.assertThat(fs.exists(new Path("/dst/dir/file")))
            .describedAs("Source path should not exist after rename")
            .isFalse();
    assertPathNotExist(getAbfsBlobClient(fs), "/file", fs);
  }

  /**
   * Test rename of a file to an implicit directory whose parent is implicit. Assert
   * that the rename operation succeeds.
   */
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

    Assertions.assertThat(fs.rename(new Path("/file"), new Path("/dst/dir")))
            .describedAs("Rename of file to implicit directory in implicit parent should succeed")
            .isTrue();
    Assertions.assertThat(fs.exists(new Path("/dst/dir/file")))
            .describedAs("Destination path should exist after rename")
            .isTrue();
    assertPathNotExist(getAbfsBlobClient(fs), "/file", fs);
  }

  /**
   *  Test rename of source directory which contain implicit sub-directories.
   *  Assert that the rename operation succeeds.
   */
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

    Assertions.assertThat(fs.rename(new Path("/src"), new Path("/dst/dir")))
            .describedAs("Rename of directory containing implicit directory should succeed")
            .isTrue();
    Assertions.assertThat(fs.exists(new Path("/dst/dir/subFile")))
            .describedAs("Files on sub-path of the source should move into destination path")
            .isTrue();
    Assertions.assertThat(fs.exists(new Path("/dst/dir/subDir/subFile")))
            .describedAs("Files on sub-directories of the source should move into destination path")
            .isTrue();
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

  /**
   * Utility method that assert the file rename behavior for different scenarios on the
   * basis of the: source-type, source-parent-type, source-subdir-type, destination-type,
   * destination-parent-type, if destination parent exists.
   *
   * @param srcParentExplicit Is source parent explicit.
   * @param srcExplicit Is source explicit.
   * @param srcSubDirExplicit Is source sub-directory explicit.
   * @param dstParentExplicit Is destination parent explicit.
   * @param dstExplicit Is destination explicit.
   * @param dstParentExists Is destination parent exists.
   * @param isDstParentFile Is destination parent a file.
   * @param dstExist Is destination exists.
   * @param isDstFile Is destination a file.
   * @param shouldRenamePass Is rename Operation expected to pass.
   *
   * @throws IOException server error, or error from Az-copy shell run.
   */
  private void explicitImplicitDirectoryRenameTest(Boolean srcParentExplicit,
      Boolean srcExplicit,
      Boolean srcSubDirExplicit,
      Boolean dstParentExplicit,
      Boolean dstExplicit,
      Boolean dstParentExists,
      Boolean isDstParentFile,
      Boolean dstExist,
      Boolean isDstFile,
      Boolean shouldRenamePass) throws IOException {
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

  /**
   * Utility method that assert the directory rename behavior for different scenarios on the
   * basis of the: source-type, source-parent-type, source-subdir-type, destination-type,
   * destination-parent-type, if destination parent exists.
   *
   * @param srcParentExplicit Is source parent explicit.
   * @param srcExplicit Is source explicit.
   * @param srcSubDirExplicit Is source sub-directory explicit.
   * @param dstParentExplicit Is destination parent explicit.
   * @param dstExplicit Is destination explicit.
   * @param dstParentExists Is destination parent exists.
   * @param isDstParentFile Is destination parent a file.
   * @param dstExist Is destination exists.
   * @param isDstFile Is destination a file.
   * @param srcName Source name.
   * @param dstName Destination name.
   * @param dstSubFileName Destination sub-file name.
   * @param dstSubDirName Destination sub-directory name.
   * @param isSubDirExplicit Is destination sub-directory explicit.
   * @param shouldRenamePass Is rename Operation expected to pass.
   *
   */
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

  /**
   * Create paths for source, sourceParent and source-subDirs.
   *
   * @param srcParentExplicit Is source parent explicit.
   * @param srcExplicit Is source explicit.
   * @param srcSubDirExplicit Is source sub-directory explicit.
   * @param fs AzureBlobFileSystem.
   * @param srcParent Source parent path.
   * @param src Source path.
   *
   * @return Pair of list of directories and list of files.
   * @throws IOException server error, or error from Az-copy shell run.
   */
  private Pair<List<Path>, List<Path>> createSourcePaths(final Boolean srcParentExplicit,
      final Boolean srcExplicit,
      final Boolean srcSubDirExplicit,
      final AzureBlobFileSystem fs,
      final Path srcParent,
      final Path src) throws IOException {
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

  /**
   * Creates paths for destination, destinationParent and destination-subDirs.
   *
   * @param dstParentExplicit Is destination parent explicit.
   * @param dstExplicit Is destination explicit.
   * @param dstParentExists Is destination parent exists.
   * @param isDstParentFile Is destination parent a file.
   * @param dstExist Is destination exists.
   * @param isDstFile Is destination a file.
   * @param fs AzureBlobFileSystem.
   * @param dstParent Destination parent path.
   * @param dst Destination path.
   * @param subFileName Sub-file name.
   * @param subDirName Sub-directory name.
   *
   * @return Pair of list of directories and list of files.
   * @throws IOException server error, or error from Az-copy shell run.
   */
  private Pair<List<Path>, List<Path>> createDestinationPaths(final Boolean dstParentExplicit,
      final Boolean dstExplicit,
      final Boolean dstParentExists,
      final Boolean isDstParentFile,
      final Boolean dstExist,
      final Boolean isDstFile,
      final AzureBlobFileSystem fs,
      final Path dstParent,
      final Path dst, final String subFileName, final String subDirName,
      final Boolean isSubDirExplicit) throws IOException {
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
