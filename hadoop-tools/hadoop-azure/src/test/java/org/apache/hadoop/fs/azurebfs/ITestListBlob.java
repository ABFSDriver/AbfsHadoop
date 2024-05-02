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
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.BlobProperty;
import org.apache.hadoop.fs.azurebfs.services.PrefixMode;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class ITestListBlob extends
    AbstractAbfsIntegrationTest {

  public ITestListBlob() throws Exception {
    super();
  }

  @Test
  public void testListBlob() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    createBlob(fs, "/dir/");
    List<BlobProperty> blobProperties;
    /*
     * Call getListBlob for a path with isDefinitiveDirSearch = false. Should give
     * results including the directory blob(hdi_isfolder=true).
     */
    blobProperties = fs.getAbfsStore()
        .getListBlobs(new Path("dir"), null, null,
            getTestTracingContext(fs, true), null, false);
    Assertions.assertThat(blobProperties)
        .describedAs(
            "BlobList should match the number of files created in tests + the directory itself")
        .hasSize(11);

    /*
     * Call getListBlob for a path with isDefinitiveDirSearch = false. Should give
     * results excluding the directory blob(hdi_isfolder=true).
     */
    blobProperties = fs.getAbfsStore()
        .getListBlobs(new Path("dir"), null, null,
            getTestTracingContext(fs, true), null, true);
    Assertions.assertThat(blobProperties)
        .describedAs(
            "BlobList should match the number of files created in tests")
        .hasSize(10);

    /*
     * Call getListBlob for a path with isDefinitiveDirSearch = false with
     * maxResult more than the number of exact blobs. Should give results including
     * the directory blob(hdi_isfolder=true).
     */
    blobProperties = fs.getAbfsStore()
        .getListBlobs(new Path("dir"), null, null,
            getTestTracingContext(fs, true), 13, false);
    Assertions.assertThat(blobProperties)
        .describedAs(
            "BlobList should match the number of files created in tests + the directory itself")
        .hasSize(11);

    /*
     * Call getListBlob for a path with isDefinitiveDirSearch = false with
     * maxResult lesser than the number of exact blobs. Should give result size
     * same as the maxResult
     */
    blobProperties = fs.getAbfsStore()
        .getListBlobs(new Path("dir"), null, null,
            getTestTracingContext(fs, true), 5, false);
    Assertions.assertThat(blobProperties)
        .describedAs(
            "BlobList should match the number of maxResult given")
        .hasSize(5);
  }

  @Test
  public void testListBlobWithMarkers() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    createBlob(fs, "/dir/");
    AbfsClient client = fs.getAbfsClient();
    AbfsClient spiedClient = Mockito.spy(client);
    fs.getAbfsStore().setClient(spiedClient);

    /*
     * Server can give lesser number of results. In this case, server will give
     * nextMarker.
     * In this case, server will return one object, expectation is that the client
     * uses nextMarker to make calls for the remaining blobs.
     */
    int count[] = new int[1];
    count[0] = 0;
    Mockito.doAnswer(answer -> {
      String marker = answer.getArgument(0);
      String prefix = answer.getArgument(1);
      String delimiter = answer.getArgument(2);
      TracingContext tracingContext = answer.getArgument(4);
      count[0]++;
      return client.getListBlobs(marker, prefix, delimiter, 1, tracingContext);
    }).when(spiedClient).getListBlobs(Mockito.nullable(String.class),
        Mockito.anyString(), Mockito.anyString(), Mockito.nullable(Integer.class),
        Mockito.any(TracingContext.class));

    List<BlobProperty> blobProperties = fs.getAbfsStore()
        .getListBlobs(new Path("dir"), null, null,
            getTestTracingContext(fs, true), 5, false);
    Assertions.assertThat(blobProperties)
        .describedAs(
            "BlobList should match the number of maxResult given")
        .hasSize(5);
    Assertions.assertThat(count[0])
        .describedAs(
            "Number of calls to backend should be equal to maxResult given")
        .isEqualTo(5);
  }

  @Test
  public void testListBlobWithDelimiter() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    fs.setWorkingDirectory(new Path("/"));
    assumeNonHnsAccountBlobEndpoint(fs);
    List<BlobProperty> blobProperties;

    // Create three levels of hierarchy.
    Path level0 = new Path("a");
    Path level1 = new Path("a/b");
    Path level2 = new Path("a/b/c");
    fs.mkdirs(level0);
    fs.mkdirs(level1);
    fs.mkdirs(level2);

    // Without delimiter, recursive listing will return all the children and sub children
    // There will be no BlobPrefix element and only explicit blobs will be returned
    blobProperties = fs.getAbfsStore()
        .getListBlobs(level0.getParent(), null, null,
            getTestTracingContext(fs, true), null, true);
    Assertions.assertThat(blobProperties)
        .describedAs(
            "BlobList should return all blobs in hierarchy")
        .hasSize(3);

    // With delimiter, non-recursive listing will return only the immediate children
    // There will be repetition of marker blobs.
    blobProperties = fs.getAbfsStore()
        .getListBlobs(level0.getParent(), null, "/",
            getTestTracingContext(fs, true), null, true);
    Assertions.assertThat(blobProperties)
        .describedAs(
            "BlobList should return only immediate Children")
        .hasSize(2);

    // ABFS Listing With delimiter, non-recursive listing will return only the immediate children
    // There will be no repetition of marker blobs.
    FileStatus[] fileStatuses = fs.listStatus(level0.getParent());
    Assertions.assertThat(fileStatuses)
        .describedAs(
            "BlobList should return only immediate Children")
        .hasSize(1);
  }

  private void assumeNonHnsAccountBlobEndpoint(final AzureBlobFileSystem fs) {
    Assume.assumeTrue("To work on only on non-HNS Blob endpoint",
        fs.getAbfsStore().getAbfsConfiguration().getPrefixMode()
            == PrefixMode.BLOB);
  }

  private void createBlob(final AzureBlobFileSystem fs, final String pathString)
      throws IOException {
    int i = 0;
    while (i < 10) {
      fs.create(new Path(pathString + i));
      i++;
    }
  }
}