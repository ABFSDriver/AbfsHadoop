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

import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutResponse;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_DIRECT_READ_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for DFS getLayout with Direct Read data handles.
 */
public class ITestAbfsDirectReadGetLayout
    extends AbstractAbfsIntegrationTest {

  public ITestAbfsDirectReadGetLayout() throws Exception {
    super();
  }

  @Test
  public void testDfsGetLayoutWithDataHandle() throws Exception {
    Configuration configuration =
        new Configuration(getRawConfiguration());

    // Enable requesting a Direct Read data handle on DFS getLayout.
    configuration.setBoolean(FS_AZURE_DIRECT_READ_ENABLED, true);

    try (AzureBlobFileSystem fs =
             (AzureBlobFileSystem) getFileSystem(configuration)) {

      final Path path = new Path("/direct-read-get-layout.bin");
      final byte[] data = new byte[1024];

      for (int i = 0; i < data.length; i++) {
        data[i] = (byte) (i % 256);
      }

      try {
        // Create test data which will be used for the layout request.
        try (FSDataOutputStream out = fs.create(path, true)) {
          out.write(data);
        }

        final AzureBlobFileSystemStore store = fs.getAbfsStore();

        // Get the configured DFS client.
        final AbfsClient client = store.getClient();

        assertThat(client.supportsLayout())
            .describedAs("DFS client should support layout retrieval")
            .isTrue();

        /*
         * Request layout for bytes 0-511.
         *
         * Since Direct Read is enabled, AbfsDfsClient should add:
         *
         *   Range: bytes=0-511
         *   x-ms-include: datahandle
         */
        final AbfsRestOperation operation = client.getBlobLayout(
            path.toString(),
            0,
            511,
            null,
            null,
            getTestTracingContext(fs, false));

        assertThat(operation)
            .describedAs("DFS getLayout operation")
            .isNotNull();

        assertThat(operation.getResult())
            .describedAs("DFS getLayout result")
            .isNotNull();

        InputStream responseStream =
            operation.getResult().getListResultStream();

        assertThat(responseStream)
            .describedAs("DFS getLayout JSON response body")
            .isNotNull();

        BlobLayoutResponse response =
            client.getLayoutParser().parse(responseStream);

        assertThat(response)
            .describedAs("Parsed DFS layout response")
            .isNotNull();

        // Verify that the requested layout range was returned.
        assertThat(response.getRanges())
            .describedAs("DFS layout ranges")
            .isNotEmpty();

        BlobLayoutResponse.Range range =
            response.getRanges().get(0);

        assertThat(range.start())
            .describedAs("Layout range start")
            .isEqualTo(0L);

        assertThat(range.end())
            .describedAs("Layout range end")
            .isEqualTo(511L);

        // Verify that at least one physical endpoint was returned.
        assertThat(response.getEndpoints())
            .describedAs("DFS layout endpoints")
            .isNotEmpty();

        /*
         * The endpointIndex returned with the range should reference one of
         * the endpoints returned by getLayout.
         */
        assertThat(response.getEndpoints().stream()
            .anyMatch(endpoint ->
                endpoint.index() == range.endpointIndex()))
            .describedAs(
                "Range endpointIndex should reference a returned endpoint")
            .isTrue();

        /*
         * Direct Read was explicitly enabled. For an eligible service/account,
         * getLayout should return a Direct Read data handle.
         */
        assertThat(range.hasDataHandle())
            .describedAs(
                "DFS getLayout should return a Direct Read handle")
            .isTrue();

        assertThat(range.dataHandle())
            .describedAs("Direct Read data handle")
            .isNotEmpty();

        assertThat(range.expiresAt())
            .describedAs("Direct Read handle expiry")
            .isGreaterThan(0L);

        assertThat(response.getNextMarker())
            .describedAs(
                "DFS getLayout does not currently paginate layout results")
            .isEmpty();

      } finally {
        fs.delete(path, false);
      }
    }
  }
}