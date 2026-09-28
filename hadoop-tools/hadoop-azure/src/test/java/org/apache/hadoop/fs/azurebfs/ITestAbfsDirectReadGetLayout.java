/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.ReadType;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutResponse;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsDfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.ReadTarget;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_PARQUET;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_READ_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_DIRECT_READ_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_DIRECT_READ_HANDLE_REFRESH_GRACE_PERIOD_MS;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_DATA_LOCALITY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_READAHEAD_V2;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READ_AHEAD_QUEUE_DEPTH;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READ_POLICY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Integration tests for DFS getLayout and Direct Read data handles.
 */
public class ITestAbfsDirectReadGetLayout extends AbstractAbfsIntegrationTest {

  private static final int FILE_SIZE = 1024;
  private static final int HANDLE_RANGE_START = 0;
  private static final int HANDLE_RANGE_END = 511;
  private static final int HANDLE_RANGE_LENGTH = HANDLE_RANGE_END - HANDLE_RANGE_START + 1;
  private static final int NON_ZERO_RANGE_START = 256;
  private static final int NON_ZERO_RANGE_END = 767;
  private static final int NON_ZERO_RANGE_LENGTH = NON_ZERO_RANGE_END - NON_ZERO_RANGE_START + 1;

  public ITestAbfsDirectReadGetLayout() throws Exception {
    super();
  }

  /**
   * DFS getLayout and Direct Read data handles are only available on the DFS
   * endpoint.
   *
   * @throws Exception if the filesystem cannot be created
   */
  @BeforeEach
  public void assumeDfsEndpoint() throws Exception {
    assumeThat(getFileSystem().getAbfsStore().getClient())
        .as("Direct Read tests require the DFS endpoint")
        .isInstanceOf(AbfsDfsClient.class);
  }

  /**
   * Verifies that DFS getLayout returns layout information and a Direct Read
   * data handle when Direct Read is enabled.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testDfsGetLayoutWithDataHandle() throws Exception {
    Configuration configuration = createConfiguration(true);

    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) getFileSystem(configuration)) {
      Path path = new Path("/direct-read-get-layout.bin");
      byte[] data = createTestData();

      try {
        writeTestFile(fs, path, data);

        AbfsClient client = fs.getAbfsStore().getClient();
        assertThat(client.supportsLayout())
            .describedAs("DFS client should support layout retrieval")
            .isTrue();

        BlobLayoutResponse response = getLayout(client, fs, path, HANDLE_RANGE_START, HANDLE_RANGE_END);
        assertThat(response).describedAs("Parsed DFS layout response").isNotNull();
        assertThat(response.getRanges()).describedAs("DFS layout ranges").isNotEmpty();

        BlobLayoutResponse.Range range = response.getRanges().get(0);
        assertThat(range.start()).describedAs("Layout range start").isEqualTo(HANDLE_RANGE_START);
        assertThat(range.end()).describedAs("Layout range end").isEqualTo(HANDLE_RANGE_END);
        assertThat(response.getEndpoints()).describedAs("DFS layout endpoints").isNotEmpty();
        assertThat(response.getEndpoints().stream()
            .anyMatch(endpoint -> endpoint.index() == range.endpointIndex()))
            .describedAs("Range endpointIndex should reference a returned endpoint")
            .isTrue();
        assertThat(range.hasDataHandle())
            .describedAs("DFS getLayout should return a Direct Read handle")
            .isTrue();
        assertThat(range.dataHandle()).describedAs("Direct Read data handle").isNotEmpty();
        assertThat(range.expiresAt()).describedAs("Direct Read handle expiry").isGreaterThan(0L);
        assertThat(response.getNextMarker())
            .describedAs("DFS getLayout does not currently paginate layout results")
            .isEmpty();
      } finally {
        fs.delete(path, false);
      }
    }
  }

  /**
   * Verifies successful redemption of a Direct Read handle for the complete
   * range associated with the handle.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testDfsReadWithDataHandle() throws Exception {
    executeSuccessfulHandleRead(
        "/direct-read-handle-redemption.bin",
        HANDLE_RANGE_START, HANDLE_RANGE_END, HANDLE_RANGE_START, HANDLE_RANGE_LENGTH);
  }

  /**
   * Verifies that a Direct Read handle can redeem a smaller subrange within
   * the authorized range.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testDfsReadSubrangeWithDataHandle() throws Exception {
    executeSuccessfulHandleRead(
        "/direct-read-handle-subrange.bin",
        HANDLE_RANGE_START, HANDLE_RANGE_END, 128, 128);
  }

  /**
   * Verifies that the last byte of the authorized range can be read.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testDfsReadLastByteWithDataHandle() throws Exception {
    executeSuccessfulHandleRead(
        "/direct-read-handle-last-byte.bin",
        HANDLE_RANGE_START, HANDLE_RANGE_END, HANDLE_RANGE_END, 1);
  }

  /**
   * Verifies that a Direct Read handle issued for a non-zero range can redeem
   * the complete range.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testDfsDirectReadWithNonZeroHandleRange() throws Exception {
    executeSuccessfulHandleRead(
        "/direct-read-non-zero-range.bin",
        NON_ZERO_RANGE_START, NON_ZERO_RANGE_END, NON_ZERO_RANGE_START, NON_ZERO_RANGE_LENGTH);
  }

  /**
   * Verifies the first byte of a non-zero authorized range.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testDfsDirectReadFirstByteOfNonZeroRange() throws Exception {
    executeSuccessfulHandleRead(
        "/direct-read-first-byte-non-zero-range.bin",
        NON_ZERO_RANGE_START, NON_ZERO_RANGE_END, NON_ZERO_RANGE_START, 1);
  }

  /**
   * Verifies the inclusive upper boundary of a non-zero authorized range.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testDfsDirectReadLastByteOfNonZeroRange() throws Exception {
    executeSuccessfulHandleRead(
        "/direct-read-last-byte-non-zero-range.bin",
        NON_ZERO_RANGE_START, NON_ZERO_RANGE_END, NON_ZERO_RANGE_END, 1);
  }

  /**
   * Verifies redemption of an unaligned subrange within an authorized range.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testDfsDirectReadUnalignedSubrange() throws Exception {
    executeSuccessfulHandleRead(
        "/direct-read-unaligned-subrange.bin",
        NON_ZERO_RANGE_START, NON_ZERO_RANGE_END, 333, 128);
  }

  /**
   * Verifies that the same Direct Read handle can be redeemed repeatedly for
   * sequential ranges.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testDfsDirectReadSequentialRangesWithSameHandle() throws Exception {
    Configuration configuration = createConfiguration(true);

    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) getFileSystem(configuration)) {
      Path path = new Path("/direct-read-sequential-ranges.bin");
      byte[] data = createTestData();

      try {
        writeTestFile(fs, path, data);

        AbfsClient client = fs.getAbfsStore().getClient();
        BlobLayoutResponse response =
            getLayout(client, fs, path, NON_ZERO_RANGE_START, NON_ZERO_RANGE_END);
        BlobLayoutResponse.Range range = getFirstRangeWithHandle(response);
        String endpoint = getEndpoint(response, range.endpointIndex());

        int[][] reads = {{256, 128}, {384, 128}, {512, 128}, {640, 128}};

        for (int[] read : reads) {
          int readStart = read[0];
          int readLength = read[1];

          ReadTarget readTarget = new ReadTarget(endpoint, range.dataHandle(), readLength);
          byte[] readBuffer = new byte[readLength];

          AbfsRestOperation operation =
              readWithTarget(client, fs, path, readStart, readBuffer, readTarget);

          assertSuccessfulRead(operation, readLength);
          assertThat(readBuffer)
              .describedAs("Sequential Direct Read data at offset " + readStart)
              .containsExactly(Arrays.copyOfRange(data, readStart, readStart + readLength));
        }
      } finally {
        fs.delete(path, false);
      }
    }
  }

  /**
   * Verifies that the same handle can be used for overlapping reads.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testDfsDirectReadOverlappingRangesWithSameHandle() throws Exception {
    Configuration configuration = createConfiguration(true);

    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) getFileSystem(configuration)) {
      Path path = new Path("/direct-read-overlapping-ranges.bin");
      byte[] data = createTestData();

      try {
        writeTestFile(fs, path, data);

        AbfsClient client = fs.getAbfsStore().getClient();
        BlobLayoutResponse response =
            getLayout(client, fs, path, NON_ZERO_RANGE_START, NON_ZERO_RANGE_END);
        BlobLayoutResponse.Range range = getFirstRangeWithHandle(response);
        String endpoint = getEndpoint(response, range.endpointIndex());

        int[][] reads = {{300, 100}, {350, 100}};

        for (int[] read : reads) {
          int readStart = read[0];
          int readLength = read[1];

          ReadTarget readTarget = new ReadTarget(endpoint, range.dataHandle(), readLength);
          byte[] readBuffer = new byte[readLength];

          AbfsRestOperation operation =
              readWithTarget(client, fs, path, readStart, readBuffer, readTarget);

          assertSuccessfulRead(operation, readLength);
          assertThat(readBuffer)
              .describedAs("Overlapping Direct Read data at offset " + readStart)
              .containsExactly(Arrays.copyOfRange(data, readStart, readStart + readLength));
        }
      } finally {
        fs.delete(path, false);
      }
    }
  }

  /**
   * Verifies that a handle issued for bytes 0-511 cannot read the first byte
   * outside the authorized range.
   *
   * @throws Exception if test setup fails
   */
  @Test
  public void testDfsReadOutsideDataHandleRangeFails() throws Exception {
    executeRejectedHandleRead(
        "/direct-read-outside-range.bin",
        HANDLE_RANGE_START, HANDLE_RANGE_END, HANDLE_RANGE_END + 1L, 1,
        "A Direct Read handle must not authorize bytes outside its range");
  }

  /**
   * Verifies that a handle cannot read the byte immediately before its
   * authorized non-zero range.
   *
   * @throws Exception if test setup fails
   */
  @Test
  public void testDfsDirectReadBeforeHandleStartFails() throws Exception {
    executeRejectedHandleRead(
        "/direct-read-before-handle-range.bin",
        NON_ZERO_RANGE_START, NON_ZERO_RANGE_END, NON_ZERO_RANGE_START - 1L, 1,
        "A Direct Read handle must not authorize bytes before its range");
  }

  /**
   * Verifies that a read which starts inside the authorized range but crosses
   * the upper boundary is rejected.
   *
   * @throws Exception if test setup fails
   */
  @Test
  public void testDfsDirectReadCrossingHandleEndFails() throws Exception {
    executeRejectedHandleRead(
        "/direct-read-crossing-range-end.bin",
        NON_ZERO_RANGE_START, NON_ZERO_RANGE_END, 700, 101,
        "A Direct Read request crossing the handle range must fail");
  }

  /**
   * Verifies that a modified Direct Read handle is rejected.
   *
   * @throws Exception if test setup fails
   */
  @Test
  public void testDfsReadWithInvalidDataHandleFails() throws Exception {
    Configuration configuration = createConfiguration(true);

    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) getFileSystem(configuration)) {
      Path path = new Path("/direct-read-invalid-handle.bin");
      byte[] data = createTestData();

      try {
        writeTestFile(fs, path, data);

        AbfsClient client = fs.getAbfsStore().getClient();
        BlobLayoutResponse response = getLayout(client, fs, path, HANDLE_RANGE_START, HANDLE_RANGE_END);
        BlobLayoutResponse.Range range = getFirstRangeWithHandle(response);
        String endpoint = getEndpoint(response, range.endpointIndex());

        ReadTarget invalidReadTarget =
            new ReadTarget(endpoint, range.dataHandle() + "-invalid", HANDLE_RANGE_LENGTH);
        byte[] readBuffer = new byte[HANDLE_RANGE_LENGTH];

        assertThatThrownBy(() ->
            readWithTarget(client, fs, path, HANDLE_RANGE_START, readBuffer, invalidReadTarget))
            .describedAs("A modified Direct Read handle must be rejected")
            .isInstanceOf(Exception.class);
      } finally {
        fs.delete(path, false);
      }
    }
  }

  /**
   * Verifies that a null ReadTarget falls back to the ordinary DFS read path.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testDfsReadWithNullReadTargetFallsBack() throws Exception {
    Configuration configuration = createConfiguration(true);

    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) getFileSystem(configuration)) {
      Path path = new Path("/direct-read-null-target-fallback.bin");
      byte[] data = createTestData();

      try {
        writeTestFile(fs, path, data);

        AbfsClient client = fs.getAbfsStore().getClient();
        byte[] readBuffer = new byte[HANDLE_RANGE_LENGTH];

        AbfsRestOperation readOperation = client.read(
            path.toString(), HANDLE_RANGE_START, readBuffer, 0, readBuffer.length,
            "*", null, null, getTestTracingContext(fs, false), null);

        assertSuccessfulRead(readOperation, HANDLE_RANGE_LENGTH);
        assertThat(readBuffer)
            .describedAs("Data returned through normal DFS read fallback")
            .containsExactly(Arrays.copyOfRange(data, HANDLE_RANGE_START, HANDLE_RANGE_END + 1));
      } finally {
        fs.delete(path, false);
      }
    }
  }

  /**
   * Verifies that a ReadTarget without a handle falls back to the ordinary
   * DFS read path.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testDfsReadWithoutDataHandleFallsBack() throws Exception {
    Configuration configuration = createConfiguration(true);

    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) getFileSystem(configuration)) {
      Path path = new Path("/direct-read-no-handle-fallback.bin");
      byte[] data = createTestData();

      try {
        writeTestFile(fs, path, data);

        AbfsClient client = fs.getAbfsStore().getClient();
        BlobLayoutResponse response = getLayout(client, fs, path, HANDLE_RANGE_START, HANDLE_RANGE_END);
        BlobLayoutResponse.Range range = response.getRanges().get(0);
        String endpoint = getEndpoint(response, range.endpointIndex());

        ReadTarget readTargetWithoutHandle = new ReadTarget(endpoint, null, HANDLE_RANGE_LENGTH);
        byte[] readBuffer = new byte[HANDLE_RANGE_LENGTH];

        AbfsRestOperation readOperation = client.read(
            path.toString(), HANDLE_RANGE_START, readBuffer, 0, readBuffer.length,
            "*", null, null, getTestTracingContext(fs, false), readTargetWithoutHandle);

        assertSuccessfulRead(readOperation, HANDLE_RANGE_LENGTH);
        assertThat(readBuffer)
            .describedAs("Data returned after no-handle fallback")
            .containsExactly(Arrays.copyOfRange(data, HANDLE_RANGE_START, HANDLE_RANGE_END + 1));
      } finally {
        fs.delete(path, false);
      }
    }
  }

  /**
   * Verifies that getLayout does not request a data handle when Direct Read
   * is disabled.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testDfsGetLayoutWithoutDataHandleWhenDirectReadDisabled() throws Exception {
    Configuration configuration = createConfiguration(false);

    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) getFileSystem(configuration)) {
      Path path = new Path("/direct-read-disabled-get-layout.bin");
      byte[] data = createTestData();

      try {
        writeTestFile(fs, path, data);

        AbfsClient client = fs.getAbfsStore().getClient();
        BlobLayoutResponse response = getLayout(client, fs, path, HANDLE_RANGE_START, HANDLE_RANGE_END);

        assertThat(response)
            .describedAs("Layout should still be returned when Direct Read is disabled")
            .isNotNull();
        assertThat(response.getRanges()).describedAs("DFS layout ranges").isNotEmpty();
        assertThat(response.getEndpoints()).describedAs("DFS layout endpoints").isNotEmpty();
        assertThat(response.getRanges())
            .describedAs("No range should contain a Direct Read handle")
            .allMatch(range -> !range.hasDataHandle());
      } finally {
        fs.delete(path, false);
      }
    }
  }

  /**
   * Verifies that a newly issued Direct Read handle has an expiry in the
   * future and has positive remaining validity.
   *
   * <p>This test deliberately does not assume a fixed lifetime because the
   * service owns the expiry duration.</p>
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testDfsDataHandleHasFutureExpiry() throws Exception {
    Configuration configuration = createConfiguration(true);

    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) getFileSystem(configuration)) {
      Path path = new Path("/direct-read-handle-expiry.bin");
      byte[] data = createTestData();

      try {
        writeTestFile(fs, path, data);

        AbfsClient client = fs.getAbfsStore().getClient();
        long beforeLayoutRequest = System.currentTimeMillis();

        BlobLayoutResponse response = getLayout(client, fs, path, HANDLE_RANGE_START, HANDLE_RANGE_END);
        BlobLayoutResponse.Range range = getFirstRangeWithHandle(response);

        assertThat(range.expiresAt())
            .describedAs("New Direct Read handle should expire in the future")
            .isGreaterThan(beforeLayoutRequest);

        long remainingValidityMillis = range.expiresAt() - System.currentTimeMillis();
        assertThat(remainingValidityMillis)
            .describedAs("New Direct Read handle should have remaining validity")
            .isPositive();
      } finally {
        fs.delete(path, false);
      }
    }
  }

  /**
   * Verifies the complete Direct Read path through AbfsInputStream.
   *
   * <p>The test intentionally does not call getBlobLayout or manually construct
   * a ReadTarget. The stream must retrieve and use the layout automatically.</p>
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testDfsDirectReadThroughAbfsInputStream() throws Exception {
    Configuration configuration = createConfiguration(true);

    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) getFileSystem(configuration)) {
      Path path = new Path("/direct-read-input-stream.bin");
      byte[] expected = createTestData();

      try {
        writeTestFile(fs, path, expected);

        byte[] actual = new byte[expected.length];

        try (FSDataInputStream inputStream = fs.open(path)) {
          inputStream.readFully(0, actual);
        }

        assertThat(actual)
            .describedAs("Data read through the AbfsInputStream Direct Read path")
            .containsExactly(expected);
      } finally {
        fs.delete(path, false);
      }
    }
  }

  /**
   * Verifies that Direct Read data handles work correctly with read-ahead.
   *
   * <p>The test enables Data Locality, Direct Read, and read-ahead, then reads
   * sequentially from a real file. It verifies that the returned data is
   * correct and that at least one prefetch request used a valid Direct Read
   * data handle.</p>
   *
   * @throws Exception if filesystem creation, file creation, reading, or
   *                   validation fails
   */
  @Test
  public void testDirectReadWithDataHandleAndReadAhead() throws Exception {
    final int oneMb = 1024 * 1024;
    final int fileSize = 16 * oneMb;
    final int readSize = 4 * oneMb;

    Configuration configuration = createConfiguration(true);
    configuration.setInt(FS_AZURE_READ_AHEAD_QUEUE_DEPTH, 2);
    configuration.setInt(AZURE_READ_BUFFER_SIZE, readSize);

    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem
        .newInstance(getFileSystem().getUri(), configuration)) {

      assumeThat(fs.getAbfsStore().getAbfsConfiguration().isDataLocalityEnabled())
          .as("Data Locality must be enabled")
          .isTrue();
      assumeThat(fs.getAbfsStore().getAbfsConfiguration().isDirectReadEnabled())
          .as("Direct Read must be enabled")
          .isTrue();

      Path testPath = new Path(
          "/direct-read-with-read-ahead-" + UUID.randomUUID() + ".bin");
      byte[] expected = createPatternData(fileSize);

      AzureBlobFileSystemStore store = fs.getAbfsStore();
      AbfsClient client = Mockito.spy(store.getClient());
      setAbfsClient(store, client);

      try {
        writeTestFile(fs, testPath, expected);

        assertThat(fs.getFileStatus(testPath).getLen())
            .as("Direct Read read-ahead test file size")
            .isEqualTo(fileSize);

        Mockito.clearInvocations(client);

        byte[] actual = new byte[fileSize];

        try (FSDataInputStream inputStream = fs.open(testPath)) {
          int totalBytesRead = 0;

          while (totalBytesRead < actual.length) {
            int bytesRead = inputStream.read(
                actual, totalBytesRead, actual.length - totalBytesRead);
            if (bytesRead < 0) {
              break;
            }
            totalBytesRead += bytesRead;
          }

          assertThat(totalBytesRead)
              .as("Total bytes read")
              .isEqualTo(fileSize);
        }

        // One array comparison instead of one assertion per byte.
        assertThat(actual)
            .as("Data read with Direct Read and read-ahead")
            .containsExactly(expected);

        /*
         * Capture the reads issued by both the foreground read path and
         * read-ahead workers.
         */
        ArgumentCaptor<TracingContext> tracingCaptor =
            ArgumentCaptor.forClass(TracingContext.class);
        ArgumentCaptor<ReadTarget> targetCaptor =
            ArgumentCaptor.forClass(ReadTarget.class);

        verify(client, atLeastOnce()).read(
            nullable(String.class),
            anyLong(),
            nullable(byte[].class),
            anyInt(),
            anyInt(),
            nullable(String.class),
            nullable(String.class),
            nullable(ContextEncryptionAdapter.class),
            tracingCaptor.capture(),
            targetCaptor.capture());

        List<TracingContext> tracingContexts = tracingCaptor.getAllValues();
        List<ReadTarget> readTargets = targetCaptor.getAllValues();

        assertThat(readTargets)
            .as("Direct Read with read-ahead should issue handle-backed reads")
            .anySatisfy(target -> {
              assertThat(target).as("Read-ahead Direct Read target").isNotNull();
              assertThat(target.hasHandle())
                  .as("Read-ahead request should contain a Direct Read data handle")
                  .isTrue();
              assertThat(target.handle())
                  .as("Read-ahead Direct Read data handle")
                  .isNotBlank();
            });

        assertThat(tracingContexts)
            .as("Read-ahead should issue a prefetch read")
            .anySatisfy(context ->
                assertThat(context.getReadType()).isEqualTo(ReadType.PREFETCH_READ));

        assertThat(hasHandleBackedPrefetch(tracingContexts, readTargets))
            .as("At least one read-ahead prefetch request should use a Direct Read data handle")
            .isTrue();
      } finally {
        fs.delete(testPath, false);
      }
    }
  }

  /**
   * Verifies that ReadAhead V2 child reads, built from layout segments, carry
   * a valid Direct Read data handle and return correct data.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testDirectReadWithDataHandleAndReadAheadV2() throws Exception {
    final int oneMb = 1024 * 1024;
    final int fileSize = 16 * oneMb;
    final int readSize = 4 * oneMb;

    Configuration configuration = createConfiguration(true);
    configuration.setBoolean(FS_AZURE_ENABLE_READAHEAD_V2, true);
    configuration.setInt(FS_AZURE_READ_AHEAD_QUEUE_DEPTH, 2);
    configuration.setInt(AZURE_READ_BUFFER_SIZE, readSize);

    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(
        getFileSystem().getUri(), configuration)) {
      assumeThat(fs.getAbfsStore().getAbfsConfiguration().isDataLocalityEnabled())
          .as("Data Locality must be enabled")
          .isTrue();
      assumeThat(fs.getAbfsStore().getAbfsConfiguration().isDirectReadEnabled())
          .as("Direct Read must be enabled")
          .isTrue();

      Path path = new Path("/direct-read-readahead-v2-" + UUID.randomUUID() + ".bin");
      byte[] data = createPatternData(fileSize);

      try {
        writeTestFile(fs, path, data);

        AzureBlobFileSystemStore store = fs.getAbfsStore();
        AbfsClient client = Mockito.spy(store.getClient());
        setAbfsClient(store, client);

        byte[] actual = new byte[fileSize];
        try (FSDataInputStream in = fs.open(path)) {
          in.readFully(0, actual);
        }
        assertThat(actual)
            .as("Data read with ReadAhead V2 and Direct Read")
            .containsExactly(data);

        ArgumentCaptor<TracingContext> tracingCaptor =
            ArgumentCaptor.forClass(TracingContext.class);
        ArgumentCaptor<ReadTarget> targetCaptor =
            ArgumentCaptor.forClass(ReadTarget.class);
        verify(client, atLeastOnce()).read(nullable(String.class), anyLong(),
            nullable(byte[].class), anyInt(), anyInt(), nullable(String.class),
            nullable(String.class), nullable(ContextEncryptionAdapter.class),
            tracingCaptor.capture(), targetCaptor.capture());

        assertThat(hasHandleBackedPrefetch(
            tracingCaptor.getAllValues(), targetCaptor.getAllValues()))
            .as("At least one ReadAhead V2 child read should carry a data handle")
            .isTrue();
      } finally {
        fs.delete(path, false);
      }
    }
  }

  /**
   * Verifies that positioned reads through AbfsInputStream each carry a
   * valid Direct Read data handle and return the correct data.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testPositionedReadsThroughStreamUseDataHandle() throws Exception {
    final int oneMb = 1024 * 1024;
    final int fileSize = 8 * oneMb;
    final int readSize = oneMb;
    final long[] positions = {0L, 2L * oneMb, 4L * oneMb, 6L * oneMb};

    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(
        getFileSystem().getUri(), createPositionedReadConfiguration(readSize))) {
      assumeThat(fs.getAbfsStore().getAbfsConfiguration().isDirectReadEnabled())
          .as("Direct Read must be enabled")
          .isTrue();

      Path path = new Path("/direct-read-positioned-" + UUID.randomUUID() + ".bin");
      byte[] data = createPatternData(fileSize);

      try {
        writeTestFile(fs, path, data);

        AzureBlobFileSystemStore store = fs.getAbfsStore();
        AbfsClient client = Mockito.spy(store.getClient());
        setAbfsClient(store, client);

        try (FSDataInputStream in = fs.open(path)) {
          for (long position : positions) {
            byte[] buffer = new byte[readSize];
            in.readFully(position, buffer);
            assertThat(buffer)
                .as("Data at position %s", position)
                .containsExactly(Arrays.copyOfRange(
                    data, (int) position, (int) position + readSize));
          }
        }

        assertThat(captureReadTargets(client))
            .as("Every positioned read should use a Direct Read handle")
            .isNotEmpty()
            .allSatisfy(target -> {
              assertThat(target).isNotNull();
              assertThat(target.hasHandle()).isTrue();
              assertThat(target.handle()).isNotBlank();
            });
      } finally {
        fs.delete(path, false);
      }
    }
  }

  /**
   * Verifies that a stream with Direct Read disabled still uses Data Locality
   * endpoints but never sends a data handle.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testStreamWithDirectReadDisabledNeverSendsHandle() throws Exception {
    final int oneMb = 1024 * 1024;
    final int fileSize = 4 * oneMb;
    final int readSize = oneMb;

    Configuration configuration = createPositionedReadConfiguration(readSize);
    configuration.setBoolean(FS_AZURE_DIRECT_READ_ENABLED, false);

    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(
        getFileSystem().getUri(), configuration)) {
      assumeThat(fs.getAbfsStore().getAbfsConfiguration().isDataLocalityEnabled())
          .as("Data Locality must be enabled")
          .isTrue();
      assumeThat(fs.getAbfsStore().getAbfsConfiguration().isDirectReadEnabled())
          .as("Direct Read must be disabled")
          .isFalse();

      Path path = new Path("/direct-read-disabled-stream-" + UUID.randomUUID() + ".bin");
      byte[] data = createPatternData(fileSize);

      try {
        writeTestFile(fs, path, data);

        AzureBlobFileSystemStore store = fs.getAbfsStore();
        AbfsClient client = Mockito.spy(store.getClient());
        setAbfsClient(store, client);

        byte[] buffer = new byte[readSize];
        try (FSDataInputStream in = fs.open(path)) {
          in.readFully(0, buffer);
        }
        assertThat(buffer).containsExactly(Arrays.copyOf(data, readSize));

        assertThat(captureReadTargets(client))
            .as("Direct Read disabled: no read may carry x-ms-data-handle")
            .isNotEmpty()
            .allSatisfy(target -> assertThat(target.hasHandle()).isFalse());
      } finally {
        fs.delete(path, false);
      }
    }
  }

  /**
   * Against the live service: a handle inside the refresh window is replaced
   * by a new one on the next read.
   *
   * <p>Handles last about 300 s. With a 290 s grace period, a handle becomes
   * eligible for refresh about 10 s after it is issued, so the test does not
   * have to wait for real expiry.</p>
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testDataHandleRefreshedInsideGracePeriod() throws Exception {
    final int oneMb = 1024 * 1024;
    final int fileSize = 8 * oneMb;
    final int readSize = oneMb;

    Configuration configuration = createPositionedReadConfiguration(readSize);
    configuration.setLong(FS_AZURE_DIRECT_READ_HANDLE_REFRESH_GRACE_PERIOD_MS,
        290_000L);

    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(
        getFileSystem().getUri(), configuration)) {
      assumeThat(fs.getAbfsStore().getAbfsConfiguration().isDirectReadEnabled())
          .as("Direct Read must be enabled")
          .isTrue();

      Path path = new Path("/direct-read-refresh-" + UUID.randomUUID() + ".bin");
      byte[] data = createPatternData(fileSize);

      try {
        writeTestFile(fs, path, data);

        AzureBlobFileSystemStore store = fs.getAbfsStore();
        AbfsClient client = Mockito.spy(store.getClient());
        setAbfsClient(store, client);

        try (FSDataInputStream in = fs.open(path)) {
          // Fetches the layout and the first handle.
          byte[] first = new byte[readSize];
          in.readFully(0, first);
          assertThat(first).containsExactly(Arrays.copyOf(data, readSize));

          // Wait until the handle is inside the refresh window. The extra
          // 5 s covers one-second expiry precision.
          Thread.sleep(TimeUnit.SECONDS.toMillis(15));

          // The cached range is still present, but its handle is due for
          // refresh, so this read must fetch a new layout first.
          byte[] second = new byte[readSize];
          in.readFully(4L * oneMb, second);
          assertThat(second).containsExactly(
              Arrays.copyOfRange(data, 4 * oneMb, 5 * oneMb));
        }

        verifyLayoutFetches(client, 2);

        List<ReadTarget> targets = captureReadTargets(client);
        assertThat(targets)
            .as("Every read should carry a handle")
            .allSatisfy(t -> assertThat(t.hasHandle()).isTrue());
        assertThat(targets.get(targets.size() - 1).handle())
            .as("The read after the refresh should use a new handle")
            .isNotEqualTo(targets.get(0).handle());
      } finally {
        fs.delete(path, false);
      }
    }
  }

  /**
   * Against the live service: when the service rejects a handle, the stream
   * fetches a new one, retries once, and returns the correct data.
   *
   * <p>The first read that carries a handle is sent with a tampered handle.
   * The service rejects it with 400 InvalidDataHandle.</p>
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testRejectedHandleRecoveredWithFreshHandle() throws Exception {
    final int oneMb = 1024 * 1024;
    final int fileSize = 4 * oneMb;
    final int readSize = oneMb;

    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(
        getFileSystem().getUri(), createPositionedReadConfiguration(readSize))) {
      assumeThat(fs.getAbfsStore().getAbfsConfiguration().isDirectReadEnabled())
          .as("Direct Read must be enabled")
          .isTrue();

      Path path = new Path("/direct-read-reject-" + UUID.randomUUID() + ".bin");
      byte[] data = createPatternData(fileSize);

      try {
        writeTestFile(fs, path, data);

        AzureBlobFileSystemStore store = fs.getAbfsStore();
        // Keep the real client: the spy delegates the actual request to it.
        AbfsClient realClient = store.getClient();
        AbfsClient client = Mockito.spy(realClient);
        setAbfsClient(store, client);

        AtomicBoolean tampered = new AtomicBoolean(false);
        List<ReadTarget> sentTargets = new CopyOnWriteArrayList<>();

        // doAnswer(...).when(...) registers the stub without calling read().
        doAnswer(invocation -> {
          ReadTarget target = invocation.getArgument(9);
          ReadTarget toSend = target;
          if (target != null && target.hasHandle()
              && tampered.compareAndSet(false, true)) {
            toSend = new ReadTarget(target.endpoint(),
                target.handle() + "-tampered", target.maxLength());
          }
          sentTargets.add(toSend);
          return realClient.read(
              invocation.getArgument(0),
              invocation.getArgument(1),
              invocation.getArgument(2),
              invocation.getArgument(3),
              invocation.getArgument(4),
              invocation.getArgument(5),
              invocation.getArgument(6),
              invocation.getArgument(7),
              invocation.getArgument(8),
              toSend);
        }).when(client).read(nullable(String.class), anyLong(),
            nullable(byte[].class), anyInt(), anyInt(), nullable(String.class),
            nullable(String.class), nullable(ContextEncryptionAdapter.class),
            nullable(TracingContext.class), nullable(ReadTarget.class));

        byte[] buffer = new byte[readSize];
        try (FSDataInputStream in = fs.open(path)) {
          in.readFully(0, buffer);
        }

        assertThat(buffer)
            .as("Read should succeed with correct data after recovery")
            .containsExactly(Arrays.copyOf(data, readSize));

        assertThat(sentTargets)
            .as("One tampered attempt followed by one retry")
            .hasSize(2);
        assertThat(sentTargets.get(0).handle())
            .as("First attempt carried the tampered handle")
            .endsWith("-tampered");
        assertThat(sentTargets.get(1).hasHandle())
            .as("The retry should carry a fresh handle")
            .isTrue();
        assertThat(sentTargets.get(1).handle())
            .as("The retry must not reuse the rejected handle")
            .doesNotEndWith("-tampered");

        // The initial fetch plus one refresh after the rejection.
        verifyLayoutFetches(client, 2);
      } finally {
        fs.delete(path, false);
      }
    }
  }

  // ---------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------

  /**
   * Executes a successful handle-backed read and validates the returned data.
   *
   * @param pathName test path
   * @param handleStart handle-authorized start
   * @param handleEnd handle-authorized end
   * @param readStart requested read start
   * @param readLength requested read length
   * @throws Exception if the test fails
   */
  private void executeSuccessfulHandleRead(
      final String pathName, final int handleStart, final int handleEnd,
      final int readStart, final int readLength) throws Exception {

    Configuration configuration = createConfiguration(true);

    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) getFileSystem(configuration)) {
      Path path = new Path(pathName);
      byte[] data = createTestData();

      try {
        writeTestFile(fs, path, data);

        AbfsClient client = fs.getAbfsStore().getClient();
        BlobLayoutResponse response = getLayout(client, fs, path, handleStart, handleEnd);
        BlobLayoutResponse.Range range = getFirstRangeWithHandle(response);

        assertThat(range.start()).describedAs("Handle-authorized range start").isEqualTo(handleStart);
        assertThat(range.end()).describedAs("Handle-authorized range end").isEqualTo(handleEnd);

        ReadTarget readTarget = createReadTarget(response, range, readLength);
        byte[] readBuffer = new byte[readLength];

        AbfsRestOperation readOperation =
            readWithTarget(client, fs, path, readStart, readBuffer, readTarget);

        assertSuccessfulRead(readOperation, readLength);
        assertThat(readBuffer)
            .describedAs("Data returned through DFS Direct Read")
            .containsExactly(Arrays.copyOfRange(data, readStart, readStart + readLength));
      } finally {
        fs.delete(path, false);
      }
    }
  }

  /**
   * Executes a read expected to be rejected because it is outside the
   * handle-authorized range.
   *
   * @param pathName test path
   * @param handleStart handle-authorized start
   * @param handleEnd handle-authorized end
   * @param readStart requested read start
   * @param readLength requested read length
   * @param description assertion description
   * @throws Exception if test setup fails
   */
  private void executeRejectedHandleRead(
      final String pathName, final int handleStart, final int handleEnd,
      final long readStart, final int readLength, final String description) throws Exception {

    Configuration configuration = createConfiguration(true);

    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) getFileSystem(configuration)) {
      Path path = new Path(pathName);
      byte[] data = createTestData();

      try {
        writeTestFile(fs, path, data);

        AbfsClient client = fs.getAbfsStore().getClient();
        BlobLayoutResponse response = getLayout(client, fs, path, handleStart, handleEnd);
        BlobLayoutResponse.Range range = getFirstRangeWithHandle(response);
        ReadTarget readTarget = createReadTarget(response, range, readLength);
        byte[] readBuffer = new byte[readLength];

        assertThatThrownBy(() ->
            readWithTarget(client, fs, path, readStart, readBuffer, readTarget))
            .describedAs(description)
            .isInstanceOf(Exception.class);
      } finally {
        fs.delete(path, false);
      }
    }
  }

  /**
   * Returns whether any captured read was a prefetch read that carried a
   * non-empty data handle. The two lists are index-aligned.
   *
   * @param contexts captured tracing contexts
   * @param targets captured read targets
   * @return true if at least one prefetch read carried a handle
   */
  private static boolean hasHandleBackedPrefetch(
      final List<TracingContext> contexts, final List<ReadTarget> targets) {
    for (int i = 0; i < contexts.size(); i++) {
      TracingContext context = contexts.get(i);
      ReadTarget target = targets.get(i);
      if (context != null
          && context.getReadType() == ReadType.PREFETCH_READ
          && target != null
          && target.hasHandle()
          && !target.handle().isEmpty()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Creates an ABFS configuration with Data Locality enabled and the requested
   * Direct Read state.
   *
   * @param directReadEnabled whether Direct Read should be enabled
   * @return test configuration
   */
  private Configuration createConfiguration(final boolean directReadEnabled) {
    Configuration configuration = new Configuration(getRawConfiguration());
    configuration.setBoolean(FS_AZURE_ENABLE_DATA_LOCALITY, true);
    configuration.setBoolean(FS_AZURE_DIRECT_READ_ENABLED, directReadEnabled);
    return configuration;
  }

  /**
   * Creates a configuration where every positioned read goes straight to
   * the service: Direct Read on, no read-ahead, and random-read policy.
   *
   * @param readSize read buffer size
   * @return test configuration
   */
  private Configuration createPositionedReadConfiguration(final int readSize) {
    Configuration configuration = createConfiguration(true);
    configuration.setInt(FS_AZURE_READ_AHEAD_QUEUE_DEPTH, 0);
    configuration.setInt(AZURE_READ_BUFFER_SIZE, readSize);
    configuration.set(FS_AZURE_READ_POLICY, FS_OPTION_OPENFILE_READ_POLICY_PARQUET);
    return configuration;
  }

  /**
   * Calls DFS getLayout and parses the JSON response.
   *
   * @param client DFS client
   * @param fs filesystem
   * @param path test file path
   * @param start requested range start
   * @param end requested range end
   * @return parsed layout response
   * @throws Exception if retrieval or parsing fails
   */
  private BlobLayoutResponse getLayout(
      final AbfsClient client, final AzureBlobFileSystem fs, final Path path,
      final long start, final long end) throws Exception {

    AbfsRestOperation operation = client.getBlobLayout(
        path.toString(), start, end, null, null, getTestTracingContext(fs, false));

    assertThat(operation).describedAs("DFS getLayout operation").isNotNull();
    assertThat(operation.getResult()).describedAs("DFS getLayout result").isNotNull();

    InputStream responseStream = operation.getResult().getListResultStream();
    assertThat(responseStream).describedAs("DFS getLayout JSON response body").isNotNull();

    BlobLayoutResponse response = client.getLayoutParser().parse(responseStream);
    assertThat(response).describedAs("Parsed DFS layout response").isNotNull();

    return response;
  }

  /**
   * Returns the first layout range and verifies that it contains a usable
   * Direct Read handle.
   *
   * @param response parsed layout response
   * @return first layout range
   */
  private BlobLayoutResponse.Range getFirstRangeWithHandle(final BlobLayoutResponse response) {
    assertThat(response).describedAs("Parsed DFS layout response").isNotNull();
    assertThat(response.getRanges()).describedAs("DFS layout ranges").isNotEmpty();

    BlobLayoutResponse.Range range = response.getRanges().get(0);

    assertThat(range.hasDataHandle())
        .describedAs("Layout range should contain a Direct Read handle")
        .isTrue();
    assertThat(range.dataHandle()).describedAs("Direct Read data handle").isNotEmpty();
    assertThat(range.expiresAt()).describedAs("Direct Read handle expiry").isGreaterThan(0L);

    return range;
  }

  /**
   * Creates a ReadTarget from a layout response and selected range.
   *
   * @param response parsed layout response
   * @param range selected layout range
   * @param maxLength maximum read length
   * @return Direct Read target
   */
  private ReadTarget createReadTarget(
      final BlobLayoutResponse response, final BlobLayoutResponse.Range range, final int maxLength) {
    String endpoint = getEndpoint(response, range.endpointIndex());
    return new ReadTarget(endpoint, range.dataHandle(), maxLength);
  }

  /**
   * Resolves an endpoint by index.
   *
   * @param response parsed layout response
   * @param endpointIndex endpoint index
   * @return endpoint value
   */
  private String getEndpoint(final BlobLayoutResponse response, final int endpointIndex) {
    String endpoint = response.getEndpoints().stream()
        .filter(value -> value.index() == endpointIndex)
        .map(BlobLayoutResponse.Endpoint::value)
        .findFirst()
        .orElseThrow();

    assertThat(endpoint).describedAs("Endpoint referenced by the layout range").isNotEmpty();

    return endpoint;
  }

  /**
   * Executes a handle-backed DFS read.
   *
   * @param client DFS client
   * @param fs filesystem
   * @param path test path
   * @param position read position
   * @param readBuffer destination buffer
   * @param readTarget read endpoint and handle
   * @return executed operation
   * @throws Exception if the read fails
   */
  private AbfsRestOperation readWithTarget(
      final AbfsClient client, final AzureBlobFileSystem fs, final Path path,
      final long position, final byte[] readBuffer, final ReadTarget readTarget) throws Exception {

    return client.read(
        path.toString(), position, readBuffer, 0, readBuffer.length,
        null, null, null, getTestTracingContext(fs, false), readTarget);
  }

  /**
   * Verifies a successful partial read.
   *
   * @param readOperation executed operation
   * @param expectedBytes expected bytes
   */
  private void assertSuccessfulRead(final AbfsRestOperation readOperation, final int expectedBytes) {
    assertThat(readOperation).describedAs("DFS read operation").isNotNull();
    assertThat(readOperation.getResult()).describedAs("DFS read result").isNotNull();
    assertThat(readOperation.getResult().getStatusCode())
        .describedAs("DFS read HTTP status")
        .isEqualTo(206);
    assertThat(readOperation.getResult().getBytesReceived())
        .describedAs("DFS read bytes received")
        .isEqualTo(expectedBytes);
  }

  /**
   * Creates deterministic test data of {@link #FILE_SIZE} bytes.
   *
   * @return test file data
   */
  private byte[] createTestData() {
    byte[] data = new byte[FILE_SIZE];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i % 256);
    }
    return data;
  }

  /**
   * Creates deterministic data of the given size.
   *
   * @param size data size in bytes
   * @return test data
   */
  private static byte[] createPatternData(final int size) {
    byte[] data = new byte[size];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i % 251);
    }
    return data;
  }

  /**
   * Writes the test file.
   *
   * @param fs filesystem
   * @param path path
   * @param data data
   * @throws Exception if writing fails
   */
  private void writeTestFile(final AzureBlobFileSystem fs, final Path path, final byte[] data)
      throws Exception {
    try (FSDataOutputStream out = fs.create(path, true)) {
      out.write(data);
    }
  }

  /**
   * Captures every ReadTarget passed to the target-aware client read.
   *
   * @param client spied client
   * @return targets in call order
   * @throws Exception if verification fails
   */
  private static List<ReadTarget> captureReadTargets(final AbfsClient client)
      throws Exception {
    ArgumentCaptor<ReadTarget> captor = ArgumentCaptor.forClass(ReadTarget.class);
    verify(client, atLeastOnce()).read(nullable(String.class), anyLong(),
        nullable(byte[].class), anyInt(), anyInt(), nullable(String.class),
        nullable(String.class), nullable(ContextEncryptionAdapter.class),
        nullable(TracingContext.class), captor.capture());
    return captor.getAllValues();
  }

  /**
   * Verifies the number of layout fetches made through the client.
   *
   * @param client spied client
   * @param expected expected number of getBlobLayout calls
   * @throws Exception if verification fails
   */
  private static void verifyLayoutFetches(final AbfsClient client,
      final int expected) throws Exception {
    verify(client, times(expected)).getBlobLayout(nullable(String.class),
        anyLong(), anyLong(), nullable(String.class), nullable(String.class),
        nullable(TracingContext.class));
  }
}
