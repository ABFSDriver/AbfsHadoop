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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutResponse;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.ReadTarget;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_PARQUET;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_READ_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_DIRECT_READ_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_DATA_LOCALITY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READ_AHEAD_QUEUE_DEPTH;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READ_POLICY;

import org.apache.hadoop.fs.azurebfs.constants.ReadType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.atLeastOnce;
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
   * Verifies that subsequent Direct Reads using a data handle are faster than
   * equivalent normal reads.
   *
   * <p>The test reads identical ranges from the same file using two filesystem
   * instances. Both instances have Data Locality enabled. Direct Read is
   * disabled for the normal filesystem and enabled for the Direct Read
   * filesystem.</p>
   *
   * <p>Every measured position is warmed before measurement so that layout
   * retrieval, data-handle acquisition, connection setup, and cold-path effects
   * are excluded. The test verifies that measured Direct Read requests use a
   * valid data handle and that their median paired latency is lower than the
   * equivalent normal-read latency.</p>
   *
   * @throws Exception if filesystem creation, file creation, reading, or
   *                   validation fails
   */
  @Test
  public void testDirectReadHasLowerMedianLatencyThanNormalRead()
      throws Exception {

    final int oneMb = 1024 * 1024;
    final int fileSize = 64 * oneMb;
    final int readSize = 4 * oneMb;
    final int warmupRounds = 2;
    final int measuredRounds = 5;

    final long[] readPositions = {
        0L,
        8L * oneMb,
        16L * oneMb,
        24L * oneMb,
        32L * oneMb,
        40L * oneMb,
        48L * oneMb,
        56L * oneMb
    };

    final int measuredIterations =
        measuredRounds * readPositions.length;

    Configuration normalConfiguration =
        new Configuration(getRawConfiguration());

    Configuration directReadConfiguration =
        new Configuration(getRawConfiguration());

    /*
     * Keep Data Locality enabled for both configurations so that the
     * comparison isolates Direct Read enablement.
     */
    normalConfiguration.setBoolean(
        FS_AZURE_ENABLE_DATA_LOCALITY,
        true);
    normalConfiguration.setBoolean(
        FS_AZURE_DIRECT_READ_ENABLED,
        false);

    directReadConfiguration.setBoolean(
        FS_AZURE_ENABLE_DATA_LOCALITY,
        true);
    directReadConfiguration.setBoolean(
        FS_AZURE_DIRECT_READ_ENABLED,
        true);

    /*
     * Disable background read-ahead so the measured latency represents only
     * the requested positioned read.
     */
    normalConfiguration.setInt(
        FS_AZURE_READ_AHEAD_QUEUE_DEPTH,
        0);

    directReadConfiguration.setInt(
        FS_AZURE_READ_AHEAD_QUEUE_DEPTH,
        0);

    /*
     * Use the same read-buffer size for both paths.
     */
    normalConfiguration.setInt(
        AZURE_READ_BUFFER_SIZE,
        readSize);

    directReadConfiguration.setInt(
        AZURE_READ_BUFFER_SIZE,
        readSize);

    /*
     * Use positioned random reads for both paths.
     */
    normalConfiguration.set(
        FS_AZURE_READ_POLICY,
        FS_OPTION_OPENFILE_READ_POLICY_PARQUET);

    directReadConfiguration.set(
        FS_AZURE_READ_POLICY,
        FS_OPTION_OPENFILE_READ_POLICY_PARQUET);

    try (AzureBlobFileSystem normalFileSystem =
             (AzureBlobFileSystem) FileSystem.newInstance(
                 getFileSystem().getUri(),
                 normalConfiguration);
         AzureBlobFileSystem directReadFileSystem =
             (AzureBlobFileSystem) FileSystem.newInstance(
                 getFileSystem().getUri(),
                 directReadConfiguration)) {

      assumeThat(
          normalFileSystem.getAbfsStore()
              .getAbfsConfiguration()
              .isDataLocalityEnabled())
          .as("Normal filesystem must have Data Locality enabled")
          .isTrue();

      assumeThat(
          directReadFileSystem.getAbfsStore()
              .getAbfsConfiguration()
              .isDataLocalityEnabled())
          .as("Direct Read filesystem must have Data Locality enabled")
          .isTrue();

      assumeThat(
          normalFileSystem.getAbfsStore()
              .getAbfsConfiguration()
              .isDirectReadEnabled())
          .as("Normal filesystem must have Direct Read disabled")
          .isFalse();

      assumeThat(
          directReadFileSystem.getAbfsStore()
              .getAbfsConfiguration()
              .isDirectReadEnabled())
          .as("Direct Read filesystem must have Direct Read enabled")
          .isTrue();

      Path testPath = new Path(
          "/direct-read-median-latency-"
              + UUID.randomUUID()
              + ".bin");

      AzureBlobFileSystemStore directReadStore =
          directReadFileSystem.getAbfsStore();

      /*
       * Spy on the actual Direct Read client so that the test can prove that
       * measured reads redeemed a data handle.
       */
      AbfsClient directReadClient =
          Mockito.spy(directReadStore.getClient());

      setAbfsClient(
          directReadStore,
          directReadClient);

      byte[] fileBlock =
          new byte[oneMb];

      for (int index = 0;
          index < fileBlock.length;
          index++) {
        fileBlock[index] =
            (byte) (index % 251);
      }

      try {
        /*
         * Create one real file. Both filesystem instances read exactly the
         * same object.
         */
        try (FSDataOutputStream outputStream =
                 normalFileSystem.create(testPath, true)) {

          for (int written = 0;
              written < fileSize;
              written += fileBlock.length) {
            outputStream.write(fileBlock);
          }
        }

        assertThat(
            normalFileSystem.getFileStatus(testPath).getLen())
            .as("Performance test file size")
            .isEqualTo(fileSize);

        /*
         * Keep both streams open across warm-up and measurement so stream-open
         * latency is excluded.
         */
        try (FSDataInputStream normalStream =
                 normalFileSystem.open(testPath);
             FSDataInputStream directReadStream =
                 directReadFileSystem.open(testPath)) {

          /*
           * Warm every measured position on both paths.
           *
           * For the Direct Read path, this fetches the layout and data handle
           * before any latency samples are recorded.
           */
          for (int round = 0;
              round < warmupRounds;
              round++) {

            for (long position : readPositions) {
              byte[] normalWarmupBuffer =
                  new byte[readSize];

              byte[] directWarmupBuffer =
                  new byte[readSize];

              int normalBytesRead =
                  normalStream.read(
                      position,
                      normalWarmupBuffer,
                      0,
                      normalWarmupBuffer.length);

              int directBytesRead =
                  directReadStream.read(
                      position,
                      directWarmupBuffer,
                      0,
                      directWarmupBuffer.length);

              assertThat(normalBytesRead)
                  .as(
                      "Normal warm-up read length at position %s",
                      position)
                  .isEqualTo(readSize);

              assertThat(directBytesRead)
                  .as(
                      "Direct Read warm-up length at position %s",
                      position)
                  .isEqualTo(readSize);

              assertThat(directWarmupBuffer)
                  .as(
                      "Warm-up data at position %s",
                      position)
                  .containsExactly(normalWarmupBuffer);
            }
          }

          /*
           * Exclude all warm-up interactions from the handle verification.
           */
          Mockito.clearInvocations(directReadClient);

          long[] normalLatencies =
              new long[measuredIterations];

          long[] directReadLatencies =
              new long[measuredIterations];

          long[] pairedLatencyDifferences =
              new long[measuredIterations];

          int sampleIndex = 0;

          for (int round = 0;
              round < measuredRounds;
              round++) {

            for (long position : readPositions) {
              byte[] normalBuffer =
                  new byte[readSize];

              byte[] directReadBuffer =
                  new byte[readSize];

              int normalBytesRead;
              int directBytesRead;

              /*
               * Alternate which path is measured first for every pair.
               */
              if ((sampleIndex & 1) == 0) {
                long normalStart =
                    System.nanoTime();

                normalBytesRead =
                    normalStream.read(
                        position,
                        normalBuffer,
                        0,
                        normalBuffer.length);

                normalLatencies[sampleIndex] =
                    System.nanoTime() - normalStart;

                long directReadStart =
                    System.nanoTime();

                directBytesRead =
                    directReadStream.read(
                        position,
                        directReadBuffer,
                        0,
                        directReadBuffer.length);

                directReadLatencies[sampleIndex] =
                    System.nanoTime() - directReadStart;
              } else {
                long directReadStart =
                    System.nanoTime();

                directBytesRead =
                    directReadStream.read(
                        position,
                        directReadBuffer,
                        0,
                        directReadBuffer.length);

                directReadLatencies[sampleIndex] =
                    System.nanoTime() - directReadStart;

                long normalStart =
                    System.nanoTime();

                normalBytesRead =
                    normalStream.read(
                        position,
                        normalBuffer,
                        0,
                        normalBuffer.length);

                normalLatencies[sampleIndex] =
                    System.nanoTime() - normalStart;
              }

              assertThat(normalBytesRead)
                  .as(
                      "Normal read length at position %s",
                      position)
                  .isEqualTo(readSize);

              assertThat(directBytesRead)
                  .as(
                      "Direct Read length at position %s",
                      position)
                  .isEqualTo(readSize);

              assertThat(directReadBuffer)
                  .as(
                      "Normal and Direct Read data at position %s",
                      position)
                  .containsExactly(normalBuffer);

              /*
               * A negative difference means Direct Read was faster for this
               * equivalent read pair.
               */
              pairedLatencyDifferences[sampleIndex] =
                  directReadLatencies[sampleIndex]
                      - normalLatencies[sampleIndex];

              sampleIndex++;
            }
          }

          assertThat(sampleIndex)
              .as("Number of collected latency samples")
              .isEqualTo(measuredIterations);

          /*
           * Capture only measured Direct Read requests.
           */
          ArgumentCaptor<ReadTarget> targetCaptor =
              ArgumentCaptor.forClass(ReadTarget.class);

          verify(directReadClient, atLeastOnce()).read(
              nullable(String.class),
              anyLong(),
              nullable(byte[].class),
              anyInt(),
              anyInt(),
              nullable(String.class),
              nullable(String.class),
              nullable(ContextEncryptionAdapter.class),
              nullable(TracingContext.class),
              targetCaptor.capture());

          List<ReadTarget> measuredTargets =
              targetCaptor.getAllValues();

          /*
           * Guard against accidentally comparing normal reads on both sides.
           */
          assertThat(measuredTargets)
              .as(
                  "Measured Direct Read requests should contain "
                      + "a valid data handle")
              .isNotEmpty()
              .allSatisfy(target -> {
                assertThat(target)
                    .as("Measured Direct Read target")
                    .isNotNull();

                assertThat(target.hasHandle())
                    .as(
                        "Every measured Direct Read target should contain "
                            + "a data handle")
                    .isTrue();

                assertThat(target.handle())
                    .as("Measured Direct Read data handle")
                    .isNotBlank();
              });

          /*
           * Sort the samples before calculating each median.
           */
          Arrays.sort(normalLatencies);
          Arrays.sort(directReadLatencies);
          Arrays.sort(pairedLatencyDifferences);

          long normalMedian =
              median(normalLatencies);

          long directReadMedian =
              median(directReadLatencies);

          long pairedDifferenceMedian =
              median(pairedLatencyDifferences);

          assertThat(normalMedian)
              .as("Normal read median latency")
              .isPositive();

          assertThat(directReadMedian)
              .as("Direct Read median latency")
              .isPositive();

          /*
           * The data handle was acquired during warm-up. For subsequent,
           * equivalent reads, the median paired Direct Read latency must be
           * lower than the normal-read latency.
           */
          assertThat(pairedDifferenceMedian)
              .as(
                  "Subsequent handle-backed Direct Reads should be faster "
                      + "than equivalent normal reads. "
                      + "Normal median: %s ns, "
                      + "Direct Read median: %s ns, "
                      + "median paired difference: %s ns",
                  normalMedian,
                  directReadMedian,
                  pairedDifferenceMedian)
              .isNegative();
        }
      } finally {
        normalFileSystem.delete(testPath, false);
      }
    }
  }

  /**
   * Returns the median from a sorted array of latency samples.
   *
   * @param sortedValues values sorted in ascending order
   * @return median value
   */
  private static long median(final long[] sortedValues) {
    assertThat(sortedValues)
        .as("Latency samples")
        .isNotEmpty();

    int middle = sortedValues.length / 2;

    if ((sortedValues.length & 1) == 1) {
      return sortedValues[middle];
    }

    /*
     * Calculate the midpoint without first adding both long values.
     */
    return sortedValues[middle - 1]
        + ((sortedValues[middle]
        - sortedValues[middle - 1]) / 2);
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
   * This test deliberately does not assume a fixed lifetime because the
   * service owns the expiry duration.
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
   * The test intentionally does not call getBlobLayout or manually construct
   * a ReadTarget. The stream must retrieve and use the layout automatically.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testDfsDirectReadThroughAbfsInputStream() throws Exception {
    Configuration configuration = new Configuration(getRawConfiguration());
    configuration.setBoolean(FS_AZURE_ENABLE_DATA_LOCALITY, true);
    configuration.setBoolean(FS_AZURE_DIRECT_READ_ENABLED, true);

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

    Configuration configuration = new Configuration(getRawConfiguration());
    configuration.setBoolean(FS_AZURE_ENABLE_DATA_LOCALITY, true);
    configuration.setBoolean(FS_AZURE_DIRECT_READ_ENABLED, true);
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

      AzureBlobFileSystemStore store = fs.getAbfsStore();
      AbfsClient client = Mockito.spy(store.getClient());
      setAbfsClient(store, client);

      byte[] fileBlock = new byte[oneMb];
      for (int index = 0; index < fileBlock.length; index++) {
        fileBlock[index] = (byte) (index % 251);
      }

      try {
        /*
         * Create one real file large enough for read-ahead to issue
         * additional reads.
         */
        try (FSDataOutputStream outputStream = fs.create(testPath, true)) {
          for (int written = 0; written < fileSize; written += fileBlock.length) {
            outputStream.write(fileBlock);
          }
        }

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

        /*
         * Verify the returned contents.
         */
        for (int offset = 0; offset < actual.length; offset++) {
          assertThat(actual[offset])
              .as("Data at offset %s", offset)
              .isEqualTo(fileBlock[offset % fileBlock.length]);
        }

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
              assertThat(target)
                  .as("Read-ahead Direct Read target")
                  .isNotNull();

              assertThat(target.hasHandle())
                  .as("Read-ahead request should contain a Direct Read data handle")
                  .isTrue();

              assertThat(target.handle())
                  .as("Read-ahead Direct Read data handle")
                  .isNotBlank();
            });

        /*
         * Prove that a prefetch request was actually issued.
         */
        assertThat(tracingContexts)
            .as("Read-ahead should issue a prefetch read")
            .anySatisfy(context ->
                assertThat(context.getReadType()).isEqualTo(ReadType.PREFETCH_READ));

        /*
         * Prove that a prefetch request itself used a data handle.
         */
        boolean handleBackedPrefetchFound = false;

        for (int index = 0; index < tracingContexts.size(); index++) {
          TracingContext context = tracingContexts.get(index);
          ReadTarget target = readTargets.get(index);

          if (context != null
              && context.getReadType() == ReadType.PREFETCH_READ
              && target != null
              && target.hasHandle()
              && target.handle() != null
              && !target.handle().isEmpty()) {
            handleBackedPrefetchFound = true;
            break;
          }
        }

        assertThat(handleBackedPrefetchFound)
            .as("At least one read-ahead prefetch request should use a Direct Read data handle")
            .isTrue();

      } finally {
        fs.delete(testPath, false);
      }
    }
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
   * Creates deterministic test data.
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
}