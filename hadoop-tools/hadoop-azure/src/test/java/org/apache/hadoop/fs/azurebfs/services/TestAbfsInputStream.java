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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbfsCountersImpl;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutResponse;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutXmlParser;
import org.apache.hadoop.fs.azurebfs.contracts.services.LayoutResponseParser;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadBufferStatus;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.constants.ReadType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TimeoutException;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.utils.TestCachedSASToken;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.impl.OpenFileParameters;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import static java.util.UUID.randomUUID;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_ADAPTIVE;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_AVRO;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_PARQUET;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COLON;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SPLIT_NO_LIMIT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_READ_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_DIRECT_READ_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_DATA_LOCALITY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_PREFETCH_REQUEST_PRIORITY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_READAHEAD;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_READAHEAD_V2;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READAHEAD_V2_CACHED_BUFFER_TTL_MILLIS;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READ_AHEAD_BLOCK_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READ_AHEAD_QUEUE_DEPTH;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_FS_AZURE_BLOB_LAYOUT_CACHE_MAX_COUNT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_REQUEST_PRIORITY;
import static org.apache.hadoop.fs.azurebfs.constants.ReadType.DIRECT_READ;
import static org.apache.hadoop.fs.azurebfs.constants.ReadType.FOOTER_READ;
import static org.apache.hadoop.fs.azurebfs.constants.ReadType.MISSEDCACHE_READ;
import static org.apache.hadoop.fs.azurebfs.constants.ReadType.NORMAL_READ;
import static org.apache.hadoop.fs.azurebfs.constants.ReadType.PREFETCH_READ;
import static org.apache.hadoop.fs.azurebfs.constants.ReadType.RANDOM_READ;
import static org.apache.hadoop.fs.azurebfs.constants.ReadType.SMALLFILE_READ;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FORWARD_SLASH;

/**
 * Unit test AbfsInputStream.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestAbfsInputStream extends AbstractAbfsIntegrationTest {

  private static final int ONE_KB = 1 * 1024;
  private static final int TWO_KB = 2 * 1024;
  private static final int THREE_KB = 3 * 1024;
  private static final int SIXTEEN_KB = 16 * ONE_KB;
  private static final int FORTY_EIGHT_KB = 48 * ONE_KB;
  private static final int ONE_MB = 1 * 1024 * 1024;
  private static final int FOUR_MB = 4 * ONE_MB;
  private static final int EIGHT_MB = 8 * ONE_MB;
  private static final int TEST_READAHEAD_DEPTH_2 = 2;
  private static final int TEST_READAHEAD_DEPTH_4 = 4;
  private static final int REDUCED_READ_BUFFER_AGE_THRESHOLD = 3000; // 3 sec
  private static final int INCREASED_READ_BUFFER_AGE_THRESHOLD =
      REDUCED_READ_BUFFER_AGE_THRESHOLD * 10; // 30 sec
  private static final int ALWAYS_READ_BUFFER_SIZE_TEST_FILE_SIZE = 16 * ONE_MB;
  private static final int POSITION_INDEX = 9;
  private static final int OPERATION_INDEX = 6;
  private static final int READTYPE_INDEX = 11;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestAbfsInputStream.class);


  @AfterEach
  @Override
  public void teardown() throws Exception {
    super.teardown();
    getBufferManager().testResetReadBufferManager();
  }

  AbfsRestOperation getMockRestOp() {
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    AbfsHttpOperation httpOp = mock(AbfsHttpOperation.class);
    when(httpOp.getBytesReceived()).thenReturn(1024L);
    when(op.getResult()).thenReturn(httpOp);
    when(op.getSasToken()).thenReturn(TestCachedSASToken.getTestCachedSASTokenInstance().get());
    return op;
  }

/**
   * Create a configured {@link AbfsInputStream} for layout-related tests.
   *
   * @param mockClient mock {@link AbfsClient} to back the stream
   * @param bufferSize read buffer size in bytes
   * @param fileSize file size in bytes to expose via the stream
   * @param enableV2 true to enable ReadAhead V2 behavior on the stream
   * @return a configured {@link AbfsInputStream} instance
   */
  AbfsInputStream getAbfsInputStreamForLayout(AbfsClient mockClient, int bufferSize, int fileSize, boolean enableV2) {

    // Create input stream context with required settings
    AbfsInputStreamContext inputStreamContext =
            new AbfsInputStreamContext(-1)
                    .withReadBufferSize(bufferSize)
                    .withReadAheadQueueDepth(2)
                    .withReadAheadBlockSize(bufferSize)
                    .isReadAheadV2Enabled(enableV2)
                    .withOptimizeFooterRead(true)
                    .withFooterReadBufferSize(512 * ONE_KB);

    // Create the input stream
    AbfsInputStream inputStream = new AbfsAdaptiveInputStream(
            mockClient,
            null,
            "/file",
            fileSize,
            inputStreamContext,
            "test-etag-" + randomUUID().toString().replace("-", "").substring(0, 4),
            new TracingContext(
                    "test-correlation-id",
                    "test-fs-id",
                    FSOperationType.READ,
                    true,
                    TracingHeaderFormat.ALL_ID_FORMAT,
                    null
            )
    );

    return inputStream;
  }

  /**
   * Creates a mocked ABFS client configured for Direct Read layout tests,
   * with read-ahead, buffer, and caching settings applied and default
   * stubbing for both the normal and layout-aware read overloads.
   *
   * @param bufferSize read-ahead block and buffer size to configure
   * @return mocked ABFS client ready for layout read testing
   * @throws Exception if client setup fails
   */
  AbfsClient getMockClientForLayoutRead(Integer bufferSize) throws Exception {
    Configuration conf = new Configuration();
    conf.set(FS_AZURE_READ_AHEAD_BLOCK_SIZE, String.valueOf(bufferSize));
    conf.set(AZURE_READ_BUFFER_SIZE, String.valueOf(bufferSize));
    conf.set(FS_AZURE_ENABLE_READAHEAD_V2, "true");
    conf.set(FS_AZURE_READAHEAD_V2_CACHED_BUFFER_TTL_MILLIS, "0");
    conf.setBoolean(FS_AZURE_DIRECT_READ_ENABLED, true);

    AbfsConfiguration abfsConfig = new AbfsConfiguration(conf, getAccountName());

    AbfsClient mockClient = mock(AbfsBlobClient.class);

    AbfsCounters abfsCounters = Mockito.spy(new AbfsCountersImpl(new URI("abcd")));
    Mockito.doReturn(abfsCounters).when(mockClient).getAbfsCounters();
    when(mockClient.getAbfsConfiguration()).thenReturn(abfsConfig);

    AbfsPerfTracker tracker =
        new AbfsPerfTracker("test", this.getAccountName(), this.getConfiguration());
    when(mockClient.getAbfsPerfTracker()).thenReturn(tracker);

    /*
     * Layout-related tests exercise both:
     *
     * client.read(..., tracingContext)
     *
     * and
     *
     * client.read(..., tracingContext, readTarget)
     *
     * Mockito returns null for an unstubbed method. readTask() accesses
     * op.getSasToken(), so provide a valid default operation for both
     * overloads.
     */
    AbfsRestOperation defaultReadOperation = mock(AbfsRestOperation.class);
    AbfsHttpOperation defaultHttpOperation = mock(AbfsHttpOperation.class);
    when(defaultReadOperation.getResult()).thenReturn(defaultHttpOperation);
    when(defaultHttpOperation.getBytesReceived()).thenReturn(0L);
    when(defaultReadOperation.getSasToken()).thenReturn(null);

    /*
     * Normal read path.
     */
    when(mockClient.read(
        nullable(String.class), anyLong(), nullable(byte[].class), anyInt(), anyInt(),
        nullable(String.class), nullable(String.class), nullable(ContextEncryptionAdapter.class),
        nullable(TracingContext.class)))
        .thenReturn(defaultReadOperation);

    /*
     * Layout / Direct Read path.
     */
    when(mockClient.read(
        nullable(String.class), anyLong(), nullable(byte[].class), anyInt(), anyInt(),
        nullable(String.class), nullable(String.class), nullable(ContextEncryptionAdapter.class),
        nullable(TracingContext.class), nullable(ReadTarget.class)))
        .thenReturn(defaultReadOperation);

    /*
     * AbfsInputStream checks this before using BlobLayoutCache.
     */
    when(mockClient.supportsLayout()).thenReturn(true);

    return mockClient;
  }

  /**
   * Generate a minimal BlobLayout XML describing contiguous ranges for a blob
   * and a set of endpoints.
   *
   * <p>The produced XML contains:
   * <ul>
   *   <li>{@code <Ranges>} - a sequence of {@code <Range>} entries covering the
   *       blob from 0 to {@code fileSize - 1} in steps of {@code rangeSize}.</li>
   *   <li>{@code <Endpoints>} - {@code endpointCount} endpoint entries with
   *       synthetic hostnames.</li>
   *   <li>{@code <NextMarker />} - an empty next-marker element.</li>
   * </ul>
   *
   * @param fileSize      total size of the file
   * @param rangeSize     size of each range in bytes
   * @param endpointCount number of endpoints to create
   * @return XML string representing the BlobLayout
   * @throws IllegalArgumentException if {@code rangeSize <= 0} or {@code endpointCount <= 0}
   */
  public static String generateBlobLayoutXml(long fileSize,
      long rangeSize,
      int endpointCount) {
    if (rangeSize <= 0) {
      throw new IllegalArgumentException("rangeSize must be > 0");
    }
    if (endpointCount <= 0) {
      throw new IllegalArgumentException("endpointCount must be > 0");
    }
    if (fileSize < 0) {
      fileSize = 0;
    }

    StringBuilder xml = new StringBuilder();

    xml.append("<BlobLayout>");
    xml.append("<Ranges>");

    long start = 0;
    int endpointIndex = 0;

    while (start < fileSize) {
      long end = Math.min(start + rangeSize - 1, fileSize - 1);

      xml.append("<Range Start=\"")
          .append(start)
          .append("\" End=\"")
          .append(end)
          .append("\" EndpointIndex=\"")
          .append(endpointIndex)
          .append("\" />");

      start += rangeSize;
      endpointIndex = (endpointIndex + 1) % endpointCount;
    }

    xml.append("</Ranges>");

    xml.append("<Endpoints>");
    for (int i = 0; i < endpointCount; i++) {
      xml.append("<Endpoint Index=\"")
          .append(i)
          .append("\" Value=\"blob.stamp")
          .append((char) ('A' + i))
          .append(".store.core.windows.net:443\" />");
    }
    xml.append("</Endpoints>");

    xml.append("<NextMarker />");
    xml.append("</BlobLayout>");

    return xml.toString();
  }

  /**
   * Helper to create an {@link AbfsInputStream}
   *
   * <p>This method:
   * <ol>
   *   <li>Creates a mocked {@link AbfsClient} suitable for layout-related reads.</li>
   *   <li>Creates an {@link AbfsInputStream} configured with the provided buffer and
   *       read-ahead settings.</li>
   *   <li>Generates a minimal BlobLayout XML covering the blob in ranges of
   *       {@code rangeSize} and with {@code endpointCount} endpoints, parses it and
   *       stores it into a {@link BlobLayoutCache} under a test etag.</li>
   *   <li>Attaches the populated cache to the created input stream and returns it.</li>
   * </ol>
   *
   * @param fileSize      total size of the file exposed by the stream in bytes
   * @param bufferSize    read buffer size in bytes
   * @param rangeSize     size of each layout range (chunk) in bytes; must be > 0
   * @param endpointCount number of endpoints to include in the generated layout; must be > 0
   * @param enableV2      enable ReadAhead V2 behavior on the created stream
   * @return a configured {@link AbfsInputStream} instance with its {@link BlobLayoutCache} set
   * @throws Exception on parser, IO, or client creation errors
   */
  private AbfsInputStream createInputStreamWithLayout(
          int fileSize,
          int bufferSize,
          long rangeSize,
          int endpointCount, boolean enableV2) throws Exception {

    // 1. Create mock client
    AbfsClient mockClient = getMockClientForLayoutRead(bufferSize);

    ReadBufferManager bufferManager = getBufferManagerForLayout(mockClient);

    // 2. Create input stream
    AbfsInputStream inputStream = getAbfsInputStreamForLayout(
            mockClient,
            bufferSize,
            fileSize, enableV2
    );

    // 3. Generate blob layout XML
    String layoutXml = generateBlobLayoutXml(fileSize, rangeSize, endpointCount);

    // 4. Parse the XML
    SAXParserFactory factory = SAXParserFactory.newInstance();
    SAXParser parser = factory.newSAXParser();
    BlobLayoutXmlParser handler = new BlobLayoutXmlParser();
    parser.parse(new ByteArrayInputStream(layoutXml.getBytes()), handler);
    BlobLayoutResponse layoutResponse = handler.getResponse();

    for (BlobLayoutResponse.Range range : layoutResponse.getRanges()) {
      LOG.debug("Layout range: {}-{}, endpoint: {}",
          range.start(), range.end(), range.endpointIndex());
    }

    BlobLayoutCache cache = BlobLayoutCache.getInstance(1,
        DEFAULT_FS_AZURE_BLOB_LAYOUT_CACHE_MAX_COUNT);
    cache.putBlobLayout(inputStream.getLayoutCacheKey(), layoutResponse, fileSize);
    bufferManager.testResetReadBufferManager(bufferSize, 0);

    return inputStream;
  }

  /**
   * Create an {@link AbfsInputStream} backed by the provided {@link AbfsClient}
   * and populate it with a generated {@link BlobLayoutResponse} stored in a
   * {@link BlobLayoutCache} under the test etag.
   *
   * <p>The method:
   * <ol>
   *   <li>Creates an {@link AbfsInputStream} configured for layout-related reads.</li>
   *   <li>Generates a minimal BlobLayout XML covering the blob in ranges of
   *       {@code rangeSize} and with {@code endpointCount} endpoints.</li>
   *   <li>Parses the XML into a {@link BlobLayoutResponse} and stores it in a
   *       {@link BlobLayoutCache} attached to the stream.</li>
   * </ol>
   *
   * @param fileSize      total size of the file exposed by the stream in bytes
   * @param bufferSize    read buffer size in bytes
   * @param rangeSize     size of each layout range
   * @param endpointCount number of endpoints to include in the generated layout
   * @param mockClient    mock {@link AbfsClient} used as the stream's client
   * @return a configured {@link AbfsInputStream} instance with its {@link BlobLayoutCache} set
   * @throws Exception on SAX parser, IO, or client creation errors
   */
  private AbfsInputStream createInputStreamWithLayout(
          int fileSize,
          int bufferSize,
          long rangeSize,
          int endpointCount, AbfsClient mockClient) throws Exception {

    // 1. Create input stream
    AbfsInputStream inputStream = getAbfsInputStreamForLayout(
            mockClient,
            bufferSize,
            fileSize, true
    );

    // 2. Generate blob layout XML
    String layoutXml = generateBlobLayoutXml(fileSize, rangeSize, endpointCount);

    // 3. Parse the XML
    SAXParserFactory factory = SAXParserFactory.newInstance();
    SAXParser parser = factory.newSAXParser();
    BlobLayoutXmlParser handler = new BlobLayoutXmlParser();
    parser.parse(new ByteArrayInputStream(layoutXml.getBytes()), handler);
    BlobLayoutResponse layoutResponse = handler.getResponse();

    for (BlobLayoutResponse.Range range : layoutResponse.getRanges()) {
      LOG.debug("Layout range: {}-{}, endpoint: {}",
          range.start(), range.end(), range.endpointIndex());
    }

    // 4. Set layout on stream
    BlobLayoutCache cache = BlobLayoutCache.getInstance(1,
        DEFAULT_FS_AZURE_BLOB_LAYOUT_CACHE_MAX_COUNT);
    cache.putBlobLayout(inputStream.getLayoutCacheKey(), layoutResponse, fileSize);
    return inputStream;
  }

  /**
   * Helper for layout-related tests.
   *
   * <p>This method:
   * <ol>
   *   <li>Creates an {@link AbfsInputStream} with a generated {@link BlobLayoutResponse}
   *       covering the file in ranges of {@code rangeSize} and attaches it to the stream.</li>
   *   <li>Reads the entire file into a buffer and verifies the total bytes read and that
   *       the read contents exactly match {@code testData}.</li>
   * </ol>
   *
   * @param fileSize  total size of the file
   * @param rangeSize size of each layout range
   * @param testData  byte array containing the expected file contents; its length must be at least {@code fileSize}
   * @throws Exception on errors creating the stream, parsing layout XML, or during mocking
   */
  private void testLayoutForDifferentRangesHelper(int fileSize,
                                                  int bufferSize, long rangeSize, byte[] testData) throws Exception {
    AbfsInputStream inputStreamWithLayout = createInputStreamWithLayout(
            fileSize,
            bufferSize,
            rangeSize,
            3, true
    );

    AbfsInputStream spyStream = Mockito.spy(inputStreamWithLayout);

    doAnswer(invocation -> {
      long position = invocation.getArgument(0);
      byte[] buffer = invocation.getArgument(1);
      int offset = invocation.getArgument(2);
      int length = invocation.getArgument(3);

      // Copy from testData into buffer
      int bytesToCopy = (int) Math.min(length, fileSize - position);
      System.arraycopy(testData, (int) position, buffer, offset, bytesToCopy);

      return bytesToCopy;
    }).when(spyStream).readRemote(
        anyLong(),
        any(byte[].class),
        anyInt(),
        anyInt(),
        any(TracingContext.class),
        any(ReadTarget.class)
    );


  byte[] readBuffer = new byte[fileSize];
    int totalBytesRead = spyStream.read(readBuffer, 0, fileSize); // Read entire file

    assertEquals(fileSize, totalBytesRead, "Should read entire file");
    assertThat(readBuffer).containsExactly(testData);
    inputStreamWithLayout.close();
    }

  /**
   * Verifies that footer read uses the correct endpoint determined by the blob layout cache. Asserts that:
   * <ul>
   *   <li>The number of bytes read equals 1024 and the content matches the expected footer bytes.</li>
   *   <li>The footer read invoked the endpoint corresponding to the final range (stampB) and
   *       did not invoke stampA.</li>
   * </ul>
   *
   * @throws Exception on any failure during mock setup, parsing, or I/O
   */
  @Test
  public void testFooterReadWithCorrectLayoutEndpoint() throws Exception {
    assumeThat(getFileSystem().getAbfsStore().getAbfsConfiguration()
            .isDataLocalityEnabled()).isTrue();

    int fileSize = FOUR_MB;
    int bufferSize = FOUR_MB;
    byte[] testData = generateTestData(fileSize);

    AbfsClient mockClient = getMockClientForLayoutRead(bufferSize);

    AtomicInteger stamp0Calls = new AtomicInteger(0);
    AtomicInteger stamp1Calls = new AtomicInteger(0);

    when(mockClient.read(
            nullable(String.class),
            anyLong(),
            nullable(byte[].class),
            anyInt(),
            anyInt(),
            nullable(String.class),
            nullable(String.class),
            nullable(ContextEncryptionAdapter.class),
            nullable(TracingContext.class),
            nullable(ReadTarget.class)
    )).thenAnswer(invocation -> {
      long position = invocation.getArgument(1);
      byte[] buffer = invocation.getArgument(2);
      int offset = invocation.getArgument(3);
      int length = invocation.getArgument(4);
      ReadTarget readTarget = invocation.getArgument(9);
      String endpoint = readTarget == null ? null : readTarget.endpoint();

      if (endpoint != null) {
        if (endpoint.contains("stampA")) {
          stamp0Calls.incrementAndGet();
        } else if (endpoint.contains("stampB")) {
          stamp1Calls.incrementAndGet();
        }
      }

      int bytesToCopy = (int) Math.min(length, fileSize - position);
      System.arraycopy(testData, (int) position, buffer, offset, bytesToCopy);

      AbfsRestOperation mockOp = mock(AbfsRestOperation.class);
      AbfsHttpOperation mockHttpOp = mock(AbfsHttpOperation.class);

      when(mockOp.getResult()).thenReturn(mockHttpOp);
      when(mockHttpOp.getBytesReceived()).thenReturn((long) bytesToCopy);
      when(mockOp.getSasToken()).thenReturn(null);

      return mockOp;
    });

    AbfsInputStream inputStream = createInputStreamWithLayout(
            fileSize,
            FOUR_MB,
            1L * ONE_MB,
            2,
            mockClient
    );

    AbfsInputStream spyStream = Mockito.spy(inputStream);
    FSDataInputStream fsStream = new FSDataInputStream(spyStream);

    // Footer starts at: fileSize - FOOTER_SIZE
    long footerStart = fileSize - AbfsInputStream.FOOTER_SIZE;

    // Read from footer region
    fsStream.seek(footerStart);
    byte[] footerBuffer = new byte[ONE_KB];
    int bytesRead = fsStream.read(footerBuffer, 0, ONE_KB);

    assertEquals(ONE_KB, bytesRead, "Should read 1KB from footer");

    byte[] expected = new byte[ONE_KB];
    System.arraycopy(testData, (int) footerStart, expected, 0, ONE_KB);
    assertArrayEquals(expected, footerBuffer, "Footer data should match");

    // Footer at position ~4MB-16KB = 4177920 falls in range 3-4MB (stampB)
    assertEquals(1, stamp1Calls.get(), "Footer read should use stampB (endpoint 1)");
    assertEquals(0, stamp0Calls.get(), "Footer read should not use stampA (endpoint 0)");

    fsStream.close();
  }

  /**
   * Test reading with blob layout for multiple file and range sizes.
   * <p>
   * For each file size in {4MB, 5MB} and for chunk sizes from 1MB to 5MB,
   * generate test data and verify that the input stream reads the entire
   * file correctly.
   *
   * @throws Exception on failure during setup, parsing, or IO operations.
   */
  @Test
  public void testLayoutReadForDifferentRanges() throws Exception {
    assumeThat(getFileSystem().getAbfsStore().getAbfsConfiguration()
        .isDataLocalityEnabled()).isTrue();
    int[] fileSizes = {FOUR_MB, 5 * ONE_MB};
    int bufferSize = 8 * ONE_MB;
    for (int fileSize : fileSizes) {
      byte[] testData = generateTestData(fileSize);
      for (int multiplier = 1; multiplier <= 5; multiplier++) {
        long rangeSize = (long) multiplier * ONE_MB;
        testLayoutForDifferentRangesHelper(fileSize, bufferSize, rangeSize, testData);
      }
    }
  }

  /**
   * Test that verifies blob layout is honored for prefetch reads.
   *
   * <ol>
   *   <li>Asserts the total bytes read and that data matches the source.</li>
   *   <li>Captures the {@code readRemote} calls and asserts:
   *     <ul>
   *       <li>The expected read positions and lengths were issued.</li>
   *       <li>Each captured {@link TracingContext} has read type {@code PREFETCH_READ}.</li>
   *       <li>The endpoint used for each read corresponds to the blob layout:
   *           stampA = 1 call, stampB = 2 calls, stampC = 2 calls.</li>
   *     </ul>
   *   </li>
   * </ol>
   *
   * @throws Exception on any failure during setup, parsing, or I/O
   */
  @Test
  public void testBlobLayoutForPrefetchReads() throws Exception {
    assumeThat(getFileSystem().getAbfsStore().getAbfsConfiguration()
            .isDataLocalityEnabled()).isTrue();

    int fileSize = 9 * ONE_MB;
    byte[] testData = generateTestData(fileSize);

    AbfsClient mockClient = getMockClientForLayoutRead(FOUR_MB);
    ReadBufferManager bufferManager = getBufferManagerForLayout(mockClient); // required to set the buffer manager configs

    AtomicInteger callCount = new AtomicInteger(0);
    CountDownLatch callsCompleted = new CountDownLatch(5);

    when(mockClient.read(
            nullable(String.class),
            anyLong(),
            nullable(byte[].class),
            anyInt(),
            anyInt(),
            nullable(String.class),
            nullable(String.class),
            nullable(ContextEncryptionAdapter.class),
            nullable(TracingContext.class),
            nullable(ReadTarget.class)
    )).thenAnswer(invocation -> {
      callCount.incrementAndGet();
      long position = invocation.getArgument(1);
      byte[] buffer = invocation.getArgument(2);
      int offset = invocation.getArgument(3);
      int length = invocation.getArgument(4);
      TracingContext tc = invocation.getArgument(8);

      int bytesToCopy = (int) Math.min(length, fileSize - position);
      System.arraycopy(testData, (int) position, buffer, offset, bytesToCopy);

      AbfsRestOperation mockOp = mock(AbfsRestOperation.class);
      AbfsHttpOperation mockHttpOp = mock(AbfsHttpOperation.class);
      when(mockOp.getResult()).thenReturn(mockHttpOp);
      when(mockHttpOp.getBytesReceived()).thenReturn((long) bytesToCopy);
      when(mockOp.getSasToken()).thenReturn(null);

      LOG.debug("read position={} read={} readType={}",
          position, bytesToCopy, tc == null ? "null" : tc.getReadType());
      callsCompleted.countDown();
      return mockOp;
    });

    AbfsInputStream inputStream = createInputStreamWithLayout(
            fileSize, FOUR_MB, 3L * ONE_MB, 3, mockClient);

    byte[] readBuffer = new byte[fileSize];
    int totalBytesRead = inputStream.read(readBuffer, 0, fileSize);

    LOG.debug("Total bytes read: {}", totalBytesRead);
    boolean completed = callsCompleted.await(15, TimeUnit.SECONDS);

    if (!completed) {
      throw new AssertionError(String.format("Only %d/5 calls completed", callCount.get()));
    }

    assertEquals(fileSize, totalBytesRead, "Should read entire file");
    assertArrayEquals(testData, readBuffer);

    ArgumentCaptor<Long> positionCaptor = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Integer> lengthCaptor = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<TracingContext> tcCaptor = ArgumentCaptor.forClass(TracingContext.class);
    ArgumentCaptor<ReadTarget> targetCaptor = ArgumentCaptor.forClass(ReadTarget.class);

    verify(mockClient, times(5)).read(
            nullable(String.class),
            positionCaptor.capture(),
            nullable(byte[].class),
            nullable(Integer.class),
            lengthCaptor.capture(),
            nullable(String.class),
            nullable(String.class),
            nullable(ContextEncryptionAdapter.class),
            tcCaptor.capture(),
            targetCaptor.capture()
    );

    List<Long> positions = positionCaptor.getAllValues();
    List<Integer> lengths = lengthCaptor.getAllValues();
    List<TracingContext> contexts = tcCaptor.getAllValues();
    List<ReadTarget> targets = targetCaptor.getAllValues();

    int countStampA = 0;
    int countStampB = 0;
    int countStampC = 0;

    for (int i = 0; i < positions.size(); i++) {
      long pos = positions.get(i);

      int expectedLen;
      if (pos == 0L) {
        expectedLen = 3 * ONE_MB;
      } else if (pos == 3 * ONE_MB || pos == 8 * ONE_MB) {
        expectedLen = ONE_MB;
      } else if (pos == FOUR_MB || pos == 6 * ONE_MB) {
        expectedLen = 2 * ONE_MB;
      } else {
        throw new IllegalStateException("Unexpected position: " + pos);
      }

      assertEquals(expectedLen, lengths.get(i).intValue(),
              "Read length mismatch at position " + pos);
      assertEquals(PREFETCH_READ, contexts.get(i).getReadType(),
              "ReadType should be PREFETCH_READ at position " + pos);

      String ep = targets.get(i).endpoint();
      if (ep.contains("stampA")) {
        countStampA++;
      } else if (ep.contains("stampB")) {
        countStampB++;
      } else if (ep.contains("stampC")) {
        countStampC++;
      } else {
        fail("Unexpected endpoint: " + ep);
      }
    }

    assertEquals(1, countStampA, "stampA should have 1 read");
    assertEquals(2, countStampB, "stampB should have 2 reads");
    assertEquals(2, countStampC, "stampC should have 2 reads");

    inputStream.close();
    bufferManager.resetBufferManager();
  }

  /**
   * Generate test data with a known pattern for verification
   */
  private byte[] generateTestData(int size) {
    byte[] data = new byte[size];
    for (int i = 0; i < size; i++) {
      data[i] = (byte) (i % 256);
    }

    return data;
  }

  AbfsClient getMockAbfsClient() throws URISyntaxException {
    // Mock failure for client.read()
    AbfsClient client = mock(AbfsClient.class);
    AbfsCounters abfsCounters = Mockito.spy(new AbfsCountersImpl(new URI("abcd")));
    Mockito.doReturn(abfsCounters).when(client).getAbfsCounters();
    AbfsPerfTracker tracker = new AbfsPerfTracker(
        "test",
        this.getAccountName(),
        this.getConfiguration());
    when(client.getAbfsPerfTracker()).thenReturn(tracker);

    return client;
  }

  AbfsInputStream getAbfsInputStream(AbfsClient mockAbfsClient,
      String fileName) throws IOException {
    AbfsInputStreamContext inputStreamContext = new AbfsInputStreamContext(-1);
    // Create AbfsInputStream with the client instance
    AbfsInputStream inputStream = new AbfsAdaptiveInputStream(
        mockAbfsClient,
        null,
        FORWARD_SLASH + fileName,
        THREE_KB,
        inputStreamContext.withReadBufferSize(ONE_KB)
            .withReadAheadQueueDepth(10)
            .withReadAheadBlockSize(ONE_KB)
            .isReadAheadV2Enabled(getConfiguration().isReadAheadV2Enabled()),
        "eTag",
        getTestTracingContext(null, false));

    inputStream.setCachedSasToken(
        TestCachedSASToken.getTestCachedSASTokenInstance());

    return inputStream;
  }

  public AbfsInputStream getAbfsInputStream(AbfsClient abfsClient,
      String fileName,
      int fileSize,
      String eTag,
      int readAheadQueueDepth,
      int readBufferSize,
      boolean alwaysReadBufferSize,
      int readAheadBlockSize) throws IOException {
    AbfsInputStreamContext inputStreamContext = new AbfsInputStreamContext(-1);
    // Create AbfsInputStream with the client instance
    AbfsInputStream inputStream = new AbfsAdaptiveInputStream(
        abfsClient,
        null,
        FORWARD_SLASH + fileName,
        fileSize,
        inputStreamContext.withReadBufferSize(readBufferSize)
            .withReadAheadQueueDepth(readAheadQueueDepth)
            .withShouldReadBufferSizeAlways(alwaysReadBufferSize)
            .withReadAheadBlockSize(readAheadBlockSize),
        eTag,
        getTestTracingContext(getFileSystem(), false));

    inputStream.setCachedSasToken(
        TestCachedSASToken.getTestCachedSASTokenInstance());

    return inputStream;
  }

  void queueReadAheads(AbfsInputStream inputStream) throws IOException {
    // Mimic AbfsInputStream readAhead queue requests
    getBufferManager()
        .queueReadAhead(inputStream, 0, ONE_KB, inputStream.getTracingContext(), null);
    getBufferManager()
        .queueReadAhead(inputStream, ONE_KB, ONE_KB,
            inputStream.getTracingContext(), null);
    getBufferManager()
        .queueReadAhead(inputStream, TWO_KB, TWO_KB,
            inputStream.getTracingContext(), null);
  }

  private void verifyReadCallCount(AbfsClient client, int count)
      throws IOException, InterruptedException {
    // ReadAhead threads are triggered asynchronously.
    // Wait a second before verifying the number of total calls.
    Thread.sleep(1000);
    verify(client, times(count)).read(any(String.class), any(Long.class),
        any(byte[].class), any(Integer.class), any(Integer.class),
        any(String.class), any(String.class), any(), any(TracingContext.class));
  }

  private void checkEvictedStatus(AbfsInputStream inputStream, int position, boolean expectedToThrowException)
      throws Exception {
    // Sleep for the eviction threshold time
    Thread.sleep(getBufferManager().getThresholdAgeMilliseconds() + 1000);

    // Eviction is done only when AbfsInputStream tries to queue new items.
    // 1 tryEvict will remove 1 eligible item. To ensure that the current test buffer
    // will get evicted (considering there could be other tests running in parallel),
    // call tryEvict for the number of items that are there in completedReadList.
    int numOfCompletedReadListItems = getBufferManager().getCompletedReadListSize();
    while (numOfCompletedReadListItems > 0) {
      getBufferManager().callTryEvict();
      numOfCompletedReadListItems--;
    }

    if (expectedToThrowException) {
      intercept(IOException.class,
          () -> inputStream.read(position, new byte[ONE_KB], 0, ONE_KB));
    } else {
      inputStream.read(position, new byte[ONE_KB], 0, ONE_KB);
    }
  }

  public TestAbfsInputStream() throws Exception {
    super();
    // Reduce thresholdAgeMilliseconds to 3 sec for the tests
    getBufferManager().setThresholdAgeMilliseconds(REDUCED_READ_BUFFER_AGE_THRESHOLD);
  }

  private void writeBufferToNewFile(Path testFile, byte[] buffer) throws IOException {
    AzureBlobFileSystem fs = getFileSystem();
    fs.create(testFile);
    FSDataOutputStream out = fs.append(testFile);
    out.write(buffer);
    out.close();
  }

  private void verifyOpenWithProvidedStatus(Path path, FileStatus fileStatus,
      byte[] buf, AbfsRestOperationType source)
      throws IOException, ExecutionException, InterruptedException {
    byte[] readBuf = new byte[buf.length];
    AzureBlobFileSystem fs = getFileSystem();
    FutureDataInputStreamBuilder builder = fs.openFile(path);
    builder.withFileStatus(fileStatus);
    FSDataInputStream in = builder.build().get();
    assertEquals(buf.length, in.read(readBuf),
        String.format("Open with fileStatus [from %s result]: Incorrect number of bytes read", source));
    assertArrayEquals(readBuf, buf,
        String.format("Open with fileStatus [from %s result]: Incorrect read data", source));
  }

  private void checkGetPathStatusCalls(Path testFile, FileStatus fileStatus,
      AzureBlobFileSystemStore abfsStore, AbfsClient mockClient,
      AbfsRestOperationType source, TracingContext tracingContext)
      throws IOException {

    // verify GetPathStatus not invoked when FileStatus is provided
    abfsStore.openFileForRead(testFile, Optional
        .ofNullable(new OpenFileParameters().withStatus(fileStatus)), null, tracingContext);
    verify(mockClient, times(0).description((String.format(
        "FileStatus [from %s result] provided, GetFileStatus should not be invoked",
        source)))).getPathStatus(anyString(), anyBoolean(), any(TracingContext.class), any(
        ContextEncryptionAdapter.class));

    // verify GetPathStatus invoked when FileStatus not provided
    abfsStore.openFileForRead(testFile,
        Optional.empty(), null,
        tracingContext);
    verify(mockClient, times(1).description(
        "GetPathStatus should be invoked when FileStatus not provided"))
        .getPathStatus(anyString(), anyBoolean(), any(TracingContext.class), nullable(
            ContextEncryptionAdapter.class));

    Mockito.reset(mockClient); //clears invocation count for next test case
  }

  @Test
  public void testOpenFileWithOptions() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    String testFolder = "/testFolder";
    Path smallTestFile = new Path(testFolder + "/testFile0");
    Path largeTestFile = new Path(testFolder + "/testFile1");
    fs.mkdirs(new Path(testFolder));
    int readBufferSize = getConfiguration().getReadBufferSize();
    byte[] smallBuffer = new byte[5];
    byte[] largeBuffer = new byte[readBufferSize + 5];
    new Random().nextBytes(smallBuffer);
    new Random().nextBytes(largeBuffer);
    writeBufferToNewFile(smallTestFile, smallBuffer);
    writeBufferToNewFile(largeTestFile, largeBuffer);

    FileStatus[] getFileStatusResults = {fs.getFileStatus(smallTestFile),
        fs.getFileStatus(largeTestFile)};
    FileStatus[] listStatusResults = fs.listStatus(new Path(testFolder));

    // open with fileStatus from GetPathStatus
    verifyOpenWithProvidedStatus(smallTestFile, getFileStatusResults[0],
        smallBuffer, AbfsRestOperationType.GetPathStatus);
    verifyOpenWithProvidedStatus(largeTestFile, getFileStatusResults[1],
        largeBuffer, AbfsRestOperationType.GetPathStatus);

    // open with fileStatus from ListStatus
    verifyOpenWithProvidedStatus(smallTestFile, listStatusResults[0], smallBuffer,
        AbfsRestOperationType.ListPaths);
    verifyOpenWithProvidedStatus(largeTestFile, listStatusResults[1], largeBuffer,
        AbfsRestOperationType.ListPaths);

    // verify number of GetPathStatus invocations
    AzureBlobFileSystemStore abfsStore = getAbfsStore(fs);
    AbfsClient mockClient = spy(getAbfsClient(abfsStore));
    setAbfsClient(abfsStore, mockClient);
    TracingContext tracingContext = getTestTracingContext(fs, false);
    checkGetPathStatusCalls(smallTestFile, getFileStatusResults[0],
        abfsStore, mockClient, AbfsRestOperationType.GetPathStatus, tracingContext);
    checkGetPathStatusCalls(largeTestFile, getFileStatusResults[1],
        abfsStore, mockClient, AbfsRestOperationType.GetPathStatus, tracingContext);
    checkGetPathStatusCalls(smallTestFile, listStatusResults[0],
        abfsStore, mockClient, AbfsRestOperationType.ListPaths, tracingContext);
    checkGetPathStatusCalls(largeTestFile, listStatusResults[1],
        abfsStore, mockClient, AbfsRestOperationType.ListPaths, tracingContext);

    // Verify with incorrect filestatus
    getFileStatusResults[0].setPath(new Path("wrongPath"));
    intercept(ExecutionException.class,
        () -> verifyOpenWithProvidedStatus(smallTestFile,
            getFileStatusResults[0], smallBuffer,
            AbfsRestOperationType.GetPathStatus));
  }

  /**
   * This test expects AbfsInputStream to throw the exception that readAhead
   * thread received on read. The readAhead thread must be initiated from the
   * active read request itself.
   * Also checks that the ReadBuffers are evicted as per the ReadBufferManager
   * threshold criteria.
   * @throws Exception
   */
  @Test
  public void testFailedReadAhead() throws Exception {
    AbfsClient client = getMockAbfsClient();
    AbfsRestOperation successOp = getMockRestOp();

    // Stub :
    // Read request leads to 3 readahead calls: Fail all 3 readahead-client.read()
    // Actual read request fails with the failure in readahead thread
    doThrow(new TimeoutException("Internal Server error for RAH-Thread-X"))
        .doThrow(new TimeoutException("Internal Server error for RAH-Thread-Y"))
        .doThrow(new TimeoutException("Internal Server error RAH-Thread-Z"))
        .doReturn(successOp) // Any extra calls to read, pass it.
        .when(client)
        .read(any(String.class), any(Long.class), any(byte[].class),
            any(Integer.class), any(Integer.class), any(String.class),
            any(String.class), any(), any(TracingContext.class));

    AbfsInputStream inputStream = getAbfsInputStream(client, "testFailedReadAhead.txt");

    // Scenario: ReadAhead triggered from current active read call failed
    // Before the change to return exception from readahead buffer,
    // AbfsInputStream would have triggered an extra readremote on noticing
    // data absent in readahead buffers
    // In this test, a read should trigger 3 client.read() calls as file is 3 KB
    // and readahead buffer size set in AbfsInputStream is 1 KB
    // There should only be a total of 3 client.read() in this test.
    intercept(IOException.class,
        () -> inputStream.read(new byte[ONE_KB]));

    // Only the 3 readAhead threads should have triggered client.read
    verifyReadCallCount(client, 3);

    // Stub returns success for the 4th read request, if ReadBuffers still
    // persisted, ReadAheadManager getBlock would have returned exception.
    checkEvictedStatus(inputStream, 0, false);
  }

  /**
   * Perform read verification using the provided {@link Configuration}.
   *
   * <p>This helper does the following:
   * <ol>
   *   <li>Create an {@link AzureBlobFileSystem} from {@code conf} and a test file of 8MB at `/txtfile.txt`.</li>
   *   <li>Assert that the total bytes read equals the file size, the read data matches
   *       the written data, exactly two remote reads occurred and each captured
   *       {@link TracingContext} reports {@code expectedReadType}.</li>
   * </ol>
   *
   * @param fs the {@link AzureBlobFileSystem} instance to use for reading
   * @param expectedReadType expected {@link ReadType} in the captured {@link TracingContext}
   * @throws Exception on IO, interruption, parser or assertion failures
   */
  private void readAndVerify(
          AzureBlobFileSystem fs,
          ReadType expectedReadType) throws Exception {

    Path testFile = new Path("/txtfile.txt");
    fs.create(testFile).close();

    int fileSize = 8 * ONE_MB;
    byte[] writeData = generateTestData(fileSize);

    try (FSDataOutputStream out = fs.append(testFile)) {
      out.write(writeData);
    }

    try (FSDataInputStream iStream = fs.open(testFile)) {
      AbfsInputStream realStream =
              (AbfsInputStream) iStream.getWrappedStream();

      AbfsInputStream spyStream = Mockito.spy(realStream);

      AtomicInteger readCount = new AtomicInteger(0);
      CountDownLatch latch = new CountDownLatch(2);

      doAnswer(invocation -> {
        int call = readCount.incrementAndGet();
        long position = invocation.getArgument(0);
        int length = invocation.getArgument(3);
        TracingContext tc = invocation.getArgument(4);
        ReadTarget readTarget = invocation.getArgument(5);
        String endpoint = readTarget == null ? null : readTarget.endpoint();

        LOG.debug("Read call {}: position={}, length={}, endpoint={}, readType={}",
            call, position, length, endpoint, tc != null ? tc.getReadType() : "null");

        assertNotNull(readTarget);
        assertNotNull(endpoint);
        assertFalse(endpoint.isEmpty());
        assertNotNull(tc);
        if (expectedReadType == PREFETCH_READ) {
          assertTrue(tc.getReadType() == PREFETCH_READ || tc.getReadType() == MISSEDCACHE_READ,
              "ReadType in TracingContext should be PREFETCH_READ or MISSEDCACHE_READ for this test");
        } else {
          assertEquals(expectedReadType, tc.getReadType(),
              "ReadType in TracingContext should match expected for this test");
        }

        Object result = invocation.callRealMethod();

        latch.countDown();
        return result;

      }).when(spyStream).readRemote(
              anyLong(),
              any(byte[].class),
              anyInt(),
              anyInt(),
              nullable(TracingContext.class),
              nullable(ReadTarget.class)
      );

      byte[] readData = new byte[fileSize];

      int bytes = spyStream.read(readData);

      boolean completed = latch.await(10, TimeUnit.SECONDS);
      if (!completed) {
        throw new AssertionError(String.format("Only %d/2 calls completed", readCount.get()));
      }

      assertEquals(fileSize, bytes);
      assertThat(readData).containsExactly(writeData);
      assertEquals(2, readCount.get());
    }
  }

  /**
   * Verifies layout read behavior when ReadAhead V2 is disabled.
   *
   * <p>This test covers two scenarios:
   * <ol>
   *   <li>With ReadAhead V2 disabled (but read-ahead enabled),
   *       layout prefetch reads should be issued with read type {@link ReadType#PREFETCH_READ} with appropriate endpoint.</li>
   *   <li>When both ReadAhead V2 and read-ahead are disabled, layout reads should have read type {@link ReadType#NORMAL_READ}
   *       with appropriate endpoint.</li>
   * </ol>
   *
   * @throws Exception on failure during setup, parsing, or I/O operations
   */
  @Test
  public void testLayoutReadsWithV2Disabled() throws Exception {
    Configuration conf = getRawConfiguration();
    AzureBlobFileSystem fs =
            (AzureBlobFileSystem) FileSystem.newInstance(conf);
    assumeThat(getConfiguration(fs).isDataLocalityEnabled()).isTrue();
    // When ReadAheadV2 is disabled, layout prefetch reads (PR) would happen with first serving endpoint
    readAndVerify(fs, ReadType.PREFETCH_READ);

    // When ReadAheadV2 and readAhead are both disabled, layout reads (NR) would happen with first serving endpoint
    conf.set(FS_AZURE_ENABLE_READAHEAD, "false");
    fs = (AzureBlobFileSystem) FileSystem.newInstance(conf);
    readAndVerify(fs, NORMAL_READ);
  }

  /**
   * Tests missed-cache read for a failed prefetch for one segment when using
   * blob layout.
   *
   * <p>Assert:
   * <ul>
   *   <li>The read completes and returns the full file (data matches
   *   {@code testData}).</li>
   *   <li>Prefetch attempts occurred for all ranges, but even if a single read
   *   range fails, we attempt a MR Read.</li>
   *   <li>Prefetch ranges are 1MB each; the missed-cache read is 4MB and
   *   targets the expected endpoint (stampA).</li>
   *   <li>Endpoint distribution for prefetch attempts matches
   *   expectations.</li>
   * </ul>
   *
   * @throws Exception on any failure during setup, parsing, or I/O
   */
  @Test
  public void testBlobLayoutWithFailedPrefetch() throws Exception {
    assumeThat(getFileSystem().getAbfsStore().getAbfsConfiguration()
        .isDataLocalityEnabled()).isTrue();

    int fileSize = FOUR_MB;
    byte[] testData = generateTestData(fileSize);

    AbfsClient mockClient = getMockClientForLayoutRead(FOUR_MB);
    ReadBufferManager bufferManager = getBufferManagerForLayout(mockClient);

    /*
     * Keep threshold at 0 so failed/stale prefetch buffers can immediately
     * transition to the missed-cache recovery path.
     */
    bufferManager.setThresholdAgeMilliseconds(0);

    long failedSegmentOffset = 2L * ONE_MB;

    /*
     * Wait only for the four prefetch attempts.
     *
     * Do not wait for an assumed total number of client reads because a
     * threshold of 0 can also cause successfully prefetched buffers to become
     * unavailable before they are consumed.
     */
    CountDownLatch prefetchCallsCompleted = new CountDownLatch(4);

    when(mockClient.read(
        nullable(String.class),
        anyLong(),
        nullable(byte[].class),
        anyInt(),
        anyInt(),
        nullable(String.class),
        nullable(String.class),
        nullable(ContextEncryptionAdapter.class),
        nullable(TracingContext.class),
        nullable(ReadTarget.class)))
        .thenAnswer(invocation -> {
          long position = invocation.getArgument(1);
          byte[] buffer = invocation.getArgument(2);
          int offset = invocation.getArgument(3);
          int length = invocation.getArgument(4);
          TracingContext tracingContext = invocation.getArgument(8);

          ReadType readType = tracingContext == null
              ? null
              : tracingContext.getReadType();

          try {
            /*
             * Intentionally fail only the prefetch for the third 1 MB
             * segment.
             */
            if (position == failedSegmentOffset
                && readType == ReadType.PREFETCH_READ) {
              throw new IOException(
                  "Simulated prefetch failure for segment at " + position);
            }
            int bytesToCopy = (int) Math.min(length, fileSize - position);
            System.arraycopy(testData, (int) position, buffer, offset,
                bytesToCopy);
            AbfsRestOperation mockOp = mock(AbfsRestOperation.class);
            AbfsHttpOperation mockHttpOp = mock(AbfsHttpOperation.class);
            when(mockOp.getResult()).thenReturn(mockHttpOp);
            when(mockHttpOp.getBytesReceived())
                .thenReturn((long) bytesToCopy);
            when(mockOp.getSasToken()).thenReturn(null);
            return mockOp;
          } finally {
            /*
             * Count both successful and failed prefetch attempts.
             */
            if (readType == ReadType.PREFETCH_READ) {
              prefetchCallsCompleted.countDown();
            }
          }
        });

    try (AbfsInputStream inputStream = createInputStreamWithLayout(
        fileSize, FOUR_MB, 1L * ONE_MB, 2, mockClient)) {
      byte[] readBuffer = new byte[fileSize];
      int totalBytesRead = inputStream.read(readBuffer, 0, fileSize);
      boolean completed = prefetchCallsCompleted.await(10, TimeUnit.SECONDS);
      assertTrue(completed, "All four prefetch attempts should complete");

      /*
       * The failed prefetch must be transparent to the caller.
       */
      assertEquals(fileSize, totalBytesRead,
          "Should read entire 4 MB file despite prefetch failure");
      assertArrayEquals(testData, readBuffer,
          "Data should match exactly despite failed prefetch");

      ArgumentCaptor<Long> positionCaptor =
          ArgumentCaptor.forClass(Long.class);
      ArgumentCaptor<Integer> lengthCaptor =
          ArgumentCaptor.forClass(Integer.class);
      ArgumentCaptor<TracingContext> tracingContextCaptor =
          ArgumentCaptor.forClass(TracingContext.class);
      ArgumentCaptor<ReadTarget> readTargetCaptor =
          ArgumentCaptor.forClass(ReadTarget.class);

      /*
       * Capture every client read.
       *
       * Do not use times(5) here. With thresholdAgeMilliseconds set to 0,
       * additional missed-cache reads are valid.
       */
      verify(mockClient, atLeastOnce()).read(
          nullable(String.class),
          positionCaptor.capture(),
          nullable(byte[].class),
          anyInt(),
          lengthCaptor.capture(),
          nullable(String.class),
          nullable(String.class),
          nullable(ContextEncryptionAdapter.class),
          tracingContextCaptor.capture(),
          readTargetCaptor.capture());

      List<Long> positions = positionCaptor.getAllValues();
      List<Integer> lengths = lengthCaptor.getAllValues();
      List<TracingContext> contexts = tracingContextCaptor.getAllValues();
      List<ReadTarget> readTargets = readTargetCaptor.getAllValues();

      List<Long> prefetchPositions = new ArrayList<>();
      List<Long> missedCachePositions = new ArrayList<>();

      ReadTarget failedSegmentRecoveryTarget = null;

      for (int i = 0; i < positions.size(); i++) {
        long position = positions.get(i);
        int length = lengths.get(i);
        ReadType readType = contexts.get(i).getReadType();

        if (readType == ReadType.PREFETCH_READ) {
          prefetchPositions.add(position);

          assertEquals(ONE_MB, length, "Prefetch segments should be 1 MB each");
        }

        if (readType == MISSEDCACHE_READ) {
          missedCachePositions.add(position);

          if (position == failedSegmentOffset) {
            failedSegmentRecoveryTarget = readTargets.get(i);
          }
        }
      }

      /*
       * All four layout ranges must have had a prefetch attempt.
       *
       * Order is not guaranteed because prefetch operations can run
       * concurrently.
       */
      assertThat(prefetchPositions)
          .as("All four layout ranges should have a prefetch attempt")
          .containsExactlyInAnyOrder(
              0L,
              1L * ONE_MB,
              2L * ONE_MB,
              3L * ONE_MB);

      /*
       * The segment whose prefetch failed must appear in the missed-cache
       * path.
       *
       * We intentionally use contains() instead of containsExactly() because
       * thresholdAgeMilliseconds is 0. Successfully prefetched buffers can
       * therefore also expire before being consumed.
       */
      assertThat(missedCachePositions)
          .as("Failed prefetch segment should recover through "
              + "MISSEDCACHE_READ")
          .contains(failedSegmentOffset);

      /*
       * Verify that the failed segment actually received a recovery target.
       */
      assertThat(failedSegmentRecoveryTarget)
          .as("Failed prefetch segment should have a recovery ReadTarget")
          .isNotNull();

      /*
       * The 2 MB segment belongs to stampA in the test layout.
       */
      assertThat(failedSegmentRecoveryTarget.endpoint())
          .as("Recovery should preserve the layout endpoint")
          .contains("stampA");
    } finally {
      bufferManager.resetBufferManager();
    }
  }

    /**
   * Test that verifies the main thread waits for all child read operations to complete.
   *
   * <p>This test validates that when reading a file with a blob layout:
   * <ol>
   *   <li>Prefetch reads are issued for each segment (1MB segments for a 4MB file).</li>
   *   <li>The main thread blocks until all child prefetch operations complete,
   *       even if one range has a controlled 4-second delay.</li>
   *   <li>Read buffer management limits concurrent operations to at most 4 buffers
   *       across in-progress, queued, and completed lists.</li>
   *    <li>We use a single buffer index (parent's index) for all reads.</li>
   *   <li>Data read is correct and exactly matches the generated test data.</li>
   *   <li>Main thread elapsed time reflects the 4-second delay of the slowest segment.</li>
   *   <li>All reads are issued as {@link ReadType#PREFETCH_READ} (no cache misses).</li>
   * </ol>
   *
   * @throws Exception on any failure during setup, mocking, parsing, I/O, or assertion checks
   */
  @Test
  public void testLayoutReadsWaitsForAllChildren() throws Exception {
    assumeThat(getFileSystem().getAbfsStore().getAbfsConfiguration()
            .isDataLocalityEnabled()).isTrue();

    int fileSize = FOUR_MB;
    byte[] testData = generateTestData(fileSize);

    getBufferManager().resetBufferManager(); // reset Buffer to avoid interference from other tests

    AbfsClient mockClient = getMockClientForLayoutRead(FOUR_MB);
    ReadBufferManager bufferManager = getBufferManagerForLayout(mockClient);


    AtomicInteger callCount = new AtomicInteger(0);
    CountDownLatch allReadsCompleted = new CountDownLatch(4);
    AtomicLong mainThreadBlockedTime = new AtomicLong(0);

    // Create stream
    AbfsInputStream inputStream = createInputStreamWithLayout(
            fileSize, FOUR_MB, ONE_MB, 2, mockClient);

    when(mockClient.read(
            nullable(String.class),
            anyLong(),
            nullable(byte[].class),
            anyInt(),
            anyInt(),
            nullable(String.class),
            nullable(String.class),
            nullable(ContextEncryptionAdapter.class),
            nullable(TracingContext.class),
            nullable(ReadTarget.class)
    )).thenAnswer(invocation -> {
      int call = callCount.incrementAndGet();
      long position = invocation.getArgument(1);
      byte[] buffer = invocation.getArgument(2);
      int offset = invocation.getArgument(3);
      int length = invocation.getArgument(4);
      TracingContext tc = invocation.getArgument(8);

      LOG.debug("[Call {}] readFromEndpoint: pos={}, len={}, type={}",
          call, position, length, tc.getReadType());

      // Add controlled delay for segment at 1MB
      if (position == ONE_MB) {
        LOG.debug("Delaying 4 seconds for segment at {}", position);
        Thread.sleep(4000);
      }

      List<ReadBuffer> inProgressBtw = bufferManager.getInProgressListCopy();
      List<ReadBuffer> queuedBtw = bufferManager.getReadAheadQueueCopy();
      List<Integer> freeListBtw = bufferManager.getFreeListCopy();
      List<ReadBuffer> completedBtw = bufferManager.getCompletedReadListCopy();

      LOG.debug("inProgress: {}, queued: {}, freeList: {}, completed: {}",
          inProgressBtw.size(), queuedBtw.size(),
          freeListBtw.size(), completedBtw.size());

      assertThat(freeListBtw.size()).as("Only one buffer index should have been used").isGreaterThanOrEqualTo(15);
      assertThat(inProgressBtw.size()).as("Maximum 4 child buffers should be in inProgressList").isLessThanOrEqualTo(4);
      assertThat(queuedBtw.size()).as("Maximum 4 child buffers should be in readAheadQueue").isLessThanOrEqualTo(4);
      assertThat(completedBtw.size()).as("Maximum 4 child buffers should be in completedList").isLessThanOrEqualTo(4);
      assertThat(completedBtw.size()+queuedBtw.size()+inProgressBtw.size()).as("Maximum 4 child buffers should be present across lists").isEqualTo(4);


      // Copy data
      int bytesToCopy = (int) Math.min(length, fileSize - position);
      System.arraycopy(testData, (int) position, buffer, offset, bytesToCopy);

      LOG.debug("Completed: {} bytes copied", bytesToCopy);

      // Count down latch
      allReadsCompleted.countDown();
      LOG.debug("Remaining calls: {}", allReadsCompleted.getCount());

      AbfsRestOperation mockOp = mock(AbfsRestOperation.class);
      AbfsHttpOperation mockHttpOp = mock(AbfsHttpOperation.class);
      when(mockOp.getResult()).thenReturn(mockHttpOp);
      when(mockHttpOp.getBytesReceived()).thenReturn((long) bytesToCopy);
      when(mockOp.getSasToken()).thenReturn(null);

      return mockOp;
    });

    // Verify initial state
    List<ReadBuffer> inProgressBefore = bufferManager.getInProgressListCopy();
    List<ReadBuffer> queuedBefore = bufferManager.getReadAheadQueueCopy();
    List<Integer> freeListBefore = bufferManager.getFreeListCopy();

    assertEquals(0, inProgressBefore.size(),
            "Should have no in-progress reads before starting");
    assertEquals(0, queuedBefore.size(),
            "Should have no queued reads before starting");
    assertEquals(16, freeListBefore.size(), "Should have at least 1 free buffer");

    LOG.debug("Starting read");
    long startTime = System.currentTimeMillis();

    // Do the read
    byte[] readBuffer = new byte[fileSize];
    int totalBytesRead = inputStream.read(readBuffer, 0, fileSize);

    long elapsed = System.currentTimeMillis() - startTime;
    mainThreadBlockedTime.set(elapsed);

    LOG.debug("Read completed: {} bytes in {} ms", totalBytesRead, elapsed);

    // Wait for all background reads to complete
    LOG.debug("Waiting for all reads to complete");
    boolean completed = allReadsCompleted.await(10, TimeUnit.SECONDS);

    if (!completed) {
      throw new AssertionError(String.format("Only %d/5 calls completed", callCount.get()));
    }
    LOG.debug("All {} calls completed", callCount.get());

    // Verify data correctness
    assertEquals(fileSize, totalBytesRead, "Should have read entire file");
    assertArrayEquals(testData, readBuffer, "Data should match exactly");

    // Verify timing - main thread should have waited for slow segment
    assertTrue(elapsed >= 4000,
            String.format("Main thread should wait at least 4s for slow segment, waited %dms", elapsed));

    LOG.debug("Main thread blocked for {} ms (expected at least 4000 ms)", elapsed);

    // Verify all reads were prefetch (no cache misses)
    ArgumentCaptor<TracingContext> tcCaptor = ArgumentCaptor.forClass(TracingContext.class);
    ArgumentCaptor<Long> positionCaptor = ArgumentCaptor.forClass(Long.class);

    verify(mockClient, times(4)).read(
            nullable(String.class),
            positionCaptor.capture(),
            nullable(byte[].class),
            nullable(Integer.class),
            anyInt(),
            nullable(String.class),
            nullable(String.class),
            nullable(ContextEncryptionAdapter.class),
            tcCaptor.capture(),
            nullable(ReadTarget.class)
    );

    List<TracingContext> contexts = tcCaptor.getAllValues();
    List<Long> positions = positionCaptor.getAllValues();

    for (int i = 0; i < contexts.size(); i++) {
      LOG.debug("Read {}: pos={}, type={}",
          i + 1, positions.get(i), contexts.get(i).getReadType());
      assertEquals(ReadType.PREFETCH_READ, contexts.get(i).getReadType(),
          "All reads should be PREFETCH_READ (no cache misses)");
    }

    // Verify we got exactly 4 reads
    assertEquals(4, callCount.get(), "Should have exactly 4 reads");

    LOG.debug("Main thread waited for all children");

    inputStream.close();
    bufferManager.resetBufferManager();
  }
  /**
   * Get ReadBufferManager with proper accessors
   */
  private ReadBufferManager getBufferManagerForLayout(AbfsClient client) throws Exception {
    AbfsConfiguration abfsConfig = client.getAbfsConfiguration();
    ReadBufferManagerV2.setReadBufferManagerConfigs(
            abfsConfig.getReadBufferSize(),
            abfsConfig
    );

    return ReadBufferManagerV2.getBufferManager(client.getAbfsCounters());
  }

  @Test
  public void testFailedReadAheadEviction() throws Exception {
    AbfsClient client = getMockAbfsClient();
    AbfsRestOperation successOp = getMockRestOp();
    getBufferManager().setThresholdAgeMilliseconds(INCREASED_READ_BUFFER_AGE_THRESHOLD);
    // Stub :
    // Read request leads to 3 readahead calls: Fail all 3 readahead-client.read()
    // Actual read request fails with the failure in readahead thread
    doThrow(new TimeoutException("Internal Server error"))
        .when(client)
        .read(any(String.class), any(Long.class), any(byte[].class),
            any(Integer.class), any(Integer.class), any(String.class),
            any(String.class), any(), any(TracingContext.class));

    AbfsInputStream inputStream = getAbfsInputStream(client, "testFailedReadAheadEviction.txt");

    // Add a failed buffer to completed queue and set to no free buffers to read ahead.
    ReadBuffer buff = new ReadBuffer();
    buff.setStatus(ReadBufferStatus.READ_FAILED);
    buff.setStream(inputStream);
    getBufferManager().testMimicFullUseAndAddFailedBuffer(buff);

    // if read failed buffer eviction is tagged as a valid eviction, it will lead to
    // wrong assumption of queue logic that a buffer is freed up and can lead to :
    // java.util.EmptyStackException
    // at java.util.Stack.peek(Stack.java:102)
    // at java.util.Stack.pop(Stack.java:84)
    // at org.apache.hadoop.fs.azurebfs.services.ReadBufferManager.queueReadAhead
    getBufferManager().queueReadAhead(inputStream, 0, ONE_KB,
        getTestTracingContext(getFileSystem(), true), null);
  }

  /**
   *
   * The test expects AbfsInputStream to initiate a remote read request for
   * the request offset and length when previous read ahead on the offset had failed.
   * Also checks that the ReadBuffers are evicted as per the ReadBufferManager
   * threshold criteria.
   * @throws Exception
   */
  @Test
  public void testOlderReadAheadFailure() throws Exception {
    AbfsClient client = getMockAbfsClient();
    AbfsRestOperation successOp = getMockRestOp();

    // Stub :
    // First Read request leads to 3 readahead calls: Fail all 3 readahead-client.read()
    // A second read request will see that readahead had failed for data in
    // the requested offset range and also that its is an older readahead request.
    // So attempt a new read only for the requested range.
    doThrow(new TimeoutException("Internal Server error for RAH-X"))
        .doThrow(new TimeoutException("Internal Server error for RAH-Y"))
        .doThrow(new TimeoutException("Internal Server error for RAH-Z"))
        .doReturn(successOp) // pass the read for second read request
        .doReturn(successOp) // pass success for post eviction test
        .when(client)
        .read(any(String.class), any(Long.class), any(byte[].class),
            any(Integer.class), any(Integer.class), any(String.class),
            any(String.class), any(), any(TracingContext.class));

    AbfsInputStream inputStream = getAbfsInputStream(client, "testOlderReadAheadFailure.txt");

    // First read request that fails as the readahead triggered from this request failed.
    intercept(IOException.class,
        () -> inputStream.read(new byte[ONE_KB]));

    // Only the 3 readAhead threads should have triggered client.read
    verifyReadCallCount(client, 3);

    // Sleep for thresholdAgeMs so that the read ahead buffer qualifies for being old.
    Thread.sleep(getBufferManager().getThresholdAgeMilliseconds());

    // Second read request should retry the read (and not issue any new readaheads)
    inputStream.read(ONE_KB, new byte[ONE_KB], 0, ONE_KB);

    // Once created, mock will remember all interactions. So total number of read
    // calls will be one more from earlier (there is a reset mock which will reset the
    // count, but the mock stub is erased as well which needs AbsInputStream to be recreated,
    // which beats the purpose)
    verifyReadCallCount(client, 4);

    // Stub returns success for the 5th read request, if ReadBuffers still
    // persisted request would have failed for position 0.
    checkEvictedStatus(inputStream, 0, false);
  }

  /**
   * The test expects AbfsInputStream to utilize any data read ahead for
   * requested offset and length.
   * @throws Exception
   */
  @Test
  public void testSuccessfulReadAhead() throws Exception {
    // Mock failure for client.read()
    AbfsClient client = getMockAbfsClient();

    // Success operation mock
    AbfsRestOperation op = getMockRestOp();

    // Stub :
    // Pass all readAheads and fail the post eviction request to
    // prove ReadAhead buffer is used
    // for post eviction check, fail all read aheads
    doReturn(op)
        .doReturn(op)
        .doReturn(op)
        .doThrow(new TimeoutException("Internal Server error for RAH-X"))
        .doThrow(new TimeoutException("Internal Server error for RAH-Y"))
        .doThrow(new TimeoutException("Internal Server error for RAH-Z"))
        .when(client)
        .read(any(String.class), any(Long.class), any(byte[].class),
            any(Integer.class), any(Integer.class), any(String.class),
            any(String.class), any(), any(TracingContext.class));

    AbfsInputStream inputStream = getAbfsInputStream(client, "testSuccessfulReadAhead.txt");
    int beforeReadCompletedListSize = getBufferManager().getCompletedReadListSize();

    // First read request that triggers readAheads.
    inputStream.read(new byte[ONE_KB]);

    // Only the 3 readAhead threads should have triggered client.read
    verifyReadCallCount(client, 3);
    int newAdditionsToCompletedRead =
        getBufferManager().getCompletedReadListSize()
            - beforeReadCompletedListSize;
    // read buffer might be dumped if the ReadBufferManager getblock preceded
    // the action of buffer being picked for reading from readaheadqueue, so that
    // inputstream can proceed with read and not be blocked on readahead thread
    // availability. So the count of buffers in completedReadQueue for the stream
    // can be same or lesser than the requests triggered to queue readahead.
    assertThat(newAdditionsToCompletedRead)
        .describedAs(
            "New additions to completed reads should be same or less than as number of readaheads")
        .isLessThanOrEqualTo(3);

    // Another read request whose requested data is already read ahead.
    inputStream.read(ONE_KB, new byte[ONE_KB], 0, ONE_KB);

    // Once created, mock will remember all interactions.
    // As the above read should not have triggered any server calls, total
    // number of read calls made at this point will be same as last.
    verifyReadCallCount(client, 3);

    // Stub will throw exception for client.read() for 4th and later calls
    // if not using the read-ahead buffer exception will be thrown on read
    checkEvictedStatus(inputStream, 0, true);
  }

  /**
   * This test expects InProgressList is not purged by the inputStream close.
   */
  @Test
  public void testStreamPurgeDuringReadAheadCallExecuting() throws Exception {
    AbfsClient client = getMockAbfsClient();
    AbfsRestOperation successOp = getMockRestOp();
    final Long serverCommunicationMockLatency = 3_000L;
    final Long readBufferTransferToInProgressProbableTime = 1_000L;
    final Integer readBufferQueuedCount = 3;

    Mockito.doAnswer(invocationOnMock -> {
          //sleeping thread to mock the network latency from client to backend.
          Thread.sleep(serverCommunicationMockLatency);
          return successOp;
        })
        .when(client)
        .read(any(String.class), any(Long.class), any(byte[].class),
            any(Integer.class), any(Integer.class), any(String.class),
            any(String.class), nullable(ContextEncryptionAdapter.class),
            any(TracingContext.class));

    final ReadBufferManager readBufferManager
        = getBufferManager();

    final int readBufferTotal = readBufferManager.getNumBuffers();
    final int expectedFreeListBufferCount = readBufferTotal
        - readBufferQueuedCount;

    try (AbfsInputStream inputStream = getAbfsInputStream(client,
        "testSuccessfulReadAhead.txt")) {
      // As this is try-with-resources block, the close() method of the created
      // abfsInputStream object shall be called on the end of the block.
      queueReadAheads(inputStream);

      //Sleeping to give ReadBufferWorker to pick the readBuffers for processing.
      Thread.sleep(readBufferTransferToInProgressProbableTime);

      assertThat(readBufferManager.getInProgressListCopy())
          .describedAs(String.format("InProgressList should have %d elements",
              readBufferQueuedCount))
          .hasSize(readBufferQueuedCount);
      assertThat(readBufferManager.getFreeListCopy())
          .describedAs(String.format("FreeList should have %d elements",
              expectedFreeListBufferCount))
          .hasSize(expectedFreeListBufferCount);
      assertThat(readBufferManager.getCompletedReadListCopy())
          .describedAs("CompletedList should have 0 elements")
          .hasSize(0);
    }

    assertThat(readBufferManager.getInProgressListCopy())
        .describedAs(String.format("InProgressList should have %d elements",
            readBufferQueuedCount))
        .hasSize(readBufferQueuedCount);
    assertThat(readBufferManager.getFreeListCopy())
        .describedAs(String.format("FreeList should have %d elements",
            expectedFreeListBufferCount))
        .hasSize(expectedFreeListBufferCount);
    assertThat(readBufferManager.getCompletedReadListCopy())
        .describedAs("CompletedList should have 0 elements")
        .hasSize(0);
  }

  /**
   * This test expects ReadAheadManager to throw exception if the read ahead
   * thread had failed within the last thresholdAgeMilliseconds.
   * Also checks that the ReadBuffers are evicted as per the ReadBufferManager
   * threshold criteria.
   * @throws Exception
   */
  @Test
  public void testReadAheadManagerForFailedReadAhead() throws Exception {
    AbfsClient client = getMockAbfsClient();
    AbfsRestOperation successOp = getMockRestOp();

    // Stub :
    // Read request leads to 3 readahead calls: Fail all 3 readahead-client.read()
    // Actual read request fails with the failure in readahead thread
    doThrow(new TimeoutException("Internal Server error for RAH-Thread-X"))
        .doThrow(new TimeoutException("Internal Server error for RAH-Thread-Y"))
        .doThrow(new TimeoutException("Internal Server error RAH-Thread-Z"))
        .doReturn(successOp) // Any extra calls to read, pass it.
        .when(client)
        .read(any(String.class), any(Long.class), any(byte[].class),
            any(Integer.class), any(Integer.class), any(String.class),
            any(String.class), any(), any(TracingContext.class));

    AbfsInputStream inputStream = getAbfsInputStream(client, "testReadAheadManagerForFailedReadAhead.txt");

    queueReadAheads(inputStream);

    // AbfsInputStream Read would have waited for the read-ahead for the requested offset
    // as we are testing from ReadAheadManager directly, sleep for a sec to
    // get the read ahead threads to complete
    Thread.sleep(1000);

    // if readAhead failed for specific offset, getBlock should
    // throw exception from the ReadBuffer that failed within last thresholdAgeMilliseconds sec
    intercept(IOException.class,
        () -> getBufferManager().getBlock(
            inputStream,
            0,
            ONE_KB,
            new byte[ONE_KB]));

    // Only the 3 readAhead threads should have triggered client.read
    verifyReadCallCount(client, 3);

    // Stub returns success for the 4th read request, if ReadBuffers still
    // persisted, ReadAheadManager getBlock would have returned exception.
    checkEvictedStatus(inputStream, 0, false);
  }

  /**
   * The test expects ReadAheadManager to return 0 receivedBytes when previous
   * read ahead on the offset had failed and not throw exception received then.
   * Also checks that the ReadBuffers are evicted as per the ReadBufferManager
   * threshold criteria.
   * @throws Exception
   */
  @Test
  public void testReadAheadManagerForOlderReadAheadFailure() throws Exception {
    AbfsClient client = getMockAbfsClient();
    AbfsRestOperation successOp = getMockRestOp();

    // Stub :
    // First Read request leads to 3 readahead calls: Fail all 3 readahead-client.read()
    // A second read request will see that readahead had failed for data in
    // the requested offset range but also that its is an older readahead request.
    // System issue could have resolved by now, so attempt a new read only for the requested range.
    doThrow(new TimeoutException("Internal Server error for RAH-X"))
        .doThrow(new TimeoutException("Internal Server error for RAH-X"))
        .doThrow(new TimeoutException("Internal Server error for RAH-X"))
        .doReturn(successOp) // pass the read for second read request
        .doReturn(successOp) // pass success for post eviction test
        .when(client)
        .read(any(String.class), any(Long.class), any(byte[].class),
            any(Integer.class), any(Integer.class), any(String.class),
            any(String.class), any(), any(TracingContext.class));

    AbfsInputStream inputStream = getAbfsInputStream(client, "testReadAheadManagerForOlderReadAheadFailure.txt");

    queueReadAheads(inputStream);

    // AbfsInputStream Read would have waited for the read-ahead for the requested offset
    // as we are testing from ReadAheadManager directly, sleep for thresholdAgeMilliseconds so that
    // read buffer qualifies for to be an old buffer
    Thread.sleep(getBufferManager().getThresholdAgeMilliseconds());

    // Only the 3 readAhead threads should have triggered client.read
    verifyReadCallCount(client, 3);

    // getBlock from a new read request should return 0 if there is a failure
    // 30 sec before in read ahead buffer for respective offset.
    int bytesRead = getBufferManager().getBlock(
        inputStream,
        ONE_KB,
        ONE_KB,
        new byte[ONE_KB]);
    assertEquals(0, bytesRead,
        "bytesRead should be zero when previously read "+ "ahead buffer had failed");

    // Stub returns success for the 5th read request, if ReadBuffers still
    // persisted request would have failed for position 0.
    checkEvictedStatus(inputStream, 0, false);
  }

  /**
   * The test expects ReadAheadManager to return data from previously read
   * ahead data of same offset.
   * @throws Exception
   */
  @Test
  public void testReadAheadManagerForSuccessfulReadAhead() throws Exception {
    // Mock failure for client.read()
    AbfsClient client = getMockAbfsClient();

    // Success operation mock
    AbfsRestOperation op = getMockRestOp();

    // Stub :
    // Pass all readAheads and fail the post eviction request to
    // prove ReadAhead buffer is used
    doReturn(op)
        .doReturn(op)
        .doReturn(op)
        .doThrow(new TimeoutException("Internal Server error for RAH-X")) // for post eviction request
        .doThrow(new TimeoutException("Internal Server error for RAH-Y"))
        .doThrow(new TimeoutException("Internal Server error for RAH-Z"))
        .when(client)
        .read(any(String.class), any(Long.class), any(byte[].class),
            any(Integer.class), any(Integer.class), any(String.class),
            any(String.class), any(), any(TracingContext.class));

    AbfsInputStream inputStream = getAbfsInputStream(client, "testSuccessfulReadAhead.txt");

    queueReadAheads(inputStream);

    // AbfsInputStream Read would have waited for the read-ahead for the requested offset
    // as we are testing from ReadAheadManager directly, sleep for a sec to
    // get the read ahead threads to complete
    Thread.sleep(1000);

    // Only the 3 readAhead threads should have triggered client.read
    verifyReadCallCount(client, 3);

    // getBlock for a new read should return the buffer read-ahead
    int bytesRead = getBufferManager().getBlock(
        inputStream,
        ONE_KB,
        ONE_KB,
        new byte[ONE_KB]);

    Assertions.assertTrue(bytesRead > 0, "bytesRead should be non-zero from the "
        + "buffer that was read-ahead");

    // Once created, mock will remember all interactions.
    // As the above read should not have triggered any server calls, total
    // number of read calls made at this point will be same as last.
    verifyReadCallCount(client, 3);

    // Stub will throw exception for client.read() for 4th and later calls
    // if not using the read-ahead buffer exception will be thrown on read
    checkEvictedStatus(inputStream, 0, true);
  }

  /**
   * Test readahead with different config settings for request request size and
   * readAhead block size
   * @throws Exception
   */
  @Order(Integer.MAX_VALUE)
  @Test
  public void testDiffReadRequestSizeAndRAHBlockSize() throws Exception {
    // Set requestRequestSize = 4MB and readAheadBufferSize=8MB
    resetReadBufferManager(FOUR_MB, INCREASED_READ_BUFFER_AGE_THRESHOLD);
    testReadAheadConfigs(FOUR_MB, TEST_READAHEAD_DEPTH_4, false, EIGHT_MB);

    // Test for requestRequestSize =16KB and readAheadBufferSize=16KB
    resetReadBufferManager(SIXTEEN_KB, INCREASED_READ_BUFFER_AGE_THRESHOLD);
    AbfsInputStream inputStream = testReadAheadConfigs(SIXTEEN_KB,
        TEST_READAHEAD_DEPTH_2, true, SIXTEEN_KB);
    testReadAheads(inputStream, SIXTEEN_KB, SIXTEEN_KB);

    // Test for requestRequestSize =16KB and readAheadBufferSize=48KB
    resetReadBufferManager(FORTY_EIGHT_KB, INCREASED_READ_BUFFER_AGE_THRESHOLD);
    inputStream = testReadAheadConfigs(SIXTEEN_KB, TEST_READAHEAD_DEPTH_2, true,
        FORTY_EIGHT_KB);
    testReadAheads(inputStream, SIXTEEN_KB, FORTY_EIGHT_KB);

    // Test for requestRequestSize =48KB and readAheadBufferSize=16KB
    resetReadBufferManager(FORTY_EIGHT_KB, INCREASED_READ_BUFFER_AGE_THRESHOLD);
    inputStream = testReadAheadConfigs(FORTY_EIGHT_KB, TEST_READAHEAD_DEPTH_2,
        true,
        SIXTEEN_KB);
    testReadAheads(inputStream, FORTY_EIGHT_KB, SIXTEEN_KB);
  }

  @Test
  public void testDefaultReadaheadQueueDepth() throws Exception {
    Configuration config = getRawConfiguration();
    config.unset(FS_AZURE_READ_AHEAD_QUEUE_DEPTH);
    AzureBlobFileSystem fs = getFileSystem(config);
    Path testFile = path("/testFile");
    fs.create(testFile).close();
    FSDataInputStream in = fs.open(testFile);
    assertThat(
        ((AbfsInputStream) in.getWrappedStream()).getReadAheadQueueDepth())
        .describedAs("readahead queue depth should be set to default value 2")
        .isEqualTo(2);
    in.close();
  }

  /**
   * Test to verify that the read type and position are correctly set in the
   * client request id header for various type of read operations performed.
   * @throws Exception if any error occurs during the test
   */
  @Test
  public void testReadTypeInTracingContextHeader() throws Exception {
    AzureBlobFileSystem spiedFs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore spiedStore = Mockito.spy(spiedFs.getAbfsStore());
    AbfsConfiguration spiedConfig = Mockito.spy(spiedStore.getAbfsConfiguration());
    AbfsClient spiedClient = Mockito.spy(spiedStore.getClient());
    AbfsClient spiedBlobClient = Mockito.spy(spiedStore.getClient(AbfsServiceType.BLOB));
    Mockito.doReturn(ONE_MB).when(spiedConfig).getReadBufferSize();
    Mockito.doReturn(ONE_MB).when(spiedConfig).getReadAheadBlockSize();
    Mockito.doReturn(spiedClient).when(spiedStore).getClient();
    Mockito.doReturn(spiedBlobClient).when(spiedStore).getClient(AbfsServiceType.BLOB);
    Mockito.doReturn(spiedStore).when(spiedFs).getAbfsStore();
    Mockito.doReturn(spiedConfig).when(spiedStore).getAbfsConfiguration();
    int totalReadCalls = 0;
    int fileSize;

    /*
     * Test to verify Normal Read Type.
     * Disabling read ahead ensures that read type is normal read.
     */
    fileSize = 3 * ONE_MB; // To make sure multiple blocks are read.
    totalReadCalls += 3; // 3 blocks of 1MB each.
    doReturn(false).when(spiedConfig).isReadAheadV2Enabled();
    doReturn(false).when(spiedConfig).isReadAheadEnabled();
    testReadTypeInTracingContextHeaderInternal(spiedFs, fileSize, NORMAL_READ, 3, totalReadCalls);

    /*
     * Test to verify Missed Cache Read Type.
     * Setting read ahead depth to 0 ensure that nothing can be got from prefetch.
     * In such a case Input Stream will do a sequential read with missed cache read type.
     */
    fileSize = 3 * ONE_MB; // To make sure multiple blocks are read with MR
    totalReadCalls += 3; // 3 block of 1MB.
    Mockito.doReturn(0).when(spiedConfig).getReadAheadQueueDepth();
    Mockito.doReturn(FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL).when(spiedConfig).getAbfsReadPolicy();
    doReturn(true).when(spiedConfig).isReadAheadEnabled();
    testReadTypeInTracingContextHeaderInternal(spiedFs, fileSize, MISSEDCACHE_READ, 3, totalReadCalls);

    /*
     * Test to verify Prefetch Read Type.
     * Setting read ahead depth to 2 with prefetch enabled ensures that prefetch is done.
     * First read here might be Normal or Missed Cache but the rest 2 should be Prefetched Read.
     */
    fileSize = 3 * ONE_MB; // To make sure multiple blocks are read.
    totalReadCalls += 3;
    doReturn(true).when(spiedConfig).isReadAheadEnabled();
    Mockito.doReturn(3).when(spiedConfig).getReadAheadQueueDepth();
    testReadTypeInTracingContextHeaderInternal(spiedFs, fileSize, PREFETCH_READ, 3, totalReadCalls);

    /*
     * Test to verify Footer Read Type.
     * Having file size less than footer read size and disabling small file opt
     */
    fileSize = 8 * ONE_KB;
    totalReadCalls += 1; // Full file will be read along with footer.
    doReturn(false).when(spiedConfig).readSmallFilesCompletely();
    doReturn(true).when(spiedConfig).optimizeFooterRead();
    testReadTypeInTracingContextHeaderInternal(spiedFs, fileSize, FOOTER_READ, 1, totalReadCalls);

    /*
     * Test to verify Small File Read Type.
     * Having file size less than block size and disabling footer read opt
     */
    totalReadCalls += 1; // Full file will be read along with footer.
    doReturn(true).when(spiedConfig).readSmallFilesCompletely();
    doReturn(false).when(spiedConfig).optimizeFooterRead();
    testReadTypeInTracingContextHeaderInternal(spiedFs, fileSize, SMALLFILE_READ, 1, totalReadCalls);

    /*
     * Test to verify Random Read Type.
     * Setting Read Policy to Parquet ensures Random Read Type.
     */
    fileSize = 3 * ONE_MB; // To make sure multiple blocks are read.
    totalReadCalls += 3; // Full file will be read along with footer.
    doReturn(FS_OPTION_OPENFILE_READ_POLICY_PARQUET).when(spiedConfig).getAbfsReadPolicy();
    testReadTypeInTracingContextHeaderInternal(spiedFs, fileSize, RANDOM_READ, 1, totalReadCalls);

    /*
     * Test to verify Direct Read Type and a read from random position.
     * Separate AbfsInputStream method needs to be called.
     */
    fileSize = ONE_MB;
    totalReadCalls += 1;
    doReturn(false).when(spiedConfig).readSmallFilesCompletely();
    doReturn(true).when(spiedConfig).isBufferedPReadDisabled();
    Path testPath = createTestFile(spiedFs, fileSize);
    try (FSDataInputStream iStream = spiedFs.open(testPath)) {
      AbfsInputStream stream = (AbfsInputStream) iStream.getWrappedStream();
      int bytesRead = stream.read(ONE_MB/3, new byte[fileSize], 0,
          fileSize);
      assertThat(fileSize - ONE_MB/3)
          .describedAs("Read size should match file size")
          .isEqualTo(bytesRead);
    }
    assertReadTypeInClientRequestId(spiedFs, 1, totalReadCalls, DIRECT_READ);
  }

  private void testReadTypeInTracingContextHeaderInternal(AzureBlobFileSystem fs,
      int fileSize, ReadType readType, int numOfReadCalls, int totalReadCalls) throws Exception {
    Path testPath = createTestFile(fs, fileSize);
    readFile(fs, testPath, fileSize, readType);
    assertReadTypeInClientRequestId(fs, numOfReadCalls, totalReadCalls, readType);
  }

  /**
   * Test to verify that both conditions of prefetch read and respective config
   * enabled needs to be true for the priority header to be added
   */
  @Test
  public void testPrefetchReadAddsPriorityHeaderWithDifferentConfigs()
      throws Exception {
    Configuration configuration1 = new Configuration(getRawConfiguration());
    configuration1.set(FS_AZURE_ENABLE_PREFETCH_REQUEST_PRIORITY, "true");

    Configuration configuration2 = new Configuration(getRawConfiguration());
    configuration2.set(FS_AZURE_ENABLE_PREFETCH_REQUEST_PRIORITY, "false");

    TracingContext tracingContext1 = mock(TracingContext.class);
    when(tracingContext1.getReadType()).thenReturn(PREFETCH_READ);

    //Prefetch Read with config enabled
    executePrefetchReadTest(tracingContext1, configuration1, true);
    //Prefetch Read with config disabled
    executePrefetchReadTest(tracingContext1, configuration2, false);

    when(tracingContext1.getReadType()).thenReturn(DIRECT_READ);

    //Non-prefetch read with config disabled
    executePrefetchReadTest(tracingContext1, configuration2, false);
    //Non-prefetch read with config enabled
    executePrefetchReadTest(tracingContext1, configuration1, false);
  }

  /**
   * Test to verify that the correct AbfsInputStream instance is created
   * based on the read policy set in AbfsConfiguration.
   */
  @Test
  public void testAbfsInputStreamInstance() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("/testPath");
    fs.create(path).close();

    // Assert that Sequential Read Policy uses Prefetch Input Stream
    getAbfsStore(fs).getAbfsConfiguration().setAbfsReadPolicy(FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL);
    InputStream stream = fs.open(path).getWrappedStream();
    assertThat(stream).isInstanceOf(AbfsPrefetchInputStream.class);
    stream.close();

    // Assert that Adaptive Read Policy uses Adaptive Input Stream
    getAbfsStore(fs).getAbfsConfiguration().setAbfsReadPolicy(FS_OPTION_OPENFILE_READ_POLICY_ADAPTIVE);
    stream = fs.open(path).getWrappedStream();
    assertThat(stream).isInstanceOf(AbfsAdaptiveInputStream.class);
    stream.close();

    // Assert that Parquet Read Policy uses Random Input Stream
    getAbfsStore(fs).getAbfsConfiguration().setAbfsReadPolicy(FS_OPTION_OPENFILE_READ_POLICY_PARQUET);
    stream = fs.open(path).getWrappedStream();
    assertThat(stream).isInstanceOf(AbfsRandomInputStream.class);
    stream.close();

    // Assert that Avro Read Policy uses Adaptive Input Stream
    getAbfsStore(fs).getAbfsConfiguration().setAbfsReadPolicy(FS_OPTION_OPENFILE_READ_POLICY_AVRO);
    stream = fs.open(path).getWrappedStream();
    assertThat(stream).isInstanceOf(AbfsAdaptiveInputStream.class);
    stream.close();
  }

  /**
   * Test to verify that Random Input Stream does not queue prefetches.
   * @throws Exception if any error occurs during the test
   */
  @Test
  public void testRandomInputStreamDoesNotQueuePrefetches() throws Exception {
    AzureBlobFileSystem spiedFs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore spiedStore = Mockito.spy(spiedFs.getAbfsStore());
    AbfsConfiguration spiedConfig = Mockito.spy(spiedStore.getAbfsConfiguration());
    AbfsClient spiedClient = Mockito.spy(spiedStore.getClient());
    AbfsClient spiedBlobClient = Mockito.spy(spiedStore.getClient(AbfsServiceType.BLOB));
    Mockito.doReturn(ONE_MB).when(spiedConfig).getReadBufferSize();
    Mockito.doReturn(ONE_MB).when(spiedConfig).getReadAheadBlockSize();
    Mockito.doReturn(spiedClient).when(spiedStore).getClient();
    Mockito.doReturn(spiedBlobClient).when(spiedStore).getClient(AbfsServiceType.BLOB);
    Mockito.doReturn(spiedStore).when(spiedFs).getAbfsStore();
    Mockito.doReturn(spiedConfig).when(spiedStore).getAbfsConfiguration();

    int fileSize = 3 * ONE_MB; // To make sure multiple blocks are read.
    int totalReadCalls = 3;
    Mockito.doReturn(3).when(spiedConfig).getReadAheadQueueDepth();
    Mockito.doReturn(FS_OPTION_OPENFILE_READ_POLICY_PARQUET).when(spiedConfig).getAbfsReadPolicy();
    testReadTypeInTracingContextHeaderInternal(spiedFs, fileSize, RANDOM_READ, 3, totalReadCalls);
  }

  /**
   * Test to verify that Adaptive Input Stream queues prefetches for in-order reads
   * and performs random reads for out-of-order seeks.
   * @throws Exception if any error occurs during the test
   */
  @Test
  public void testAdaptiveInputStream() throws Exception {
    AzureBlobFileSystem spiedFs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore spiedStore = Mockito.spy(spiedFs.getAbfsStore());
    AbfsConfiguration spiedConfig = Mockito.spy(spiedStore.getAbfsConfiguration());
    AbfsClient spiedClient = Mockito.spy(spiedStore.getClient());
    AbfsClient spiedBlobClient = Mockito.spy(spiedStore.getClient(AbfsServiceType.BLOB));
    Mockito.doReturn(ONE_MB).when(spiedConfig).getReadBufferSize();
    Mockito.doReturn(ONE_MB).when(spiedConfig).getReadAheadBlockSize();
    Mockito.doReturn(ONE_KB).when(spiedConfig).getReadAheadRange();
    Mockito.doReturn(FS_OPTION_OPENFILE_READ_POLICY_ADAPTIVE).when(spiedConfig).getAbfsReadPolicy();
    Mockito.doReturn(spiedClient).when(spiedStore).getClient();
    Mockito.doReturn(spiedBlobClient).when(spiedStore).getClient(AbfsServiceType.BLOB);
    Mockito.doReturn(spiedStore).when(spiedFs).getAbfsStore();
    Mockito.doReturn(spiedConfig).when(spiedStore).getAbfsConfiguration();

    int fileSize = 10 * ONE_MB;
    Path testPath = createTestFile(spiedFs, fileSize);

    try (FSDataInputStream iStream = spiedFs.open(testPath)) {
      assertThat(iStream.getWrappedStream()).isInstanceOf(AbfsAdaptiveInputStream.class);

      // In order reads trigger prefetches in adaptive stream
      int bytesRead = iStream.read(new byte[2 * ONE_MB], 0, 2 * ONE_MB);
      assertReadTypeInClientRequestId(spiedFs, 3, 3, PREFETCH_READ);
      assertThat(bytesRead).isEqualTo(2 * ONE_MB);

      // Out of order seek causes random read
      iStream.seek(7 * ONE_MB);
      bytesRead = iStream.read(new byte[ONE_MB/2], 0, ONE_MB/2);
      assertReadTypeInClientRequestId(spiedFs, 1, 4, RANDOM_READ);
    }
  }

  @Test
  public void testCacheStateAfterMultipleReads() throws Exception {
    AzureBlobFileSystem fs = dataLocalityCacheCheck();

    Path filePath = createTestFile(fs, 100 * ONE_MB);
    FileStatus fileStatus = fs.getFileStatus(filePath);
    String eTag = ((VersionedFileStatus) fileStatus).getEtag();
    try (FSDataInputStream iStream = fs.open(filePath)) {
      // 0-1MB call, but it will fetch extra layout: (0-64MB)
      iStream.read(new byte[ONE_MB], 0, ONE_MB);
      BlobLayoutCache instance = BlobLayoutCache.getInstance(1,
          DEFAULT_FS_AZURE_BLOB_LAYOUT_CACHE_MAX_COUNT);
      String layoutKey =
          ((AbfsInputStream) iStream.getWrappedStream()).getLayoutCacheKey();
      List<BlobLayout.BlobRange> gaps = instance.getGaps(layoutKey, 0,
          64 * ONE_MB - 1);
      assertThat(gaps).describedAs("No gaps").isEmpty();

      iStream.read(65 * ONE_MB, new byte[ONE_MB], 0, ONE_MB);
      gaps = instance.getGaps(layoutKey, 0, 100 * ONE_MB);
      assertThat(gaps).describedAs("No gaps").isEmpty();
    }
  }

  @Test
  public void testNumberOfLayoutCalls() throws Exception {
    Configuration configuration = getRawConfiguration();
    configuration.setBoolean(FS_AZURE_ENABLE_READAHEAD_V2, true);
    AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(
        configuration);
    assumeThat(fs.getAbfsStore().getAbfsConfiguration()
        .isDataLocalityEnabled()).isTrue();
    AbfsBlobClient client = (AbfsBlobClient) Mockito.spy(
        fs.getAbfsStore().getClient(AbfsServiceType.BLOB));
    Path filePath = createTestFile(fs, 100 * ONE_MB);
    String eTag = ((VersionedFileStatus) fs.getFileStatus(filePath)).getEtag();

    AtomicInteger getlayoutCallCount = new AtomicInteger(0);
    doAnswer(invocation -> {
      getlayoutCallCount.incrementAndGet();
      return invocation.callRealMethod();
    }).when(client)
        .getBlobLayout(anyString(), anyLong(), anyLong(), anyString(),
            nullable(String.class), any());

    AtomicInteger getBlobCallCount = new AtomicInteger(0);
    doAnswer(invocation -> {
      getBlobCallCount.incrementAndGet();
      return invocation.callRealMethod();
    }).when(client).read(anyString(), anyLong(), any(byte[].class), anyInt(),
        anyInt(), anyString(), nullable(String.class), any(),
        any(TracingContext.class), any(ReadTarget.class));

    // 0-4 and 4-8 MB call
    Thread thread1 = new Thread(() -> inputStreamCall(client,
        fs.getAbfsStore().getRelativePath(fs.makeQualified(filePath)), eTag,
        0));

    // 8-12 and 12-16 MB call
    Thread thread2 = new Thread(() -> inputStreamCall(client,
        fs.getAbfsStore().getRelativePath(fs.makeQualified(filePath)), eTag,
        8 * ONE_MB));

    // 16-20 and 20-24 MB call
    Thread thread3 = new Thread(() -> inputStreamCall(client,
        fs.getAbfsStore().getRelativePath(fs.makeQualified(filePath)), eTag,
        16 * ONE_MB));

    // We want first call to proceed and trigger layout fetch before other calls come in, so adding sleep.
    thread1.start();
    Thread.sleep(100);
    thread2.start();
    thread3.start();
    thread1.join();
    thread2.join();
    thread3.join();
    // Since Three streams trying to access same position of same file,
    // but flow will call get layout call only once and result will be shared
    // across all streams.
    assertThat(getlayoutCallCount.get()).isEqualTo(1);
    assertThat(getBlobCallCount.get()).isEqualTo(6);
  }

  private void inputStreamCall(AbfsClient client,
      String filePath,
      String eTag,
      long position) {
    BlobLayoutCache instance = BlobLayoutCache.getInstance(1,
        DEFAULT_FS_AZURE_BLOB_LAYOUT_CACHE_MAX_COUNT);
    try {
      AbfsInputStreamContext inputStreamContext = new AbfsInputStreamContext(
          -1);
      AbfsInputStream inputStream = new AbfsAdaptiveInputStream(
          client,
          null,
          filePath,
          100 * ONE_MB,
          inputStreamContext.withReadBufferSize(4 * ONE_MB)
              .withReadAheadQueueDepth(2)
              .withReadAheadBlockSize(4 * ONE_MB)
              .isReadAheadV2Enabled(true),
          eTag,
          getTestTracingContext(null, false));

      int length = inputStream.read(position, new byte[4 * ONE_MB], 0,
          4 * ONE_MB);
      assertThat(length).isEqualTo(4 * ONE_MB);
      List<BlobLayout.BlobRange> gaps = instance.getGaps(inputStream.getLayoutCacheKey(), 0,
          64 * ONE_MB - 1);
      assertThat(gaps).describedAs("No gaps").isEmpty();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testLayoutCacheAfterFooterRead() throws Exception {
    AzureBlobFileSystem fs = dataLocalityCacheCheck();

    // 100MB file created
    Path filePath = createTestFile(fs, 100 * ONE_MB);
    String eTag = ((VersionedFileStatus) fs.getFileStatus(filePath)).getEtag();
    try (FSDataInputStream iStream = fs.open(filePath)) {
      BlobLayoutCache instance = BlobLayoutCache.getInstance(1,
          DEFAULT_FS_AZURE_BLOB_LAYOUT_CACHE_MAX_COUNT);

      // Read last two MB data
      iStream.read(98 * ONE_MB, new byte[4 * ONE_MB], 0, 4 * ONE_MB);
      String layoutKey =
          ((AbfsInputStream) iStream.getWrappedStream()).getLayoutCacheKey();
      // above read call will fetch the layout for 36MB to 100MB-1
      List<BlobLayout.BlobRange> gaps = instance.getGaps(layoutKey, 0, 100 * ONE_MB);
      assertThat(gaps)
          .describedAs("One gap is present from 0 to 36MB-1")
          .hasSize(1);
      assertThat(gaps.get(0).start())
          .describedAs("Gap should start from 0").isEqualTo(0);
      assertThat(gaps.get(0).end())
          .describedAs("Gap should end at 36MB - 1").isEqualTo(36 * ONE_MB - 1);
    }
  }

  @Test
  public void testLayoutCacheAfterRandomRead() throws Exception {
    AzureBlobFileSystem fs = dataLocalityCacheCheck();

    // 100MB file created
    Path filePath = createTestFile(fs, 100 * ONE_MB);
    FileStatus fileStatus = fs.getFileStatus(filePath);
    String eTag = ((VersionedFileStatus) fileStatus).getEtag();
    try (FSDataInputStream iStream = fs.open(filePath)) {
      BlobLayoutCache instance = BlobLayoutCache.getInstance(1,
          DEFAULT_FS_AZURE_BLOB_LAYOUT_CACHE_MAX_COUNT);

      String layoutKey =
          ((AbfsInputStream) iStream.getWrappedStream()).getLayoutCacheKey();
      // Read 4MB of data from 30MB. Layout fetch will happen from 30MB to 94MB - 1
      iStream.read(30 * ONE_MB, new byte[4 * ONE_MB], 0, 4 * ONE_MB);
      // above read call will fetch the layout for 36MB to 100MB-1
      List<BlobLayout.BlobRange> gaps = instance.getGaps(layoutKey, 0, 100 * ONE_MB);
      assertThat(gaps)
          .describedAs(
              "Two gaps are present from 0 to 30MB-1 & 94MB to 100MB -1")
          .hasSize(2);
      assertThat(gaps.get(0).start())
          .describedAs("First gap should start from 0").isEqualTo(0);
      assertThat(gaps.get(0).end())
          .describedAs("First gap should end at 30MB - 1")
          .isEqualTo(30 * ONE_MB - 1);
      assertThat(gaps.get(1).start())
          .describedAs("Second gap should start from 94MB")
          .isEqualTo(94 * ONE_MB);
      assertThat(gaps.get(1).end())
          .describedAs("Second gap should end at 100MB - 1")
          .isEqualTo(100 * ONE_MB - 1);
    }
  }

  private AzureBlobFileSystem dataLocalityCacheCheck() throws IOException {
    Configuration config = new Configuration(this.getRawConfiguration());
    config.setBoolean(FS_AZURE_ENABLE_READAHEAD_V2, true);
    AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(
        config);
    assumeThat(fs.getAbfsStore()
        .getAbfsConfiguration()
        .isDataLocalityEnabled()).isTrue();
    return fs;
  }

  /*
   * Helper method to execute read and verify if priority header is added or not as expected
   */
  private void executePrefetchReadTest(TracingContext tracingContext,
      Configuration rawConfig,
      boolean shouldHaveHeader) throws Exception {
    try (AzureBlobFileSystem azureFs = (AzureBlobFileSystem) FileSystem.newInstance(
        rawConfig)) {
      AzureBlobFileSystemStore store = Mockito.spy(azureFs.getAbfsStore());

      AbfsClient abfsClient = Mockito.spy(store.getClient());
      Mockito.doReturn(abfsClient).when(store).getClient();

      List<AbfsHttpHeader> headersList = new ArrayList<>();

      doAnswer(invocation -> {
        AbfsRestOperation realOp
            = (AbfsRestOperation) invocation.callRealMethod();
        AbfsRestOperation spiedOp = spy(realOp);

        headersList.addAll(spiedOp.getRequestHeaders());

        doNothing().when(spiedOp).execute(any(TracingContext.class));
        return spiedOp;
      })
          .when(abfsClient)
          .getAbfsRestOperation(
              any(AbfsRestOperationType.class),
              anyString(),
              any(URL.class),
              anyList(),
              any(byte[].class),
              anyInt(),
              anyInt(),
              nullable(String.class)
          );

      abfsClient.read(
          "dummy-path", 0L, new byte[1], 0, 1,
          "etag", "leaseId", null, tracingContext);

      AbfsConfiguration abfsConfig = store.getAbfsConfiguration();
      if (shouldHaveHeader) {
        assertThat(headersList)
            .anySatisfy(header -> {
              assertThat(header.getName()).isEqualTo(
                  X_MS_REQUEST_PRIORITY);
              assertThat(header.getValue()).isEqualTo(
                  abfsConfig.getPrefetchRequestPriorityValue());
            });
      } else {
        assertThat(headersList)
            .noneSatisfy(header -> assertThat(header.getName()).isEqualTo(
                X_MS_REQUEST_PRIORITY));
      }
    }
  }

  private Path createTestFile(AzureBlobFileSystem fs, int fileSize) throws Exception {
    Path testPath = new Path("testFile");
    byte[] fileContent = getRandomBytesArray(fileSize);
    try (FSDataOutputStream oStream = fs.create(testPath)) {
      oStream.write(fileContent);
      oStream.flush();
    }
    return testPath;
  }

  private void readFile(AzureBlobFileSystem fs, Path testPath, int fileSize, ReadType readType) throws Exception {
    try (FSDataInputStream iStream = fs.open(testPath)) {
      if (readType == PREFETCH_READ || readType == MISSEDCACHE_READ) {
        assertThat(iStream.getWrappedStream()).isInstanceOf(AbfsPrefetchInputStream.class);
      } else if (readType == NORMAL_READ) {
        assertThat(iStream.getWrappedStream()).isInstanceOf(AbfsAdaptiveInputStream.class);
      } else if (readType == RANDOM_READ) {
        assertThat(iStream.getWrappedStream()).isInstanceOf(AbfsRandomInputStream.class);
      }
      int bytesRead = iStream.read(new byte[fileSize], 0,
          fileSize);
      assertThat(fileSize)
          .describedAs("Read size should match file size")
          .isEqualTo(bytesRead);
    }
  }

  /**
   * Verifies that the expected read type and read position are present in the
   * tracing header of the client read requests.
   *
   * <p>The client used by the filesystem stream is obtained through
   * {@link AzureBlobFileSystemStore#getClient()}. Data locality being enabled
   * does not imply that reads use the Blob client, because the DFS client also
   * supports layout-aware reads.</p>
   *
   * @param fs filesystem used to issue the reads
   * @param numOfReadCalls number of recent calls to validate
   * @param totalReadCalls total number of calls expected on the client
   * @param readType expected read type
   * @throws Exception if verification or header validation fails
   */
  private void assertReadTypeInClientRequestId(
      AzureBlobFileSystem fs,
      int numOfReadCalls,
      int totalReadCalls,
      ReadType readType) throws Exception {

    ArgumentCaptor<String> pathCaptor =
        ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> positionCaptor =
        ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<byte[]> bufferCaptor =
        ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> offsetCaptor =
        ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Integer> lengthCaptor =
        ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<String> eTagCaptor =
        ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> sasTokenCaptor =
        ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<ContextEncryptionAdapter> encryptionCaptor =
        ArgumentCaptor.forClass(ContextEncryptionAdapter.class);
    ArgumentCaptor<TracingContext> tracingContextCaptor =
        ArgumentCaptor.forClass(TracingContext.class);
    ArgumentCaptor<ReadTarget> readTargetCaptor =
        ArgumentCaptor.forClass(ReadTarget.class);

    /*
     * Verify the same default client used when the filesystem opens the stream.
     *
     * Do not select the Blob client merely because data locality is enabled.
     * DFS now supports layout-aware reads and can therefore remain the active
     * stream client.
     */
    AbfsClient readClient = fs.getAbfsStore().getClient();

    // Prefetch reads run on background threads. Wait for them to reach the
    // client before counting.
    verify(readClient, timeout(10_000).times(totalReadCalls)).read(
        pathCaptor.capture(),
        positionCaptor.capture(),
        bufferCaptor.capture(),
        offsetCaptor.capture(),
        lengthCaptor.capture(),
        eTagCaptor.capture(),
        sasTokenCaptor.capture(),
        encryptionCaptor.capture(),
        tracingContextCaptor.capture(),
        readTargetCaptor.capture());

    List<TracingContext> tracingContexts =
        tracingContextCaptor.getAllValues();

    if (readType == PREFETCH_READ) {
      /*
       * The first read can be a normal or missed-cache read. Validate the
       * remaining calls expected to carry the prefetch read type.
       *
       * Prefetch operations are asynchronous, so exact position ordering is
       * intentionally not asserted.
       */
      for (int i = tracingContexts.size() - (numOfReadCalls - 1);
          i < tracingContexts.size();
          i++) {
        verifyHeaderForReadTypeInTracingContextHeader(
            tracingContexts.get(i),
            readType,
            -1);
      }
    } else if (readType == DIRECT_READ) {
      int expectedReadPosition = ONE_MB / 3;

      for (int i = tracingContexts.size() - numOfReadCalls;
          i < tracingContexts.size();
          i++) {
        verifyHeaderForReadTypeInTracingContextHeader(
            tracingContexts.get(i),
            readType,
            expectedReadPosition);

        expectedReadPosition += ONE_MB;
      }
    } else {
      int expectedReadPosition = 0;

      for (int i = tracingContexts.size() - numOfReadCalls;
          i < tracingContexts.size();
          i++) {
        verifyHeaderForReadTypeInTracingContextHeader(
            tracingContexts.get(i),
            readType,
            expectedReadPosition);

        expectedReadPosition += ONE_MB;
      }
    }
  }

  private void verifyHeaderForReadTypeInTracingContextHeader(TracingContext tracingContext, ReadType readType, int expectedReadPos) {
    AbfsHttpOperation mockOp = Mockito.mock(AbfsHttpOperation.class);
    doReturn(EMPTY_STRING).when(mockOp).getTracingContextSuffix();
    tracingContext.constructHeader(mockOp, null, null);
    String[] idList = tracingContext.getHeader().split(COLON, SPLIT_NO_LIMIT);
    assertThat(idList).describedAs("Client Request Id should have all fields").hasSize(
        TracingHeaderVersion.getCurrentVersion().getFieldCount());
    if (expectedReadPos > 0) {
      assertThat(idList[POSITION_INDEX])
          .describedAs("Read Position should match")
          .isEqualTo(Integer.toString(expectedReadPos));
    }
    assertThat(idList[OPERATION_INDEX]).describedAs("Operation Type Should Be Read")
        .isEqualTo(FSOperationType.READ.toString());
    if (readType == PREFETCH_READ) {
      // For prefetch read, it might be missed cache as well.
      assertThat(idList[READTYPE_INDEX]).describedAs("Read type in tracing context header should match")
          .isIn(PREFETCH_READ.toString(), MISSEDCACHE_READ.toString());
    } else {
      assertThat(idList[READTYPE_INDEX]).describedAs("Read type in tracing context header should match")
              .isEqualTo(readType.toString());
    }
  }


  private void testReadAheads(AbfsInputStream inputStream,
      int readRequestSize,
      int readAheadRequestSize)
      throws Exception {
    if (readRequestSize > readAheadRequestSize) {
      readAheadRequestSize = readRequestSize;
    }

    byte[] firstReadBuffer = new byte[readRequestSize];
    byte[] secondReadBuffer = new byte[readAheadRequestSize];

    // get the expected bytes to compare
    byte[] expectedFirstReadAheadBufferContents = new byte[readRequestSize];
    byte[] expectedSecondReadAheadBufferContents = new byte[readAheadRequestSize];
    getExpectedBufferData(0, readRequestSize, expectedFirstReadAheadBufferContents);
    getExpectedBufferData(readRequestSize, readAheadRequestSize,
        expectedSecondReadAheadBufferContents);

    assertThat(inputStream.read(firstReadBuffer, 0, readRequestSize))
        .describedAs("Read should be of exact requested size")
        .isEqualTo(readRequestSize);

    assertTrue(
       Arrays.equals(firstReadBuffer,
            expectedFirstReadAheadBufferContents), "Data mismatch found in RAH1");

    assertThat(inputStream.read(secondReadBuffer, 0, readAheadRequestSize))
        .describedAs("Read should be of exact requested size")
        .isEqualTo(readAheadRequestSize);

    assertTrue(
       Arrays.equals(secondReadBuffer,
            expectedSecondReadAheadBufferContents), "Data mismatch found in RAH2");
  }

  public AbfsInputStream testReadAheadConfigs(int readRequestSize,
      int readAheadQueueDepth,
      boolean alwaysReadBufferSizeEnabled,
      int readAheadBlockSize) throws Exception {
    Configuration
        config = new Configuration(
        this.getRawConfiguration());
    config.set("fs.azure.read.request.size", Integer.toString(readRequestSize));
    config.set("fs.azure.readaheadqueue.depth",
        Integer.toString(readAheadQueueDepth));
    config.set("fs.azure.read.alwaysReadBufferSize",
        Boolean.toString(alwaysReadBufferSizeEnabled));
    config.set("fs.azure.read.readahead.blocksize",
        Integer.toString(readAheadBlockSize));
    if (readRequestSize > readAheadBlockSize) {
      readAheadBlockSize = readRequestSize;
    }

    Path testPath = path("/testReadAheadConfigs");
    final AzureBlobFileSystem fs = createTestFile(testPath,
        ALWAYS_READ_BUFFER_SIZE_TEST_FILE_SIZE, config);
    byte[] byteBuffer = new byte[ONE_MB];
    AbfsInputStream inputStream = this.getAbfsStore(fs)
        .openFileForRead(testPath, null, getTestTracingContext(fs, false));

    assertThat(inputStream.getBufferSize())
        .describedAs("Unexpected AbfsInputStream buffer size")
        .isEqualTo(readRequestSize);

    assertThat(inputStream.getReadAheadQueueDepth())
        .describedAs("Unexpected ReadAhead queue depth")
        .isEqualTo(readAheadQueueDepth);

    assertThat(inputStream.shouldAlwaysReadBufferSize())
        .describedAs("Unexpected AlwaysReadBufferSize settings")
        .isEqualTo(alwaysReadBufferSizeEnabled);

    assertThat(getBufferManager().getReadAheadBlockSize())
        .describedAs("Unexpected readAhead block size")
        .isEqualTo(readAheadBlockSize);

    return inputStream;
  }

  private void getExpectedBufferData(int offset, int length, byte[] b) {
    boolean startFillingIn = false;
    int indexIntoBuffer = 0;
    char character = 'a';

    for (int i = 0; i < (offset + length); i++) {
      if (i == offset) {
        startFillingIn = true;
      }

      if ((startFillingIn) && (indexIntoBuffer < length)) {
        b[indexIntoBuffer] = (byte) character;
        indexIntoBuffer++;
      }

      character = (character == 'z') ? 'a' : (char) ((int) character + 1);
    }
  }

  private AzureBlobFileSystem createTestFile(Path testFilePath, long testFileSize,
      Configuration config) throws Exception {
    AzureBlobFileSystem fs;

    if (config == null) {
      fs = this.getFileSystem();
    } else {
      final AzureBlobFileSystem currentFs = getFileSystem();
      fs = (AzureBlobFileSystem) FileSystem.newInstance(currentFs.getUri(),
          config);
    }

    if (fs.exists(testFilePath)) {
      FileStatus status = fs.getFileStatus(testFilePath);
      if (status.getLen() >= testFileSize) {
        return fs;
      }
    }

    byte[] buffer = new byte[EIGHT_MB];
    char character = 'a';
    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = (byte) character;
      character = (character == 'z') ? 'a' : (char) ((int) character + 1);
    }

    try (FSDataOutputStream outputStream = fs.create(testFilePath)) {
      int bytesWritten = 0;
      while (bytesWritten < testFileSize) {
        outputStream.write(buffer);
        bytesWritten += buffer.length;
      }
    }

    assertThat(fs.getFileStatus(testFilePath).getLen())
        .describedAs("File not created of expected size")
        .isEqualTo(testFileSize);

    return fs;
  }

  private void resetReadBufferManager(int bufferSize, int threshold)
      throws IOException {
    getBufferManager()
        .testResetReadBufferManager(bufferSize, threshold);
    // Trigger GC as aggressive recreation of ReadBufferManager buffers
    // by successive tests can lead to OOM based on the dev VM/machine capacity.
    System.gc();
  }

  private ReadBufferManager getBufferManager() throws IOException {
    if (getConfiguration().isReadAheadV2Enabled()) {
      ReadBufferManagerV2.setReadBufferManagerConfigs(
          getConfiguration().getReadAheadBlockSize(), getConfiguration());
      return ReadBufferManagerV2.getBufferManager(getFileSystem().getAbfsStore().getClient().getAbfsCounters());
    }
    return ReadBufferManagerV1.getBufferManager();
  }

  /**
   * Verifies that an unexpired Direct Read data handle is used for the portion
   * of the read covered by its {@link ReadTarget}.
   *
   * <p>The data handle is valid for the range [0, 127], so the first client
   * read must use the Direct Read target and must be limited to 128 bytes.
   * Once that range is exhausted, the stream should continue satisfying the
   * caller's request using the normal read target.</p>
   *
   * <p>The {@link AbfsInputStream} read itself is not limited to the Direct Read
   * target's maximum length. The complete caller-requested buffer can therefore
   * be filled using multiple underlying client reads.</p>
   *
   * @throws Exception if stream creation or reading fails
   */
  @Test
  public void testUnexpiredDataHandleIsUsed() throws Exception {
    HandleReadTestContext context = createHandleReadTestContext(
        0,
        127,
        "unexpired-handle",
        System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5));

    try (AbfsInputStream stream = context.stream()) {
      byte[] buffer = new byte[512];

      // Request more data than the Direct Read target covers. This verifies
      // that ABFS uses the handle for its valid range and then continues
      // reading through the normal read target.
      int bytesRead = stream.read(0, buffer, 0, buffer.length);

      ArgumentCaptor<Long> positionCaptor =
          ArgumentCaptor.forClass(Long.class);
      ArgumentCaptor<Integer> lengthCaptor =
          ArgumentCaptor.forClass(Integer.class);
      ArgumentCaptor<ReadTarget> targetCaptor =
          ArgumentCaptor.forClass(ReadTarget.class);

      // Two underlying reads are expected:
      // 1. Direct Read for bytes [0, 127].
      // 2. Normal read starting at position 128.
      verify(context.client(), times(2)).read(
          nullable(String.class),
          positionCaptor.capture(),
          nullable(byte[].class),
          anyInt(),
          lengthCaptor.capture(),
          nullable(String.class),
          nullable(String.class),
          nullable(ContextEncryptionAdapter.class),
          nullable(TracingContext.class),
          targetCaptor.capture());

      List<Long> positions = positionCaptor.getAllValues();
      List<Integer> lengths = lengthCaptor.getAllValues();
      List<ReadTarget> targets = targetCaptor.getAllValues();

      // The Direct Read target limits only the underlying handle-backed read.
      // It must not limit the total number of bytes returned to the caller.
      assertThat(bytesRead).isEqualTo(buffer.length);

      // Verify that the first client read starts at position 0 and is bounded
      // by the 128-byte Direct Read target.
      assertThat(positions.get(0)).isEqualTo(0L);
      assertThat(lengths.get(0)).isEqualTo(128);

      ReadTarget directReadTarget = targets.get(0);

      // The unexpired data handle must be propagated through the first read.
      assertThat(directReadTarget).isNotNull();
      assertThat(directReadTarget.hasHandle()).isTrue();
      assertThat(directReadTarget.handle()).isEqualTo("unexpired-handle");
      assertThat(directReadTarget.maxLength()).isEqualTo(128);

      // After consuming the Direct Read range, the next client read must start
      // immediately after it.
      assertThat(positions.get(1)).isEqualTo(128L);

      ReadTarget normalReadTarget = targets.get(1);

      // The remaining data must be fetched through the normal read target,
      // which must not carry a Direct Read data handle.
      assertThat(normalReadTarget).isNotNull();
      assertThat(normalReadTarget.hasHandle()).isFalse();

      // Verify that combining the Direct Read and normal read paths still
      // returns the expected data to the caller.
      assertThat(Arrays.copyOf(buffer, bytesRead))
          .containsExactly(Arrays.copyOf(context.data(), bytesRead));
    }
  }

  /**
   * Verifies that a handle with an unknown expiry ({@code expiresAt == 0}) is
   * treated as non-expiring and is used.
   *
   * <p>Asserts that a 64-byte read at position 0 carries
   * {@code "unknown-expiry-handle"} in its {@link ReadTarget}.
   *
   * @throws Exception on any failure during setup, mocking or I/O
   */
  @Test
  public void testDataHandleWithUnknownExpiryIsUsed() throws Exception {
    // 0 means the server did not report an expiry.
    HandleReadTestContext context = createHandleReadTestContext(
        0, 511, "unknown-expiry-handle", 0L);

    try (AbfsInputStream stream = context.stream()) {
      byte[] buffer = new byte[64];
      assertEquals(buffer.length, stream.read(0, buffer, 0, buffer.length));

      ReadTarget target = captureReadTargetAtPosition(context.client(), 0);
      assertThat(target.hasHandle()).isTrue();
      assertThat(target.handle()).isEqualTo("unknown-expiry-handle");
    }
  }

  /**
   * Verifies that a handle with an extremely distant expiry
   * ({@link Long#MAX_VALUE}) is used, i.e. the expiry check does not overflow
   * or otherwise misclassify it as expired.
   *
   * <p>Asserts that a 64-byte read at position 0 carries
   * {@code "far-future-handle"} in its {@link ReadTarget}.
   *
   * @throws Exception on any failure during setup, mocking or I/O
   */
  @Test
  public void testDataHandleWithFarFutureExpiryIsUsed() throws Exception {
    HandleReadTestContext context = createHandleReadTestContext(
        0, 511, "far-future-handle", Long.MAX_VALUE);

    try (AbfsInputStream stream = context.stream()) {
      byte[] buffer = new byte[64];
      assertEquals(buffer.length, stream.read(0, buffer, 0, buffer.length));

      ReadTarget target = captureReadTargetAtPosition(context.client(), 0);
      assertThat(target.hasHandle()).isTrue();
      assertThat(target.handle()).isEqualTo("far-future-handle");
    }
  }

  /**
   * Verifies that a handle-backed request is clamped to the handle's authorized
   * range when the application asks for more bytes than that range holds.
   *
   * <p>Setup: the file is 1024 bytes; range {@code 256-767} (512 bytes) carries
   * {@code "range-handle"}. Ranges {@code 0-255} and {@code 768-1023} use the
   * normal endpoint without a handle.
   *
   * <p>The application reads 1000 bytes from position 256, which spans beyond
   * the handle's range. Asserts that:
   * <ul>
   *   <li>the stream-level read returns the 768 bytes remaining in the file and
   *       the data matches the source;</li>
   *   <li>the backend request carrying the handle starts at 256, has length 512
   *       and {@code maxLength == 512}, i.e. it stops at byte 767;</li>
   *   <li>that request goes to the direct-read endpoint with the handle set.</li>
   * </ul>
   *
   * <p>The exact number of internal reads is intentionally not asserted, as
   * {@link AbfsInputStream} may issue additional reads for buffering or read
   * optimizations.
   *
   * @throws Exception on any failure during setup, mocking or I/O
   */
  @Test
  public void testReadTargetMaxLengthForFullRange()
      throws Exception {
    // Handle authorizes bytes 256-767 only.
    HandleReadTestContext context = createHandleReadTestContext(
        256,
        767,
        "range-handle",
        System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5));

    try (AbfsInputStream stream = context.stream()) {
      byte[] buffer = new byte[1000];
      int bytesRead = stream.read(256, buffer, 0, buffer.length);

      /*
       * The file size is 1024 bytes. Starting at position 256 means
       * 768 bytes remain in the file.
       *
       * The application-level read can span multiple layout ranges.
       */
      assertEquals(768, bytesRead);
      assertThat(Arrays.copyOf(buffer, bytesRead))
          .describedAs("Data returned by the complete stream read")
          .containsExactly(
              Arrays.copyOfRange(
                  context.data(),
                  256,
                  1024));

      ArgumentCaptor<Long> positionCaptor =
          ArgumentCaptor.forClass(Long.class);
      ArgumentCaptor<Integer> lengthCaptor =
          ArgumentCaptor.forClass(Integer.class);
      ArgumentCaptor<ReadTarget> targetCaptor =
          ArgumentCaptor.forClass(ReadTarget.class);

      /*
       * Do not assert the exact number of internal reads.
       *
       * AbfsInputStream may perform additional internal reads depending on
       * buffering/read optimization. What matters here is the backend request
       * carrying the Direct Read handle.
       */
      verify(context.client(), atLeastOnce()).read(
          nullable(String.class),
          positionCaptor.capture(),
          nullable(byte[].class),
          anyInt(),
          lengthCaptor.capture(),
          nullable(String.class),
          nullable(String.class),
          nullable(ContextEncryptionAdapter.class),
          nullable(TracingContext.class),
          targetCaptor.capture());

      List<Long> positions = positionCaptor.getAllValues();
      List<Integer> lengths = lengthCaptor.getAllValues();
      List<ReadTarget> targets = targetCaptor.getAllValues();

      // Locate the backend call(s) that used the handle and validate them.
      boolean directReadCallFound = false;

      for (int i = 0; i < targets.size(); i++) {
        ReadTarget target = targets.get(i);

        if (target != null
            && "range-handle".equals(target.handle())) {
          directReadCallFound = true;

          /*
           * The handle covers bytes 256-767.
           *
           * Therefore:
           *
           * 767 - 256 + 1 = 512 bytes
           *
           * Even though the application requested 1000 bytes, the
           * handle-backed backend request must stop at byte 767.
           */
          assertThat(positions.get(i))
              .describedAs(
                  "Direct Read should start at the handle range start")
              .isEqualTo(256L);
          assertThat(lengths.get(i))
              .describedAs(
                  "Direct Read request should be clamped to the handle range")
              .isEqualTo(512);
          assertThat(target.maxLength())
              .describedAs(
                  "ReadTarget maxLength should match the authorized range")
              .isEqualTo(512);
          assertThat(target.endpoint())
              .describedAs("Direct Read endpoint")
              .isEqualTo("https://direct-read.test/");
          assertThat(target.hasHandle())
              .describedAs(
                  "Direct Read target should contain the data handle")
              .isTrue();
          assertThat(target.handle())
              .describedAs("Direct Read data handle")
              .isEqualTo("range-handle");
        }
      }

      // Guard against the loop passing vacuously.
      assertThat(directReadCallFound)
          .describedAs(
              "Expected a backend read using the Direct Read handle")
          .isTrue();
    }
  }

  /**
   * Verifies that a handle-backed request covers the handle's authorized range
   * even when the application read begins in the middle of that range.
   *
   * <p>Setup: range {@code 256-767} carries {@code "middle-range-handle"}.
   * The application reads 300 bytes from position 700 (inside the range, and
   * ending at 999, past its end).
   *
   * <p>Asserts that:
   * <ul>
   *   <li>the application read returns all 300 bytes;</li>
   *   <li>the backend request using the handle starts at the range start (256),
   *       has length 512, and {@code maxLength == 512};</li>
   *   <li>the request length never exceeds {@code ReadTarget.maxLength()};</li>
   *   <li>the target has the handle and the direct-read endpoint.</li>
   * </ul>
   *
   * @throws Exception on any failure during setup, mocking or I/O
   */
  @Test
  public void testReadTargetMaxLengthFromMiddleOfRange()
      throws Exception {
    HandleReadTestContext context = createHandleReadTestContext(
        256,
        767,
        "middle-range-handle",
        System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5));

    try (AbfsInputStream stream = context.stream()) {
      // Starts mid-range (700) and runs past the range end (767).
      byte[] buffer = new byte[300];
      int bytesRead = stream.read(700, buffer, 0, buffer.length);

      assertEquals(300, bytesRead);

      ArgumentCaptor<Long> positionCaptor =
          ArgumentCaptor.forClass(Long.class);
      ArgumentCaptor<Integer> lengthCaptor =
          ArgumentCaptor.forClass(Integer.class);
      ArgumentCaptor<ReadTarget> targetCaptor =
          ArgumentCaptor.forClass(ReadTarget.class);

      verify(context.client(), atLeastOnce()).read(
          nullable(String.class),
          positionCaptor.capture(),
          nullable(byte[].class),
          anyInt(),
          lengthCaptor.capture(),
          nullable(String.class),
          nullable(String.class),
          nullable(ContextEncryptionAdapter.class),
          nullable(TracingContext.class),
          targetCaptor.capture());

      List<Long> positions = positionCaptor.getAllValues();
      List<Integer> lengths = lengthCaptor.getAllValues();
      List<ReadTarget> targets = targetCaptor.getAllValues();

      boolean handleReadFound = false;

      for (int i = 0; i < targets.size(); i++) {
        ReadTarget target = targets.get(i);

        if (target != null
            && "middle-range-handle".equals(target.handle())) {
          handleReadFound = true;

          // Handle-backed read is aligned to the full authorized range.
          assertThat(positions.get(i))
              .describedAs("Handle-backed read start position")
              .isEqualTo(256L);
          assertThat(lengths.get(i))
              .describedAs("Handle-backed read length")
              .isEqualTo(512);
          assertThat(target.maxLength())
              .describedAs("Maximum length permitted by ReadTarget")
              .isEqualTo(512);
          // The read must never ask the backend for more than it authorized.
          assertThat(lengths.get(i))
              .describedAs(
                  "Handle-backed read must not exceed ReadTarget maxLength")
              .isLessThanOrEqualTo(target.maxLength());
          assertThat(target.hasHandle())
              .describedAs(
                  "Expected Direct Read target to contain the handle")
              .isTrue();
          assertThat(target.endpoint())
              .describedAs("Direct Read endpoint")
              .isEqualTo("https://direct-read.test/");
        }
      }

      assertThat(handleReadFound)
          .describedAs("Expected a backend read using middle-range-handle")
          .isTrue();
    }
  }

  /**
   * Verifies that the read task honors {@link ReadTarget#maxLength()} when the
   * handle's range is much smaller than the requested read.
   *
   * <p>Setup: range {@code 0-67} (68 bytes) carries {@code "short-range-handle"};
   * the remainder of the file is served by the normal endpoint. The application
   * reads 300 bytes from position 0.
   *
   * <p>Asserts that:
   * <ul>
   *   <li>the target at position 0 has {@code maxLength == 68};</li>
   *   <li>the first backend read is clamped to 68 bytes;</li>
   *   <li>exactly two backend reads occur (the clamped handle read, then one
   *       read for the rest of the requested data);</li>
   *   <li>the application still receives all 300 bytes with correct contents.</li>
   * </ul>
   *
   * @throws Exception on any failure during setup, mocking or I/O
   */
  @Test
  public void testReadTaskHonorsReadTargetMaxLength() throws Exception {
    // Very short handle range: 0-67 => 68 bytes.
    HandleReadTestContext context = createHandleReadTestContext(
        0,
        67,
        "short-range-handle",
        System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5));

    try (AbfsInputStream stream = context.stream()) {
      byte[] buffer = new byte[300];

      int bytesRead = stream.read(0, buffer, 0, buffer.length);

      ReadTarget target =
          captureReadTargetAtPosition(context.client(), 0);

      ArgumentCaptor<Integer> lengthCaptor =
          ArgumentCaptor.forClass(Integer.class);

      // Two reads: one bounded by the handle range, one for the remainder.
      verify(context.client(), times(2)).read(
          nullable(String.class),
          anyLong(),
          nullable(byte[].class),
          anyInt(),
          lengthCaptor.capture(),
          nullable(String.class),
          nullable(String.class),
          nullable(ContextEncryptionAdapter.class),
          nullable(TracingContext.class),
          nullable(ReadTarget.class));

      // First request must be clamped to the authorized 68 bytes.
      assertThat(target.maxLength()).isEqualTo(68);
      assertThat(lengthCaptor.getAllValues().get(0)).isEqualTo(68);

      // Clamping must not cause short reads or corrupt data at the caller.
      assertThat(bytesRead).isEqualTo(buffer.length);
      assertThat(Arrays.copyOf(buffer, bytesRead))
          .containsExactly(Arrays.copyOf(context.data(), bytesRead));
    }
  }

  /**
   * Creates a test context with a mocked ABFS client and cached blob layout
   * for exercising Direct Read handle behavior.
   *
   * <p>The context uses a 1024-byte file and a single read buffer of the same
   * size. The cached layout covers the entire file with up to three ranges:
   * <ul>
   *   <li>{@code [0, rangeStart - 1]}: normal endpoint, no handle (only if
   *       {@code rangeStart > 0});</li>
   *   <li>{@code [rangeStart, rangeEnd]}: direct-read endpoint carrying
   *       {@code handle} and {@code expiresAt};</li>
   *   <li>{@code [rangeEnd + 1, fileSize - 1]}: normal endpoint, no handle
   *       (only if {@code rangeEnd < fileSize - 1}).</li>
   * </ul>
   *
   * <p>The mock client serves reads from an in-memory copy of the file, so the
   * returned {@link HandleReadTestContext#data()} can be used to verify the
   * bytes returned by the stream.
   *
   * @param rangeStart start of the range carrying the Direct Read handle
   * @param rangeEnd end of the range carrying the Direct Read handle
   * @param handle Direct Read data handle to associate with the range
   * @param expiresAt expiry timestamp for the data handle
   * @return test context containing the stream, mock client, and test data
   * @throws Exception if context setup fails
   */
  private HandleReadTestContext createHandleReadTestContext(
      final long rangeStart, final long rangeEnd, final String handle, final long expiresAt)
      throws Exception {

    // Small file, single buffer: the whole file fits in one read buffer.
    int fileSize = 1024;
    int bufferSize = 1024;

    AbfsClient mockClient = getMockClientForLayoutRead(bufferSize);
    when(mockClient.supportsLayout()).thenReturn(true);

    // Deterministic content (i % 256) so returned bytes can be compared exactly.
    byte[] testData = generateTestData(fileSize);

    /*
     * Both read overloads serve bytes from testData:
     * - the layout-aware read, for reads that carry a ReadTarget
     * - the normal read, used when a layout refresh fails and the stream
     *   falls back to reading without a target
     */
    when(mockClient.read(
        nullable(String.class), anyLong(), nullable(byte[].class), anyInt(), anyInt(),
        nullable(String.class), nullable(String.class), nullable(ContextEncryptionAdapter.class),
        nullable(TracingContext.class), nullable(ReadTarget.class)))
        .thenAnswer(serveFrom(testData));

    when(mockClient.read(
        nullable(String.class), anyLong(), nullable(byte[].class), anyInt(), anyInt(),
        nullable(String.class), nullable(String.class), nullable(ContextEncryptionAdapter.class),
        nullable(TracingContext.class)))
        .thenAnswer(serveFrom(testData));

    /*
     * A cached handle that is expired or inside the refresh window triggers a
     * layout fetch. Return a fresh full-file layout carrying REFRESHED_HANDLE.
     * Handles outside the refresh window never reach this stub.
     */
    stubLayoutFetch(mockClient, singleRangeLayout(fileSize, REFRESHED_HANDLE,
        System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5)));

    AbfsInputStream stream = getAbfsInputStreamForLayout(mockClient, bufferSize, fileSize, false);

    BlobLayoutResponse response = new BlobLayoutResponse();

    /*
     * Endpoint used by the Direct Read range.
     */
    response.addEndpoint(new BlobLayoutResponse.Endpoint(0, DIRECT_READ_ENDPOINT));

    /*
     * Endpoint used by surrounding ranges.
     *
     * Having complete layout coverage is important because getBlobRanges()
     * checks the entire application-requested range before findReadTarget()
     * selects the first range and applies maxLength.
     */
    response.addEndpoint(new BlobLayoutResponse.Endpoint(1, "https://normal-read.test/"));

    /*
     * Cover anything before the Direct Read range.
     */
    if (rangeStart > 0) {
      response.addRange(new BlobLayoutResponse.Range(0, rangeStart - 1, 1, null, 0L));
    }

    /*
     * The range under test carries the Direct Read handle.
     */
    response.addRange(new BlobLayoutResponse.Range(rangeStart, rangeEnd, 0, handle, expiresAt));

    /*
     * Cover anything after the Direct Read range.
     *
     * This is required for maxLength tests where the application asks for
     * more bytes than are available in the Direct Read range.
     */
    if (rangeEnd < fileSize - 1) {
      response.addRange(new BlobLayoutResponse.Range(rangeEnd + 1, fileSize - 1, 1, null, 0L));
    }

    // Pre-populate the layout cache under the stream's layout key so the
    // stream uses this layout instead of fetching one from the (mock) service.
    BlobLayoutCache cache =
        BlobLayoutCache.getInstance(1, DEFAULT_FS_AZURE_BLOB_LAYOUT_CACHE_MAX_COUNT);
    cache.putBlobLayout(stream.getLayoutCacheKey(), response, fileSize);

    return new HandleReadTestContext(stream, mockClient, testData);
  }

  /**
   * Returns the {@link ReadTarget} of the first {@code client.read(...)} call
   * made at the given file position.
   *
   * <p>Verifies that the client was read from at least once, captures the
   * position and target arguments of every call, and picks the first call whose
   * position matches {@code expectedPosition}.
   *
   * @param client           mocked client that served the reads
   * @param expectedPosition file position of the read to look up
   * @return the {@link ReadTarget} passed with the matching call (may be
   *         {@code null} if the call carried no target)
   * @throws Exception if no read was issued at {@code expectedPosition}
   *                   (via {@code fail}) or if verification fails
   */
  private ReadTarget captureReadTargetAtPosition(final AbfsClient client,
      final long expectedPosition) throws Exception {

    ArgumentCaptor<Long> positionCaptor = ArgumentCaptor.forClass(Long.class);

    ArgumentCaptor<ReadTarget> targetCaptor = ArgumentCaptor.forClass(ReadTarget.class);
    verify(client, atLeastOnce()).read(
        nullable(String.class),
        positionCaptor.capture(),
        nullable(byte[].class),
        anyInt(),
        anyInt(),
        nullable(String.class),
        nullable(String.class),
        nullable(ContextEncryptionAdapter.class),
        nullable(TracingContext.class),
        targetCaptor.capture());
    List<Long> positions = positionCaptor.getAllValues();
    List<ReadTarget> targets = targetCaptor.getAllValues();

    // Position and target lists are index-aligned (one entry per invocation).
    for (int i = 0; i < positions.size(); i++) {
      if (positions.get(i) == expectedPosition) {
        return targets.get(i);
      }
    }

    fail("No client read found at position " + expectedPosition);
    return null;
  }

  /**
   * Bundle of objects produced by
   * {@link #createHandleReadTestContext(long, long, String, long)}.
   *
   * @param stream the {@link AbfsInputStream} under test, backed by the mock client
   *               and a pre-populated {@link BlobLayoutCache}
   * @param client the mocked {@link AbfsClient} serving reads and recording calls
   * @param data   the deterministic file contents the mock client serves,
   *               used to verify data returned by the stream
   */
  private record HandleReadTestContext(AbfsInputStream stream, AbfsClient client, byte[] data) {
  }

  /**
   * Verifies that a Direct Read stream receives a data handle even when a
   * stream with Direct Read disabled has already cached the same file's
   * layout.
   *
   * <p>Both streams share the JVM-wide {@link BlobLayoutCache} and the same
   * eTag. The normal client's layout response carries no handle, as the
   * service does when {@code x-ms-include: datahandle} is not sent. The Direct
   * Read client's response carries a handle.</p>
   *
   * <p>With the current eTag-only cache key, the Direct Read stream gets a
   * cache hit on the handle-less layout, never fetches its own layout, and
   * reads with {@code handle == null}. This test fails until layouts fetched
   * with and without data handles are cached separately.</p>
   *
   * @throws Exception on setup, mocking or I/O failure
   */
  @Test
  public void testDirectReadStreamNotServedHandlelessCachedLayout()
      throws Exception {
    final int fileSize = ONE_KB;
    final String eTag = "shared-etag-" + UUID.randomUUID();
    final String handle = "direct-read-handle";
    byte[] data = generateTestData(fileSize);

    AbfsClient normalClient = createLayoutFetchingClient(false, data, handle);
    AbfsClient directClient = createLayoutFetchingClient(true, data, handle);

    // 1. The normal stream reads first and populates the shared cache.
    try (AbfsInputStream normalStream =
             createSharedETagStream(normalClient, fileSize, eTag)) {
      normalStream.read(0, new byte[fileSize], 0, fileSize);

      // The normal client fetched the layout (without data handles).
      verify(normalClient, times(1)).getBlobLayout(
          nullable(String.class), anyLong(), anyLong(),
          nullable(String.class), nullable(String.class),
          nullable(TracingContext.class));

      // 2. The Direct Read stream reads the same file while the entry is live.
      try (AbfsInputStream directStream =
               createSharedETagStream(directClient, fileSize, eTag)) {
        byte[] buffer = new byte[fileSize];
        assertThat(directStream.read(0, buffer, 0, fileSize))
            .isEqualTo(fileSize);
        assertThat(buffer).containsExactly(data);

        long directLayoutCalls = Mockito.mockingDetails(directClient)
            .getInvocations().stream()
            .filter(i -> "getBlobLayout".equals(i.getMethod().getName()))
            .count();

        ReadTarget target = captureReadTargetAtPosition(directClient, 0);

        assertThat(target)
            .as("Direct Read stream should have a read target")
            .isNotNull();
        assertThat(target.hasHandle())
            .as("Direct Read stream must not reuse a layout cached without "
                    + "data handles. getBlobLayout calls by directClient: %s",
                directLayoutCalls)
            .isTrue();
        assertThat(target.handle()).isEqualTo(handle);
      }
    }
  }

  /**
   * Verifies that a stream with Direct Read disabled never redeems a data
   * handle, even when a Direct Read stream has already cached a layout with
   * handles for the same file.
   *
   * <p>With the current code, the normal stream gets a cache hit on the layout
   * with handles, and {@code findReadTarget()} returns the handle without
   * checking whether Direct Read is enabled. This test fails until that is
   * fixed.</p>
   *
   * @throws Exception on setup, mocking or I/O failure
   */
  @Test
  public void testNormalStreamDoesNotRedeemHandleCachedByDirectReadStream()
      throws Exception {
    final int fileSize = ONE_KB;
    final String eTag = "shared-etag-" + UUID.randomUUID();
    final String handle = "direct-read-handle";
    byte[] data = generateTestData(fileSize);

    AbfsClient directClient = createLayoutFetchingClient(true, data, handle);
    AbfsClient normalClient = createLayoutFetchingClient(false, data, handle);

    // 1. The Direct Read stream reads first and caches a layout with handles.
    try (AbfsInputStream directStream =
             createSharedETagStream(directClient, fileSize, eTag)) {
      directStream.read(0, new byte[fileSize], 0, fileSize);

      // Sanity check: the Direct Read path itself works.
      assertThat(captureReadTargetAtPosition(directClient, 0).handle())
          .as("Direct Read stream should use its own handle")
          .isEqualTo(handle);

      // 2. The normal stream (Direct Read disabled) reads the same file.
      try (AbfsInputStream normalStream =
               createSharedETagStream(normalClient, fileSize, eTag)) {
        byte[] buffer = new byte[fileSize];
        assertThat(normalStream.read(0, buffer, 0, fileSize))
            .isEqualTo(fileSize);
        assertThat(buffer).containsExactly(data);

        ReadTarget target = captureReadTargetAtPosition(normalClient, 0);

        assertThat(target)
            .as("Normal stream should still have a Data Locality target")
            .isNotNull();
        assertThat(target.hasHandle())
            .as("A client with Direct Read disabled must never send "
                + "x-ms-data-handle")
            .isFalse();
        assertThat(target.handle()).isNull();
      }
    }
  }

  /**
   * Creates a mock client that serves layout fetches and data reads through
   * the real {@link AbfsInputStream} fetch path.
   *
   * <p>Models {@code AbfsDfsClient.getBlobLayout()}: the returned layout
   * carries a data handle only when Direct Read is enabled for this client,
   * because only then is {@code x-ms-include: datahandle} sent.</p>
   *
   * @param directReadEnabled whether Direct Read is enabled for this client
   * @param data the in-memory file contents to serve
   * @param handle the data handle returned when Direct Read is enabled
   * @return the configured mock client
   * @throws Exception if client setup fails
   */
  private AbfsClient createLayoutFetchingClient(boolean directReadEnabled,
      byte[] data, String handle) throws Exception {
    Configuration conf = new Configuration();
    conf.set(FS_AZURE_READ_AHEAD_BLOCK_SIZE, String.valueOf(data.length));
    conf.set(AZURE_READ_BUFFER_SIZE, String.valueOf(data.length));
    conf.setBoolean(FS_AZURE_ENABLE_DATA_LOCALITY, true);
    conf.setBoolean(FS_AZURE_DIRECT_READ_ENABLED, directReadEnabled);
    AbfsConfiguration abfsConfig = new AbfsConfiguration(conf, getAccountName());

    AbfsClient client = mock(AbfsBlobClient.class);
    AbfsCounters counters = Mockito.spy(new AbfsCountersImpl(new URI("abcd")));
    doReturn(counters).when(client).getAbfsCounters();
    when(client.getAbfsConfiguration()).thenReturn(abfsConfig);
    when(client.getAbfsPerfTracker()).thenReturn(
        new AbfsPerfTracker("test", getAccountName(), getConfiguration()));
    when(client.supportsLayout()).thenReturn(true);

    // Layout response: a handle only if this client requested data handles.
    BlobLayoutResponse layout = new BlobLayoutResponse();
    layout.addEndpoint(
        new BlobLayoutResponse.Endpoint(0, "https://direct-read.test/"));
    layout.addRange(new BlobLayoutResponse.Range(
        0,
        data.length - 1,
        0,
        directReadEnabled ? handle : null,
        directReadEnabled
            ? System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5)
            : 0L));

    LayoutResponseParser parser = mock(LayoutResponseParser.class);
    when(parser.parse(any(InputStream.class))).thenReturn(layout);
    when(client.getLayoutParser()).thenReturn(parser);

    when(client.getBlobLayout(
        nullable(String.class), anyLong(), anyLong(),
        nullable(String.class), nullable(String.class),
        nullable(TracingContext.class)))
        .thenAnswer(invocation -> {
          AbfsRestOperation op = mock(AbfsRestOperation.class);
          AbfsHttpOperation result = mock(AbfsHttpOperation.class);
          when(op.getResult()).thenReturn(result);
          // Must support reset(); the body itself is ignored by the parser mock.
          when(result.getListResultStream())
              .thenReturn(new ByteArrayInputStream(new byte[] {'{'}));
          return op;
        });

    // Data reads: serve bytes from the in-memory file.
    when(client.read(
        nullable(String.class), anyLong(), nullable(byte[].class),
        anyInt(), anyInt(),
        nullable(String.class), nullable(String.class),
        nullable(ContextEncryptionAdapter.class),
        nullable(TracingContext.class), nullable(ReadTarget.class)))
        .thenAnswer(invocation -> {
          long position = invocation.getArgument(1);
          byte[] destination = invocation.getArgument(2);
          int destinationOffset = invocation.getArgument(3);
          int length = invocation.getArgument(4);
          int bytesToCopy = (int) Math.min(length, data.length - position);
          System.arraycopy(data, (int) position, destination,
              destinationOffset, bytesToCopy);

          AbfsRestOperation op = mock(AbfsRestOperation.class);
          AbfsHttpOperation result = mock(AbfsHttpOperation.class);
          when(op.getResult()).thenReturn(result);
          when(result.getBytesReceived()).thenReturn((long) bytesToCopy);
          when(op.getSasToken()).thenReturn(null);
          return op;
        });

    return client;
  }

  /**
   * Creates a stream with an explicit eTag so that two streams share one
   * {@link BlobLayoutCache} entry. {@link #getAbfsInputStreamForLayout} uses
   * a random eTag, so it cannot be used for this scenario.
   *
   * @param client the client backing the stream
   * @param fileSize the file size exposed by the stream
   * @param eTag the shared eTag
   * @return the configured stream
   */
  private AbfsInputStream createSharedETagStream(AbfsClient client,
      int fileSize, String eTag) {
    AbfsInputStreamContext context = new AbfsInputStreamContext(-1)
        .withReadBufferSize(fileSize)
        .withReadAheadQueueDepth(0)
        .withReadAheadBlockSize(fileSize)
        .isReadAheadV2Enabled(false)
        .withOptimizeFooterRead(true)
        .withFooterReadBufferSize(512 * ONE_KB);

    return new AbfsAdaptiveInputStream(
        client,
        null,
        "/file",
        fileSize,
        context,
        eTag,
        new TracingContext(
            "test-correlation-id",
            "test-fs-id",
            FSOperationType.READ,
            true,
            TracingHeaderFormat.ALL_ID_FORMAT,
            null));
  }

  private static final String DIRECT_READ_ENDPOINT = "https://direct-read.test/";
  private static final String REFRESHED_HANDLE = "refreshed-handle";

  /** Answer that serves a client read from an in-memory file. */
  private static Answer<AbfsRestOperation> serveFrom(final byte[] data) {
    return invocation -> {
      long position = invocation.getArgument(1);
      byte[] destination = invocation.getArgument(2);
      int destinationOffset = invocation.getArgument(3);
      int length = invocation.getArgument(4);
      int bytesToCopy = (int) Math.min(length, data.length - position);
      System.arraycopy(data, (int) position, destination, destinationOffset,
          bytesToCopy);
      AbfsRestOperation op = mock(AbfsRestOperation.class);
      AbfsHttpOperation result = mock(AbfsHttpOperation.class);
      when(op.getResult()).thenReturn(result);
      when(result.getBytesReceived()).thenReturn((long) bytesToCopy);
      when(op.getSasToken()).thenReturn(null);
      return op;
    };
  }

  /** A layout with one range covering the file, carrying the given handle. */
  private static BlobLayoutResponse singleRangeLayout(int fileSize,
      String handle, long expiresAt) {
    BlobLayoutResponse layout = new BlobLayoutResponse();
    layout.addEndpoint(new BlobLayoutResponse.Endpoint(0, DIRECT_READ_ENDPOINT));
    layout.addRange(new BlobLayoutResponse.Range(0, fileSize - 1, 0, handle,
        expiresAt));
    return layout;
  }

  /** A mocked getBlobLayout operation with a resettable body. */
  private static AbfsRestOperation layoutOperation() throws Exception {
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    AbfsHttpOperation result = mock(AbfsHttpOperation.class);
    when(op.getResult()).thenReturn(result);
    when(result.getListResultStream())
        .thenReturn(new ByteArrayInputStream(new byte[] {'{'}));
    return op;
  }

  /**
   * Stubs getBlobLayout so that successive fetches return the given layouts.
   * The last layout is returned for any further fetch.
   */
  private static void stubLayoutFetch(AbfsClient client,
      BlobLayoutResponse first, BlobLayoutResponse... rest) throws Exception {
    LayoutResponseParser parser = mock(LayoutResponseParser.class);
    when(parser.parse(any(InputStream.class))).thenReturn(first, rest);
    when(client.getLayoutParser()).thenReturn(parser);
    when(client.getBlobLayout(nullable(String.class), anyLong(), anyLong(),
        nullable(String.class), nullable(String.class),
        nullable(TracingContext.class)))
        .thenAnswer(invocation -> layoutOperation());
  }

  /**
   * A Direct Read client whose layout fetches return the given layouts in
   * order. Both read overloads serve bytes from {@code data}.
   */
  private AbfsClient createRefreshingLayoutClient(byte[] data,
      BlobLayoutResponse first, BlobLayoutResponse... rest) throws Exception {
    AbfsClient client = createLayoutFetchingClient(true, data, "unused");
    stubLayoutFetch(client, first, rest);
    when(client.read(nullable(String.class), anyLong(), nullable(byte[].class),
        anyInt(), anyInt(), nullable(String.class), nullable(String.class),
        nullable(ContextEncryptionAdapter.class), nullable(TracingContext.class)))
        .thenAnswer(serveFrom(data));
    return client;
  }

  /**
   * A stream where every positioned read goes to the client, with no
   * buffering and no read-ahead, so each pread maps to one findReadTarget().
   */
  private AbfsInputStream createPreadStream(AbfsClient client, int fileSize,
      String eTag) {
    AbfsInputStreamContext context = new AbfsInputStreamContext(-1)
        .withReadBufferSize(fileSize)
        .withReadAheadQueueDepth(0)
        .withReadAheadBlockSize(fileSize)
        .isReadAheadV2Enabled(false)
        .withBufferedPreadDisabled(true);
    return new AbfsAdaptiveInputStream(client, null, "/file", fileSize, context,
        eTag, new TracingContext("test-correlation-id", "test-fs-id",
        FSOperationType.READ, true, TracingHeaderFormat.ALL_ID_FORMAT, null));
  }

  /** All ReadTargets passed to the target-aware client read, in call order. */
  private static List<ReadTarget> captureAllReadTargets(AbfsClient client)
      throws Exception {
    ArgumentCaptor<ReadTarget> captor = ArgumentCaptor.forClass(ReadTarget.class);
    verify(client, atLeast(0)).read(nullable(String.class), anyLong(),
        nullable(byte[].class), anyInt(), anyInt(), nullable(String.class),
        nullable(String.class), nullable(ContextEncryptionAdapter.class),
        nullable(TracingContext.class), captor.capture());
    return captor.getAllValues();
  }

  private static void verifyLayoutCalls(AbfsClient client, int expected)
      throws Exception {
    verify(client, times(expected)).getBlobLayout(nullable(String.class),
        anyLong(), anyLong(), nullable(String.class), nullable(String.class),
        nullable(TracingContext.class));
  }

  /**
   * A cached handle inside the refresh window (default 30 s) is replaced by a
   * fresh one before the next read.
   */
  @Test
  public void testHandleInsideGracePeriodIsRefreshed() throws Exception {
    int fileSize = ONE_KB;
    byte[] data = generateTestData(fileSize);
    long now = System.currentTimeMillis();
    AbfsClient client = createRefreshingLayoutClient(data,
        singleRangeLayout(fileSize, "old-handle", now + TimeUnit.SECONDS.toMillis(10)),
        singleRangeLayout(fileSize, "new-handle", now + TimeUnit.MINUTES.toMillis(5)));

    try (AbfsInputStream stream =
             createPreadStream(client, fileSize, "etag-" + UUID.randomUUID())) {
      byte[] first = new byte[128];
      byte[] second = new byte[128];
      assertThat(stream.read(0, first, 0, 128)).isEqualTo(128);
      assertThat(stream.read(128, second, 0, 128)).isEqualTo(128);
      assertThat(second).containsExactly(Arrays.copyOfRange(data, 128, 256));
    }

    // Fetch 1 issues old-handle. Read 2 sees it in the refresh window
    // and fetches again.
    verifyLayoutCalls(client, 2);
    assertThat(captureAllReadTargets(client))
        .extracting(ReadTarget::handle)
        .containsExactly("old-handle", "new-handle");
  }

  /** A handle well outside the refresh window is reused with no extra fetch. */
  @Test
  public void testHandleOutsideGracePeriodIsReused() throws Exception {
    int fileSize = ONE_KB;
    byte[] data = generateTestData(fileSize);
    AbfsClient client = createRefreshingLayoutClient(data,
        singleRangeLayout(fileSize, "handle-1",
            System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5)));

    try (AbfsInputStream stream =
             createPreadStream(client, fileSize, "etag-" + UUID.randomUUID())) {
      stream.read(0, new byte[128], 0, 128);
      stream.read(128, new byte[128], 0, 128);
    }

    verifyLayoutCalls(client, 1);
    assertThat(captureAllReadTargets(client))
        .extracting(ReadTarget::handle)
        .containsExactly("handle-1", "handle-1");
  }

  /**
   * Each handle rejection refreshes the layout and retries once with the new
   * handle. The caller gets the correct data.
   */
  @ParameterizedTest(name = "{1}")
  @CsvSource({
      "400, InvalidDataHandle",
      "409, DataHandleExpired",
      "409, DataHandleInvalidated"})
  public void testRejectedHandleIsRefreshedAndRetriedOnce(int status,
      String errorCode) throws Exception {
    int fileSize = ONE_KB;
    byte[] data = generateTestData(fileSize);
    long expiry = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5);
    AbfsClient client = createRefreshingLayoutClient(data,
        singleRangeLayout(fileSize, "old-handle", expiry),
        singleRangeLayout(fileSize, "new-handle", expiry));

    // Reject only the old handle. The refreshed handle succeeds.
    // doAnswer(...).when(...) does not call read() while stubbing.
    doAnswer(invocation -> {
      ReadTarget target = invocation.getArgument(9);
      if (target != null && "old-handle".equals(target.handle())) {
        throw new AbfsRestOperationException(status, errorCode, errorCode, null);
      }
      return serveFrom(data).answer(invocation);
    }).when(client).read(nullable(String.class), anyLong(),
        nullable(byte[].class), anyInt(), anyInt(), nullable(String.class),
        nullable(String.class), nullable(ContextEncryptionAdapter.class),
        nullable(TracingContext.class), nullable(ReadTarget.class));

    byte[] buffer = new byte[256];
    try (AbfsInputStream stream =
             createPreadStream(client, fileSize, "etag-" + UUID.randomUUID())) {
      assertThat(stream.read(0, buffer, 0, buffer.length)).isEqualTo(256);
    }

    assertThat(buffer).containsExactly(Arrays.copyOf(data, 256));

    // The initial fetch plus one refresh after the rejection.
    verifyLayoutCalls(client, 2);

    // One rejected attempt, then one successful retry with the new handle.
    assertThat(captureAllReadTargets(client))
        .extracting(ReadTarget::handle)
        .containsExactly("old-handle", "new-handle");
  }


  /** If the retry is also rejected, the error reaches the caller. No loop. */
  @Test
  public void testSecondHandleRejectionIsPropagated() throws Exception {
    int fileSize = ONE_KB;
    byte[] data = generateTestData(fileSize);
    long expiry = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5);
    AbfsClient client = createRefreshingLayoutClient(data,
        singleRangeLayout(fileSize, "old-handle", expiry),
        singleRangeLayout(fileSize, "new-handle", expiry));

    // Every handle-backed read is rejected, including the retry.
    doThrow(new AbfsRestOperationException(400, "InvalidDataHandle",
        "InvalidDataHandle", null))
        .when(client).read(nullable(String.class), anyLong(),
            nullable(byte[].class), anyInt(), anyInt(), nullable(String.class),
            nullable(String.class), nullable(ContextEncryptionAdapter.class),
            nullable(TracingContext.class), nullable(ReadTarget.class));

    try (AbfsInputStream stream =
             createPreadStream(client, fileSize, "etag-" + UUID.randomUUID())) {
      assertThatThrownBy(() -> stream.read(0, new byte[128], 0, 128))
          .isInstanceOf(IOException.class);
    }

    // One attempt with the old handle, one retry with the refreshed handle,
    // then the error is propagated. No further retries.
    assertThat(captureAllReadTargets(client))
        .describedAs("Exactly one attempt plus one retry")
        .extracting(ReadTarget::handle)
        .containsExactly("old-handle", "new-handle");

    // The initial fetch plus one refresh after the rejection.
    verifyLayoutCalls(client, 2);
  }

  /** Errors that are not handle errors are not retried here. */
  @Test
  public void testNonHandleErrorIsNotRetried() throws Exception {
    int fileSize = ONE_KB;
    byte[] data = generateTestData(fileSize);
    AbfsClient client = createRefreshingLayoutClient(data,
        singleRangeLayout(fileSize, "handle-1",
            System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5)));

    // doThrow(...).when(...) does not call read(), so the existing
    // data-copying answer is not triggered during stubbing.
    doThrow(new AbfsRestOperationException(500, "InternalError",
        "InternalError", null))
        .when(client).read(nullable(String.class), anyLong(),
            nullable(byte[].class), anyInt(), anyInt(), nullable(String.class),
            nullable(String.class), nullable(ContextEncryptionAdapter.class),
            nullable(TracingContext.class), nullable(ReadTarget.class));

    try (AbfsInputStream stream =
             createPreadStream(client, fileSize, "etag-" + UUID.randomUUID())) {
      assertThatThrownBy(() -> stream.read(0, new byte[128], 0, 128))
          .isInstanceOf(IOException.class);
    }

    // One attempt, no retry, and no extra layout fetch.
    assertThat(captureAllReadTargets(client)).hasSize(1);
    verifyLayoutCalls(client, 1);
  }

  /**
   * DataHandleInvalidated after the file changed: the refetch and the fallback
   * read both carry the stream's original eTag, so the change is detected
   * instead of new content being returned.
   */
  @Test
  public void testInvalidatedHandleKeepsOriginalETagCondition()
      throws Exception {
    int fileSize = ONE_KB;
    byte[] data = generateTestData(fileSize);
    String eTag = "etag-" + UUID.randomUUID();
    AbfsClient client = createRefreshingLayoutClient(data,
        singleRangeLayout(fileSize, "old-handle",
            System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5)));

    AbfsRestOperationException conditionNotMet = new AbfsRestOperationException(
        412, "ConditionNotMet", "ConditionNotMet", null);

    // The first fetch succeeds. The refetch fails its If-Match because the
    // file changed. doX().when() registers stubs without calling the method.
    doAnswer(invocation -> layoutOperation())
        .doThrow(conditionNotMet)
        .when(client).getBlobLayout(nullable(String.class), anyLong(),
            anyLong(), nullable(String.class), nullable(String.class),
            nullable(TracingContext.class));

    // The read with the handle is rejected because the data changed.
    doThrow(new AbfsRestOperationException(409, "DataHandleInvalidated",
        "DataHandleInvalidated", null))
        .when(client).read(nullable(String.class), anyLong(),
            nullable(byte[].class), anyInt(), anyInt(), nullable(String.class),
            nullable(String.class), nullable(ContextEncryptionAdapter.class),
            nullable(TracingContext.class), nullable(ReadTarget.class));

    // The normal read that follows also fails its eTag check.
    doThrow(conditionNotMet)
        .when(client).read(nullable(String.class), anyLong(),
            nullable(byte[].class), anyInt(), anyInt(), nullable(String.class),
            nullable(String.class), nullable(ContextEncryptionAdapter.class),
            nullable(TracingContext.class));

    try (AbfsInputStream stream = createPreadStream(client, fileSize, eTag)) {
      assertThatThrownBy(() -> stream.read(0, new byte[128], 0, 128))
          .isInstanceOf(IOException.class);
    }

    // The initial fetch plus one refetch, both under the stream's eTag.
    ArgumentCaptor<String> layoutETags = ArgumentCaptor.forClass(String.class);
    verify(client, times(2)).getBlobLayout(nullable(String.class), anyLong(),
        anyLong(), layoutETags.capture(), nullable(String.class),
        nullable(TracingContext.class));
    assertThat(layoutETags.getAllValues())
        .describedAs("Every layout fetch uses the stream's eTag")
        .containsOnly(eTag);

    // The failed refetch marks the layout unavailable, so the retry goes
    // through the normal read path, which must still send If-Match.
    ArgumentCaptor<String> readETags = ArgumentCaptor.forClass(String.class);
    verify(client, times(1)).read(nullable(String.class), anyLong(),
        nullable(byte[].class), anyInt(), anyInt(), readETags.capture(),
        nullable(String.class), nullable(ContextEncryptionAdapter.class),
        nullable(TracingContext.class));
    assertThat(readETags.getValue())
        .describedAs("Fallback read keeps If-Match, not \"*\"")
        .isEqualTo(eTag);
  }

  /** An expired cached handle is replaced by a fresh one, never sent. */
  @Test
  public void testExpiredCachedHandleIsRefreshed() throws Exception {
    HandleReadTestContext context = createHandleReadTestContext(0, 511,
        "expired-data-handle",
        System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(1));

    try (AbfsInputStream stream = context.stream()) {
      byte[] buffer = new byte[128];
      assertEquals(buffer.length, stream.read(0, buffer, 0, buffer.length));
      assertThat(buffer).containsExactly(Arrays.copyOf(context.data(), 128));
    }

    verifyLayoutCalls(context.client(), 1);
    assertThat(captureReadTargetAtPosition(context.client(), 0).handle())
        .isEqualTo(REFRESHED_HANDLE);
    assertThat(captureAllReadTargets(context.client()))
        .noneMatch(t -> t != null && "expired-data-handle".equals(t.handle()));
  }

  /** At exactly the expiry time, the handle is refreshed. */
  @Test
  public void testDataHandleAtExpiryBoundaryIsRefreshed() throws Exception {
    HandleReadTestContext context = createHandleReadTestContext(0, 511,
        "boundary-data-handle", System.currentTimeMillis());

    try (AbfsInputStream stream = context.stream()) {
      assertEquals(1, stream.read(0, new byte[1], 0, 1));
    }

    assertThat(captureReadTargetAtPosition(context.client(), 0).handle())
        .isEqualTo(REFRESHED_HANDLE);
  }

  /**
   * If the refresh fails, the read still succeeds through the normal path,
   * and the expired handle is never sent.
   */
  @Test
  public void testExpiredHandleFallsBackWhenRefreshFails() throws Exception {
    HandleReadTestContext context = createHandleReadTestContext(0, 511,
        "expired-data-handle", System.currentTimeMillis() - 1);

    // The layout refresh fails. doThrow(...).when(...) replaces the existing
    // getBlobLayout stub without calling the method.
    doThrow(new AbfsRestOperationException(500, "InternalError",
        "Simulated layout failure", null))
        .when(context.client()).getBlobLayout(nullable(String.class),
            anyLong(), anyLong(), nullable(String.class),
            nullable(String.class), nullable(TracingContext.class));

    try (AbfsInputStream stream = context.stream()) {
      byte[] buffer = new byte[256];
      assertEquals(buffer.length, stream.read(0, buffer, 0, buffer.length));
      assertThat(buffer).containsExactly(Arrays.copyOf(context.data(), 256));
    }

    // The expired handle triggered a refresh attempt.
    verify(context.client(), atLeastOnce()).getBlobLayout(
        nullable(String.class), anyLong(), anyLong(),
        nullable(String.class), nullable(String.class),
        nullable(TracingContext.class));

    // The expired handle never reached the service.
    assertThat(captureAllReadTargets(context.client()))
        .noneMatch(t -> t != null && "expired-data-handle".equals(t.handle()));

    // The failed refresh marked the layout unavailable, so the data came
    // through the normal read path.
    verify(context.client(), atLeastOnce()).read(nullable(String.class),
        anyLong(), nullable(byte[].class), anyInt(), anyInt(),
        nullable(String.class), nullable(String.class),
        nullable(ContextEncryptionAdapter.class), nullable(TracingContext.class));
  }

  /** An expired cached handle causes one refresh, not one per read. */
  @Test
  public void testExpiredCachedHandleTriggersOneLayoutRefresh()
      throws Exception {
    HandleReadTestContext context = createHandleReadTestContext(0, 511,
        "expired-cached-handle", System.currentTimeMillis() - 1);

    try (AbfsInputStream stream = context.stream()) {
      assertEquals(64, stream.read(0, new byte[64], 0, 64));
      assertEquals(64, stream.read(64, new byte[64], 0, 64));
    }

    verifyLayoutCalls(context.client(), 1);
    assertThat(captureAllReadTargets(context.client()))
        .noneMatch(t -> t != null && "expired-cached-handle".equals(t.handle()));
  }

  /** With a zero grace period, a handle 10 s from expiry is reused, not refreshed. */
  @Test
  public void testZeroGracePeriodDoesNotRefreshEarly() throws Exception {
    int fileSize = ONE_KB;
    byte[] data = generateTestData(fileSize);
    AbfsClient client = createRefreshingLayoutClient(data,
        singleRangeLayout(fileSize, "handle-1",
            System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10)));

    AbfsConfiguration conf = spy(client.getAbfsConfiguration());
    doReturn(0L).when(conf).getDirectReadHandleRefreshGracePeriodMs();
    doReturn(conf).when(client).getAbfsConfiguration();

    try (AbfsInputStream stream =
             createPreadStream(client, fileSize, "etag-" + UUID.randomUUID())) {
      stream.read(0, new byte[128], 0, 128);
      stream.read(128, new byte[128], 0, 128);
    }

    verifyLayoutCalls(client, 1);
    assertThat(captureAllReadTargets(client))
        .extracting(ReadTarget::handle)
        .containsExactly("handle-1", "handle-1");
  }

}
