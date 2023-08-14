package org.apache.hadoop.fs.azurebfs.services;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;
import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Random;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOCK;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOCK_BLOB_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_DNS_PREFIX;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.WASB_DNS_PREFIX;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_LENGTH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_MD5;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_BLOB_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_COMP;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.TEST_CONFIGURATION_FILE_NAME;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class ITestBlobOperation extends AbstractAbfsIntegrationTest {
    private static final int BUFFER_LENGTH = 5;
    private static final int BUFFER_OFFSET = 0;
    private static final String TEST_PATH = "/testfile";
    AzureBlobFileSystem fs;
    private final Path testPath = new Path("/testfile");

    public ITestBlobOperation() throws Exception {
        super.setup();
        fs = getFileSystem();
        PrefixMode prefixMode = getPrefixMode(fs);
        Assume.assumeTrue(prefixMode == PrefixMode.BLOB);
    }

    /**
     * Test helper method to get random bytes array.
     *
     * @param length The length of byte buffer
     * @return byte buffer
     */
    private byte[] getRandomBytesArray(int length) {
        final byte[] b = new byte[length];
        new Random().nextBytes(b);
        return b;
    }

    private String computeMd5(byte[] data) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(data);

        String md5Base64 = Base64.getEncoder().encodeToString(digest);
        return md5Base64;
    }

    /**
     * Tests the putblob success scenario.
     */
    @Test
    public void testPutBlob() throws Exception {
        // Get the filesystem.
        final AzureBlobFileSystem fs = getFileSystem();
        final Configuration configuration = new Configuration();
        configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
        AbfsClient abfsClient = getClient(fs);

        AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
                configuration.get(FS_AZURE_ABFS_ACCOUNT_NAME));

        // Gets the client.
        AbfsClient testClient = Mockito.spy(ITestAbfsClient.createTestClientFromCurrentContext(
                abfsClient,
                abfsConfiguration));

        byte[] buffer = getRandomBytesArray(5);

        // Create a test container to upload the data.
        Path testPath = path(TEST_PATH);
        String finalTestPath = testPath.toString().substring(testPath.toString().lastIndexOf("/"));

        // Creates a list of request headers.
        final List<AbfsHttpHeader> requestHeaders = ITestAbfsClient.getTestRequestHeaders(testClient);
        requestHeaders.add(new AbfsHttpHeader(CONTENT_LENGTH, String.valueOf(buffer.length)));
        requestHeaders.add(new AbfsHttpHeader(X_MS_BLOB_TYPE, BLOCK_BLOB_TYPE));
        String ContentMD5 = computeMd5(buffer);
        // Updates the query parameters.
        final AbfsUriQueryBuilder abfsUriQueryBuilder = testClient.createDefaultUriQueryBuilder();

        // Creates the url for the specified path.
        URL url = testClient.createRequestUrl(finalTestPath, abfsUriQueryBuilder.toString());
        /*
        * Since the AbfsRestOperation object is not similar to what is there in
        * client.createBlobPath (we dont send buffer in client.createBlobPath),
        * the url would be replaced for `.dfs.` to .blob.`
        */
        url = new URL(url.toString().replace(ABFS_DNS_PREFIX, WASB_DNS_PREFIX));

        // Create a mock of the AbfsRestOperation to set the urlConnection in the corresponding httpOperation.
        AbfsRestOperation op = new AbfsRestOperation(
                AbfsRestOperationType.PutBlob,
                testClient,
                HTTP_METHOD_PUT,
                url,
                requestHeaders, buffer,
                BUFFER_OFFSET,
                BUFFER_LENGTH, null);

        TracingContext tracingContext = new TracingContext("abcd",
                "abcde", FSOperationType.CREATE,
                TracingHeaderFormat.ALL_ID_FORMAT, null);

        op.execute(tracingContext);

        // Validate the content by comparing the md5 computed and the value obtained from server
        Assertions.assertThat(op.getResult().getResponseHeader(CONTENT_MD5))
                .describedAs("The content md5 value is not correct")
                .isEqualTo(ContentMD5);
        Assertions.assertThat(op.getResult().getStatusCode())
                .describedAs("The creation failed")
                .isEqualTo(HTTP_CREATED);
    }

    private static String generateBlockListXml(List<String> blockIds) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        stringBuilder.append("<BlockList>\n");
        for (String blockId : blockIds) {
            String blockId1 = Base64.getEncoder().encodeToString(blockId.getBytes());
            stringBuilder.append(String.format("<Latest>%s</Latest>\n", blockId1));
        }
        stringBuilder.append("</BlockList>\n");
        return stringBuilder.toString();
    }

    @Test
    public void testPutBlockWithNullData() throws Exception {
        final AzureBlobFileSystem fs = getFileSystem();
        final Configuration configuration = new Configuration();
        configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
        AbfsClient abfsClient = getClient(fs);

        AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
                configuration.get(FS_AZURE_ABFS_ACCOUNT_NAME));

        AbfsClient testClient = Mockito.spy(ITestAbfsClient.createTestClientFromCurrentContext(
                abfsClient,
                abfsConfiguration));

        String blockId = "block1";
        byte[] data = new byte[0];
        Path testPath = path(TEST_PATH);
        String finalTestPath = testPath.toString()
          .substring(testPath.toString().lastIndexOf("/"));

        AbfsRestOperation[] op = new AbfsRestOperation[1];
        Mockito.doAnswer(answer -> {
            AbfsRestOperation answerOp
                = (AbfsRestOperation) answer.callRealMethod();
            op[0] = answerOp;
            return answerOp;
          })
          .when(testClient)
          .getPutBlockOperation(Mockito.any(byte[].class),
              Mockito.any(AppendRequestParameters.class), Mockito.anyList(),
              Mockito.nullable(String.class), Mockito.any(URL.class));

        TracingContext tracingContext = Mockito.spy(new TracingContext("abcd",
          "abcde", FSOperationType.APPEND,
          TracingHeaderFormat.ALL_ID_FORMAT, null));
        String blockId1 = Base64.getEncoder().encodeToString(blockId.getBytes());
        AppendRequestParameters appendRequestParameters = Mockito.mock(
          AppendRequestParameters.class);
        Mockito.doReturn(0).when(appendRequestParameters).getoffset();
        Mockito.doReturn(0).when(appendRequestParameters).getLength();
        intercept(IOException.class, () -> {
          testClient.append(blockId1, finalTestPath, data,
            appendRequestParameters, null, tracingContext, "");
        });
    }

    @Test
    public void testPutBlockWithDifferentLengthBlockIds() throws Exception {
        final AzureBlobFileSystem fs = getFileSystem();
        final Configuration configuration = new Configuration();
        configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
        AbfsClient abfsClient = getClient(fs);

        AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
                configuration.get(FS_AZURE_ABFS_ACCOUNT_NAME));
        List<String> blockIds = new ArrayList<>(Arrays.asList(
                "block-1",
                "block-2122",
                "block-312234"
        ));
        List<byte[]> blockData = new ArrayList<>(Arrays.asList(
                "hello".getBytes(),
                "world".getBytes(),
                "!".getBytes()
        ));
        AbfsClient testClient = Mockito.spy(ITestAbfsClient.createTestClientFromCurrentContext(
                abfsClient,
                abfsConfiguration));

        Path testPath = path(TEST_PATH);
        String finalTestPath = testPath.toString().substring(testPath.toString().lastIndexOf("/"));

        AbfsRestOperation[] op = new AbfsRestOperation[1];
        Mockito.doAnswer(answer -> {
            AbfsRestOperation answerOp
                = (AbfsRestOperation) answer.callRealMethod();
            op[0] = answerOp;
            return answerOp;
          })
          .when(testClient)
          .getPutBlockOperation(Mockito.any(byte[].class),
              Mockito.any(AppendRequestParameters.class), Mockito.anyList(),
              Mockito.nullable(String.class), Mockito.any(URL.class));
        TracingContext tracingContext = Mockito.spy(new TracingContext("abcd",
          "abcde", FSOperationType.APPEND,
          TracingHeaderFormat.ALL_ID_FORMAT, null));
        List<String> encodedBlockIds = new ArrayList<>();
        for (int i = 0; i < blockIds.size(); i++) {
          String blockId1 = Base64.getEncoder()
            .encodeToString(blockIds.get(i).getBytes());
          byte[] data = blockData.get(i);
          AppendRequestParameters appendRequestParameters = Mockito.mock(
            AppendRequestParameters.class);
          Mockito.doReturn(0).when(appendRequestParameters).getoffset();
          Mockito.doReturn(data.length).when(appendRequestParameters).getLength();
          if (i >= 1) {
            intercept(IOException.class, () -> {
              testClient.append(blockId1, finalTestPath, data,
                appendRequestParameters, null, tracingContext, "abcd");
            encodedBlockIds.add(blockId1);
          });
          Assertions.assertThat(op[0].getResult().getStatusCode())
              .describedAs("The error code is not correct")
              .isEqualTo(HTTP_BAD_REQUEST);
          } else {
            testClient.append(blockId1, finalTestPath, data,
              appendRequestParameters, null, tracingContext, "abcd");
            encodedBlockIds.add(blockId1);
          }
        }
    }

    @Test
    public void testGetCommittedBlockList() throws IOException, IllegalAccessException {
        final AzureBlobFileSystem fs = getFileSystem();
        final Configuration configuration = new Configuration();
        configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
        AbfsClient abfsClient = getClient(fs);

        AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
                configuration.get(FS_AZURE_ABFS_ACCOUNT_NAME));
        List<String> blockIds = new ArrayList<>(Arrays.asList(
                "block-1",
                "block-2",
                "block-3"
        ));
        List<byte[]> blockData = new ArrayList<>(Arrays.asList(
                "hello".getBytes(),
                "world".getBytes(),
                "!".getBytes()
        ));
        AbfsClient testClient = Mockito.spy(ITestAbfsClient.createTestClientFromCurrentContext(
                abfsClient,
                abfsConfiguration));
        Path testPath = path(TEST_PATH);
        String finalTestPath = testPath.toString().substring(testPath.toString().lastIndexOf("/"));

        TracingContext tracingContext = Mockito.spy(new TracingContext("abcd",
          "abcde", FSOperationType.APPEND,
          TracingHeaderFormat.ALL_ID_FORMAT, null));
        List<String> encodedBlockIds = new ArrayList<>();
        for (int i = 0; i < blockIds.size(); i++) {
          String blockId1 = Base64.getEncoder()
            .encodeToString(blockIds.get(i).getBytes());
          byte[] data = blockData.get(i);
          AppendRequestParameters appendRequestParameters = Mockito.mock(
            AppendRequestParameters.class);
          Mockito.doReturn(0).when(appendRequestParameters).getoffset();
          Mockito.doReturn(data.length).when(appendRequestParameters).getLength();
          testClient.append(blockId1, finalTestPath, data,
            appendRequestParameters, null, tracingContext, "");
          encodedBlockIds.add(blockId1);
        }
        byte[] bufferString = generateBlockListXml(blockIds).getBytes(
          StandardCharsets.UTF_8);

        tracingContext = Mockito.spy(new TracingContext("abcd",
          "abcde", FSOperationType.APPEND,
          TracingHeaderFormat.ALL_ID_FORMAT, null));
        testClient.flush(bufferString, finalTestPath, false, null, null, "",
          tracingContext);

        /* Validates that all blocks are committed and fetched */
        AbfsRestOperation op1 = testClient.getBlockList(finalTestPath, tracingContext);
        List<String> committedBlockList = op1.getResult().getBlockIdList();
        assertEquals(encodedBlockIds, committedBlockList);
    }

    @Test
    public void testPutBlockListForAdditionalBlockId() throws Exception {
        final AzureBlobFileSystem fs = getFileSystem();
        final Configuration configuration = new Configuration();
        configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
        AbfsClient abfsClient = getClient(fs);

        AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
                configuration.get(FS_AZURE_ABFS_ACCOUNT_NAME));
        List<String> blockIds = new ArrayList<>(Arrays.asList(
                "block-1",
                "block-2",
                "block-3",
                "block-4"
        ));
        List<byte[]> blockData = new ArrayList<>(Arrays.asList(
                "hello".getBytes(),
                "world".getBytes(),
                "!".getBytes()
        ));
        AbfsClient testClient = Mockito.spy(ITestAbfsClient.createTestClientFromCurrentContext(
                abfsClient,
                abfsConfiguration));
        Path testPath = path(TEST_PATH);
        String finalTestPath = testPath.toString().substring(testPath.toString().lastIndexOf("/"));
        final List<AbfsHttpHeader> requestHeaders = ITestAbfsClient.getTestRequestHeaders(testClient);
        final AbfsUriQueryBuilder abfsUriQueryBuilder = testClient.createDefaultUriQueryBuilder();
        abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, BLOCK);

        TracingContext tracingContext = Mockito.spy(new TracingContext("abcd",
          "abcde", FSOperationType.APPEND,
          TracingHeaderFormat.ALL_ID_FORMAT, null));
        List<String> encodedBlockIds = new ArrayList<>();
        AbfsRestOperation op1 = null;
        for (int i = 0; i < blockIds.size() - 1; i++) {
          String blockId1 = Base64.getEncoder()
            .encodeToString(blockIds.get(i).getBytes());
          byte[] data = blockData.get(i);
          AppendRequestParameters appendRequestParameters = Mockito.mock(
            AppendRequestParameters.class);
          Mockito.doReturn(0).when(appendRequestParameters).getoffset();
          Mockito.doReturn(data.length).when(appendRequestParameters).getLength();
          op1 = testClient.append(blockId1, finalTestPath, data,
            appendRequestParameters, null, tracingContext, "");
          encodedBlockIds.add(blockId1);
        }
        byte[] bufferString = generateBlockListXml(blockIds).getBytes(
          StandardCharsets.UTF_8);

        final TracingContext blockListTc = Mockito.spy(new TracingContext("abcd",
          "abcde", FSOperationType.APPEND,
          TracingHeaderFormat.ALL_ID_FORMAT, null));

        /* Verify that an additional blockId which is not staged if we try to commit, it throws an exception */
        AbfsRestOperation[] op = new AbfsRestOperation[1];
        Mockito.doAnswer(answer -> {
            AbfsRestOperation answerOp
                = (AbfsRestOperation) answer.callRealMethod();
            op[0] = answerOp;
            return answerOp;
          })
          .when(testClient)
          .getPutBlockListOperation(Mockito.any(byte[].class),
              Mockito.anyList(), Mockito.nullable(String.class),
              Mockito.any(URL.class));
        AbfsRestOperation finalOp = op1;
        intercept(IOException.class, () -> {
          testClient.flush(bufferString, finalTestPath, false, null, null, "",
            blockListTc);
        });
        Assertions.assertThat(op[0].getResult().getStatusCode())
          .describedAs("The error code is not correct")
          .isEqualTo(HTTP_BAD_REQUEST);
    }

    /*
     * Helper method that creates test data of size provided by the
     * "size" parameter.
     */
    private static byte[] getTestData(int size) {
        byte[] testData = new byte[size];
        System.arraycopy(RandomStringUtils.randomAlphabetic(size).getBytes(), 0, testData, 0, size);
        return testData;
    }

    // Helper method to create file and write fileSize bytes of data on it.
    private byte[] createBaseFileWithData(int fileSize, Path testPath) throws Throwable {
        // To create versions
        try (FSDataOutputStream createStream = fs.create(testPath)) {
        }
        fs.delete(testPath, false);
        try (FSDataOutputStream createStream = fs.create(testPath)) {
            byte[] fileData = null;
            if (fileSize != 0) {
                fileData = getTestData(fileSize);
                createStream.write(fileData);
            }
            assertTrue(fs.exists(testPath));
            return fileData;
        }
    }

    /*
     * Helper method to verify a file data equal to "dataLength" parameter
     */
    private boolean verifyFileData(int dataLength, byte[] testData, int testDataIndex,
                                   FSDataInputStream srcStream) {
        try {
            byte[] fileBuffer = new byte[dataLength];
            byte[] testDataBuffer = new byte[dataLength];
            int fileBytesRead = srcStream.read(fileBuffer);
            if (fileBytesRead < dataLength) {
                return false;
            }
            System.arraycopy(testData, testDataIndex, testDataBuffer, 0, dataLength);
            if (!Arrays.equals(fileBuffer, testDataBuffer)) {
                return false;
            }
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    /*
     * Helper method to verify Append on a testFile.
     */
    private boolean verifyAppend(byte[] testData, Path testFile) {
        try (FSDataInputStream srcStream = fs.open(testFile)) {
            int baseBufferSize = 2048;
            int testDataSize = testData.length;
            int testDataIndex = 0;
            while (testDataSize > baseBufferSize) {
                if (!verifyFileData(baseBufferSize, testData, testDataIndex, srcStream)) {
                    return false;
                }
                testDataIndex += baseBufferSize;
                testDataSize -= baseBufferSize;
            }
            if (!verifyFileData(testDataSize, testData, testDataIndex, srcStream)) {
                return false;
            }
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    /*
     * Test case to verify if an append on small size data works. This tests
     * append E2E
     */
    @Test
    public void testSingleAppend() throws Throwable {
        FSDataOutputStream appendStream = null;
        try {
            int baseDataSize = 50;
            byte[] baseDataBuffer = createBaseFileWithData(baseDataSize, testPath);

            int appendDataSize = 20;
            byte[] appendDataBuffer = getTestData(appendDataSize);
            appendStream = fs.append(testPath, 10);
            appendStream.write(appendDataBuffer);
            appendStream.close();
            byte[] testData = new byte[baseDataSize + appendDataSize];
            System.arraycopy(baseDataBuffer, 0, testData, 0, baseDataSize);
            System.arraycopy(appendDataBuffer, 0, testData, baseDataSize, appendDataSize);

            assertTrue(verifyAppend(testData, testPath));
        } finally {
            if (appendStream != null) {
                appendStream.close();
            }
        }
    }

    /*
     * Test case to verify append to an empty file.
     */
    @Test
    public void testSingleAppendOnEmptyFile() throws Throwable {
        FSDataOutputStream appendStream = null;
        try {
            createBaseFileWithData(0, testPath);
            int appendDataSize = 20;
            byte[] appendDataBuffer = getTestData(appendDataSize);
            appendStream = fs.append(testPath, 10);
            appendStream.write(appendDataBuffer);
            appendStream.close();
            assertTrue(verifyAppend(appendDataBuffer, testPath));
        } finally {
            if (appendStream != null) {
                appendStream.close();
            }
        }
    }

    /*
     * Tests to verify multiple appends on a Blob.
     */
    @Test
    public void testMultipleAppends() throws Throwable {
        int baseDataSize = 50;
        byte[] baseDataBuffer = createBaseFileWithData(baseDataSize, testPath);
        int appendDataSize = 100;
        int targetAppendCount = 50;
        byte[] testData = new byte[baseDataSize + (appendDataSize * targetAppendCount)];
        int testDataIndex = 0;
        System.arraycopy(baseDataBuffer, 0, testData, testDataIndex, baseDataSize);
        testDataIndex += baseDataSize;
        int appendCount = 0;
        FSDataOutputStream appendStream = null;
        try {
            while (appendCount < targetAppendCount) {
                byte[] appendDataBuffer = getTestData(appendDataSize);
                appendStream = fs.append(testPath, 30);
                appendStream.write(appendDataBuffer);
                appendStream.close();

                System.arraycopy(appendDataBuffer, 0, testData, testDataIndex, appendDataSize);
                testDataIndex += appendDataSize;
                appendCount++;
            }
            assertTrue(verifyAppend(testData, testPath));
        } finally {
            if (appendStream != null) {
                appendStream.close();
            }
        }
    }

    /*
     * Test to verify we multiple appends on the same stream.
     */
    @Test
    public void testMultipleAppendsOnSameStream() throws Throwable {
        int baseDataSize = 50;
        byte[] baseDataBuffer = createBaseFileWithData(baseDataSize, testPath);
        int appendDataSize = 100;
        int targetAppendCount = 50;
        byte[] testData = new byte[baseDataSize + (appendDataSize * targetAppendCount)];
        int testDataIndex = 0;
        System.arraycopy(baseDataBuffer, 0, testData, testDataIndex, baseDataSize);
        testDataIndex += baseDataSize;
        int appendCount = 0;
        FSDataOutputStream appendStream = null;
        try {
            while (appendCount < targetAppendCount) {
                appendStream = fs.append(testPath, 50);
                int singleAppendChunkSize = 20;
                int appendRunSize = 0;
                while (appendRunSize < appendDataSize) {
                    byte[] appendDataBuffer = getTestData(singleAppendChunkSize);
                    appendStream.write(appendDataBuffer);
                    System.arraycopy(appendDataBuffer, 0, testData,
                            testDataIndex + appendRunSize, singleAppendChunkSize);

                    appendRunSize += singleAppendChunkSize;
                }
                appendStream.close();
                testDataIndex += appendDataSize;
                appendCount++;
            }
            assertTrue(verifyAppend(testData, testPath));
        } finally {
            if (appendStream != null) {
                appendStream.close();
            }
        }
    }
}