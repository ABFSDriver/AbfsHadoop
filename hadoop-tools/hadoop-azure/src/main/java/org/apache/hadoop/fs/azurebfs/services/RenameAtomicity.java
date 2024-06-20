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

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Random;
import java.util.TimeZone;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.permission.FsPermission;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;
import static org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore.extractEtagHeader;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.RENAME_PENDING_JSON_DATE_FORMAT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.UTC_TIME_ZONE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.BLOCK_ID_LENGTH;
import static org.apache.hadoop.fs.azurebfs.services.AzureIngressHandler.generateBlockListXml;

/**
 * For a directory enabled for atomic-rename, before rename starts, a file with
 * -RenamePending.json suffix is created. In this file, the states required for the
 * rename operation are given. This file is created by {@link #preRename()} method.
 * This is important in case the JVM process crashes during rename, the atomicity
 * will be maintained, when the job calls {@link AzureBlobFileSystem#listStatus(Path)}
 * or {@link AzureBlobFileSystem#getFileStatus(Path)}. On these API calls to filesystem,
 * it will be checked if there is any RenamePending JSON file. If yes, the crashed rename
 * operation would be resumed as per the file.
 */
public class RenameAtomicity {

  private final TracingContext tracingContext;

  private Path src, dst;

  private String srcEtag;

  private final AbfsBlobClient abfsClient;

  private final Path renameJsonPath;

  public static final String SUFFIX = "-RenamePending.json";

  private int preRenameRetryCount = 0;

  private int renamePendingJsonLen;

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static final ObjectReader READER = new ObjectMapper()
      .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
      .readerFor(JsonNode.class);

  /**
   * Performs pre-rename operations. Creates a file with -RenamePending.json
   * suffix in the source parent directory. This file contains the states
   * required for the rename operation.
   *
   * @param src Source path
   * @param dst Destination path
   * @param renameJsonPath Path of the JSON file to be created
   * @param tracingContext Tracing context
   * @param srcEtag ETag of the source directory
   * @param abfsClient AbfsClient instance
   * @throws IOException Server error while creating / writing json file.
   */
  public RenameAtomicity(final Path src, final Path dst,
      final Path renameJsonPath,
      TracingContext tracingContext,
      final String srcEtag,
      final AbfsClient abfsClient) throws IOException {
    this.src = src;
    this.dst = dst;
    this.abfsClient = (AbfsBlobClient) abfsClient;
    this.renameJsonPath = renameJsonPath;
    this.tracingContext = tracingContext;
    this.srcEtag = srcEtag;
  }

  /**
   * Resumes the rename operation from the JSON file.
   *
   * @param renameJsonPath Path of the JSON file
   * @param tracingContext Tracing context
   * @param srcEtag ETag of the source directory
   * @param abfsClient AbfsClient instance
   * @throws IOException Server error while reading / deleting json file.
   */
  public RenameAtomicity(final Path renameJsonPath,
      final int renamePendingJsonFileLen,
      TracingContext tracingContext,
      final String srcEtag,
      final AbfsClient abfsClient) {
    this.abfsClient = (AbfsBlobClient) abfsClient;
    this.renameJsonPath = renameJsonPath;
    this.tracingContext = tracingContext;
    this.srcEtag = srcEtag;
    this.renamePendingJsonLen = renamePendingJsonFileLen;
  }

  public void redo() throws IOException {
    // parse the JSON
    byte[] buffer = readRenamePendingJson(renameJsonPath, renamePendingJsonLen);
    String contents = new String(buffer, Charset.defaultCharset());
    JsonNode oldFolderName, newFolderName, eTag;
    try {
      try {
        JsonNode json = READER.readValue(contents);

        // initialize this object's fields
        oldFolderName = json.get("OldFolderName");
        newFolderName = json.get("NewFolderName");
        eTag = json.get("ETag");
      } catch (JsonMappingException | JsonParseException e) {
        this.src = null;
        this.dst = null;
        this.srcEtag = null;
        return;
      }
      if (oldFolderName != null && StringUtils.isNotEmpty(
          oldFolderName.textValue())
          && newFolderName != null && StringUtils.isNotEmpty(
          newFolderName.textValue()) && eTag != null
          && StringUtils.isNotEmpty(
          eTag.textValue())) {
        this.src = new Path(oldFolderName.asText());
        this.dst = new Path(newFolderName.asText());
        this.srcEtag = eTag.asText();

        BlobRenameHandler blobRenameHandler = new BlobRenameHandler(
            this.src.toUri().getPath(), dst.toUri().getPath(),
            abfsClient, srcEtag, true, true, tracingContext);

        blobRenameHandler.execute();
      } else {
        this.src = null;
        this.dst = null;
        this.srcEtag = null;
      }
    } finally {
      deleteRenamePendingJson();
    }
  }

  @VisibleForTesting
  byte[] readRenamePendingJson(Path path, int len)
      throws AzureBlobFileSystemException {
    byte[] bytes = new byte[len];
    abfsClient.read(path.toUri().getPath(), 0, bytes, 0,
        len, null, null, null,
        tracingContext);
    return bytes;
  }

  @VisibleForTesting
  void createRenamePendingJson(Path path, byte[] bytes)
      throws AzureBlobFileSystemException {
    // PutBlob on the path.
    AbfsRestOperation putBlobOp = abfsClient.createPath(path.toUri().getPath(),
        true,
        true, null, false, null, null, tracingContext);
    String eTag = extractEtagHeader(putBlobOp.getResult());

    // PutBlock on the path.
    byte[] blockIdByteArray = new byte[BLOCK_ID_LENGTH];
    new Random().nextBytes(blockIdByteArray);
    String blockId = new String(Base64.encodeBase64(blockIdByteArray),
        StandardCharsets.UTF_8);
    AppendRequestParameters appendRequestParameters
        = new AppendRequestParameters(0, 0,
        bytes.length, AppendRequestParameters.Mode.APPEND_MODE, false, null,
        abfsClient.getAbfsConfiguration().isExpectHeaderEnabled(), blockId,
        eTag);

    abfsClient.append(path.toUri().getPath(), bytes,
        appendRequestParameters, null, null, tracingContext);

    // PutBlockList on the path.
    String blockList = generateBlockListXml(Collections.singleton(blockId));
    abfsClient.flush(blockList.getBytes(StandardCharsets.UTF_8),
        path.toUri().getPath(), true, null, null, eTag, tracingContext);
  }

  @VisibleForTesting
  public int preRename() throws IOException {
    String makeRenamePendingFileContents = makeRenamePendingFileContents(
        srcEtag);

    try {
      createRenamePendingJson(renameJsonPath,
          makeRenamePendingFileContents.getBytes(StandardCharsets.UTF_8));
      return makeRenamePendingFileContents.length();
    } catch (IOException e) {
      /*
       * Scenario: file has been deleted by parallel thread before the RenameJSON
       * could be written and flushed. In such case, there has to be one retry of
       * preRename.
       * ref: https://issues.apache.org/jira/browse/HADOOP-12678
       * On DFS endpoint, flush API is called. If file is not there, server returns
       * 404.
       * On blob endpoint, flush API is not there. PutBlockList is called with
       * if-match header. If file is not there, the conditional header will fail,
       * the server will return 412.
       */
      if (isPreRenameRetriableException(e)) {
        preRenameRetryCount++;
        if (preRenameRetryCount == 1) {
          return preRename();
        }
      }
      throw e;
    }
  }

  private boolean isPreRenameRetriableException(IOException e) {
    AbfsRestOperationException ex;
    while (e != null) {
      if (e instanceof AbfsRestOperationException) {
        ex = (AbfsRestOperationException) e;
        return ex.getStatusCode() == HTTP_NOT_FOUND
            || ex.getStatusCode() == HTTP_PRECON_FAILED;
      }
      e = (IOException) e.getCause();
    }
    return false;
  }

  public void postRename() throws IOException {
    deleteRenamePendingJson();
  }

  private void deleteRenamePendingJson() throws AzureBlobFileSystemException {
    try {
      abfsClient.deleteBlobPath(renameJsonPath, null,
          tracingContext);
    } catch (AzureBlobFileSystemException e) {
      if (e instanceof AbfsRestOperationException
          && ((AbfsRestOperationException) e).getStatusCode()
          == HTTP_NOT_FOUND) {
        return;
      }
      throw e;
    }
  }


  /**
   * Return the contents of the JSON file to represent the operations
   * to be performed for a folder rename.
   *
   * @return JSON string which represents the operation.
   */
  private String makeRenamePendingFileContents(String eTag) throws
      JsonProcessingException {
    SimpleDateFormat sdf = new SimpleDateFormat(RENAME_PENDING_JSON_DATE_FORMAT);
    sdf.setTimeZone(UTC_TIME_ZONE);
    String time = sdf.format(new Date());

    // Make file contents as a string. Again, quote file names, escaping
    // characters as appropriate.

    final RenamePendingJsonFormat renamePendingJsonFormat = new RenamePendingJsonFormat(
        src.toUri().getPath(), dst.toUri().getPath(), time, eTag);

    return objectMapper.writeValueAsString(renamePendingJsonFormat);
  }

  /**
   * This is an exact copy of org.codehaus.jettison.json.JSONObject.quote
   * method.
   *
   * Produce a string in double quotes with backslash sequences in all the
   * right places. A backslash will be inserted within </, allowing JSON
   * text to be delivered in HTML. In JSON text, a string cannot contain a
   * control character or an unescaped quote or backslash.
   * @param string A String
   * @return A String correctly formatted for insertion in a JSON text.
   */
  private String quote(String string) {
    if (string == null || string.length() == 0) {
      return "\"\"";
    }

    char c = 0;
    int i;
    int len = string.length();
    StringBuilder sb = new StringBuilder(len + 4);
    String t;

    sb.append('"');
    for (i = 0; i < len; i += 1) {
      c = string.charAt(i);
      switch (c) {
      case '\\':
      case '"':
        sb.append('\\');
        sb.append(c);
        break;
      case '/':
        sb.append('\\');
        sb.append(c);
        break;
      case '\b':
        sb.append("\\b");
        break;
      case '\t':
        sb.append("\\t");
        break;
      case '\n':
        sb.append("\\n");
        break;
      case '\f':
        sb.append("\\f");
        break;
      case '\r':
        sb.append("\\r");
        break;
      default:
        if (c < ' ') {
          t = "000" + Integer.toHexString(c);
          sb.append("\\u" + t.substring(t.length() - 4));
        } else {
          sb.append(c);
        }
      }
    }
    sb.append('"');
    return sb.toString();
  }


}
