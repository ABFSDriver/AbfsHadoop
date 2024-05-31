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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;

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

  private final AzureBlobFileSystem.GetCreateCallback
      renameAtomicityCreateCallback;

  private final AzureBlobFileSystem.GetReadCallback renameAtomicityReadCallback;

  private final TracingContext tracingContext;

  private Path src, dst;

  private String srcEtag;

  private final AbfsBlobClient abfsClient;

  private final Path renameJsonPath;

  public static final String SUFFIX = "-RenamePending.json";

  private int preRenameRetryCount = 0;

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
   * @param renameAtomicityCreateCallback Callback to create the JSON file
   * @param renameAtomicityReadCallback Callback to read the JSON file
   * @param tracingContext Tracing context
   * @param srcEtag ETag of the source directory
   * @param abfsClient AbfsClient instance
   * @throws IOException Server error while creating / writing json file.
   */
  public RenameAtomicity(final Path src, final Path dst,
      final Path renameJsonPath,
      final AzureBlobFileSystem.GetCreateCallback renameAtomicityCreateCallback,
      final AzureBlobFileSystem.GetReadCallback renameAtomicityReadCallback,
      TracingContext tracingContext,
      final String srcEtag,
      final AbfsClient abfsClient) throws IOException {
    this.src = src;
    this.dst = dst;
    this.abfsClient = (AbfsBlobClient) abfsClient;
    this.renameJsonPath = renameJsonPath;
    this.renameAtomicityCreateCallback = renameAtomicityCreateCallback;
    this.renameAtomicityReadCallback = renameAtomicityReadCallback;
    this.tracingContext = tracingContext;
    this.srcEtag = srcEtag;
    preRename();
  }

  /**
   * Resumes the rename operation from the JSON file.
   *
   * @param renameJsonPath Path of the JSON file
   * @param renameAtomicityCreateCallback Callback to create the JSON file
   * @param renameAtomicityReadCallback Callback to read the JSON file
   * @param tracingContext Tracing context
   * @param srcEtag ETag of the source directory
   * @param abfsClient AbfsClient instance
   * @throws IOException Server error while reading / deleting json file.
   */
  public RenameAtomicity(final Path renameJsonPath,
      final AzureBlobFileSystem.GetCreateCallback renameAtomicityCreateCallback,
      final AzureBlobFileSystem.GetReadCallback renameAtomicityReadCallback,
      TracingContext tracingContext,
      final String srcEtag,
      final AbfsClient abfsClient) throws IOException {
    this.abfsClient = (AbfsBlobClient) abfsClient;
    this.renameJsonPath = renameJsonPath;
    this.renameAtomicityCreateCallback = renameAtomicityCreateCallback;
    this.renameAtomicityReadCallback = renameAtomicityReadCallback;
    this.tracingContext = tracingContext;
    this.srcEtag = srcEtag;
    redo();
  }

  private void redo() throws IOException {
    try (FSDataInputStream is = renameAtomicityReadCallback.get(
        renameJsonPath, tracingContext)) {
      // parse the JSON
      byte[] buffer = new byte[is.available()];
      is.readFully(0, buffer);
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
  }

  @VisibleForTesting
  public void preRename() throws IOException {
    String makeRenamePendingFileContents = makeRenamePendingFileContents(
        srcEtag);
    try (FSDataOutputStream os = renameAtomicityCreateCallback.createFile(
        renameJsonPath, tracingContext)) {
      os.write(makeRenamePendingFileContents.getBytes(StandardCharsets.UTF_8));
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
      if (e instanceof FileNotFoundException
          || (e instanceof AbfsRestOperationException
          && isPreRenameRetriableException((AbfsRestOperationException) e))) {
        preRenameRetryCount++;
        if (preRenameRetryCount == 1) {
          preRename();
          return;
        }
      }
      throw e;
    }
  }

  private boolean isPreRenameRetriableException(final AbfsRestOperationException e) {
    return e.getStatusCode() == HTTP_NOT_FOUND
        || e.getStatusCode() == HTTP_PRECON_FAILED;
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
  private String makeRenamePendingFileContents(String eTag) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    String time = sdf.format(new Date());
    if (StringUtils.isNotEmpty(eTag) && !eTag.startsWith("\"")
        && !eTag.endsWith("\"")) {
      eTag = quote(eTag);
    }

    // Make file contents as a string. Again, quote file names, escaping
    // characters as appropriate.
    String contents = "{\n"
        + "  FormatVersion: \"1.0\",\n"
        + "  OperationUTCTime: \"" + time + "\",\n"
        + "  OldFolderName: " + quote(src.toUri().getPath()) + ",\n"
        + "  NewFolderName: " + quote(dst.toUri().getPath()) + ",\n"
        + "  ETag: " + eTag + "\n"
        + "}\n";

    return contents;
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
