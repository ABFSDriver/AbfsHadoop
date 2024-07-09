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
import java.io.OutputStream;
import java.net.HttpURLConnection;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

/**
 * For a directory enabled for atomic-rename, before rename starts, a
 * file with -RenamePending.json suffix is created. In this file, the states required
 * for the rename are given. This file is created by {@link #preRename(Boolean, String)} ()} method.
 * This is important in case the JVM process crashes during rename, the atomicity
 * will be maintained, when the job calls {@link AzureBlobFileSystem#listStatus(Path)}
 * or {@link AzureBlobFileSystem#getFileStatus(Path)}. On these API calls to filesystem,
 * it will be checked if there is any RenamePending JSON file. If yes, the rename
 * would be resumed as per the file.
 */
public class RenameAtomicityUtils {

  private static final Logger LOG = LoggerFactory.getLogger(
      RenameAtomicityUtils.class);

  private final AzureBlobFileSystem azureBlobFileSystem;
  private Path srcPath;
  private Path dstPath;
  private TracingContext tracingContext;
  private Boolean isReDone;

  private static final int MAX_RENAME_PENDING_FILE_SIZE = 10000000;
  private static final int FORMATTING_BUFFER = 10000;

  public static final String SUFFIX = "-RenamePending.json";

  private static final ObjectReader READER = new ObjectMapper()
      .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
      .readerFor(JsonNode.class);

  public RenameAtomicityUtils(final AzureBlobFileSystem azureBlobFileSystem,
      final Path srcPath,
      final Path dstPath,
      final TracingContext tracingContext) throws IOException {
    this.azureBlobFileSystem = azureBlobFileSystem;
    this.srcPath = srcPath;
    this.dstPath = dstPath;
    this.tracingContext = tracingContext;
  }

  public RenameAtomicityUtils(final AzureBlobFileSystem azureBlobFileSystem,
      final Path renamePendingJsonPath,
      final RedoRenameInvocation redoRenameInvocation,
      final String srcEtag,
      final AbfsInputStream renamePendingJsonInputStream)
      throws IOException {
    this.azureBlobFileSystem = azureBlobFileSystem;
    final RenamePendingFileInfo renamePendingFileInfo = readFile(
        renamePendingJsonPath, renamePendingJsonInputStream);
    if (renamePendingFileInfo != null
        && renamePendingFileInfo.eTag.equalsIgnoreCase(srcEtag)) {
      redoRenameInvocation.redo(renamePendingFileInfo.destination,
          renamePendingFileInfo.src);
      isReDone = true;
    } else {
      isReDone = false;
    }
  }

  private RenamePendingFileInfo readFile(final Path redoFile,
      final AbfsInputStream redoFileInputStream)
      throws IOException {
    Path f = redoFile;
    byte[] bytes = new byte[MAX_RENAME_PENDING_FILE_SIZE];
    int l = redoFileInputStream.read(bytes);
    if (l <= 0) {
      // Jira HADOOP-12678 -Handle empty rename pending metadata file during
      // atomic rename in redo path. If during renamepending file is created
      // but not written yet, then this means that rename operation
      // has not started yet. So we should delete rename pending metadata file.
      LOG.error("Deleting empty rename pending file "
          + redoFile + " -- no data available");
      deleteRenamePendingFile(azureBlobFileSystem, redoFile);
      return null;
    }
    if (l == MAX_RENAME_PENDING_FILE_SIZE) {
      throw new IOException(
          "Error reading pending rename file contents -- "
              + "maximum file size exceeded");
    }
    String contents = new String(bytes, 0, l, StandardCharsets.UTF_8);

    // parse the JSON
    JsonNode json = null;
    boolean committed;
    try {
      json = READER.readValue(contents);
      committed = true;
    } catch (JsonMappingException e) {

      // The -RedoPending.json file is corrupted, so we assume it was
      // not completely written
      // and the redo operation did not commit.
      committed = false;
    } catch (JsonParseException e) {
      committed = false;
    }

    if (!committed) {
      LOG.error("Deleting corruped rename pending file {} \n {}",
          redoFile, contents);

      // delete the -RenamePending.json file
      deleteRenamePendingFile(azureBlobFileSystem, redoFile);
      return null;
    }

    // initialize this object's fields
    JsonNode oldFolderName = json.get("OldFolderName");
    JsonNode newFolderName = json.get("NewFolderName");
    JsonNode eTag = json.get("ETag");

    if (oldFolderName != null && StringUtils.isNotEmpty(
        oldFolderName.textValue())
        && newFolderName != null && StringUtils.isNotEmpty(
        newFolderName.textValue()) && eTag != null && StringUtils.isNotEmpty(
        eTag.textValue())) {
      RenamePendingFileInfo renamePendingFileInfo = new RenamePendingFileInfo();
      renamePendingFileInfo.destination = new Path(newFolderName.textValue());
      renamePendingFileInfo.src = new Path(oldFolderName.textValue());
      renamePendingFileInfo.eTag = eTag.textValue();
      return renamePendingFileInfo;
    }
    return null;
  }

  private void deleteRenamePendingFile(FileSystem fs, Path redoFile)
      throws IOException {
    try {
      fs.delete(redoFile, false);
    } catch (IOException e) {
      // If the rename metadata was not found then somebody probably
      // raced with us and finished the delete first
      if (e instanceof FileNotFoundException) {
        LOG.warn("rename pending file " + redoFile + " is already deleted");
      } else {
        throw e;
      }
    }
  }

  /**
   * Write to disk the information needed to redo folder rename,
   * in JSON format. The file name will be
   * {@code abfs://<sourceFolderPrefix>/folderName-RenamePending.json}
   * The file format will be:
   * <pre>{@code
   * {
   *   FormatVersion: "1.0",
   *   OperationTime: "<YYYY-MM-DD HH:MM:SS.MMM>",
   *   OldFolderName: "<key>",
   *   NewFolderName: "<key>"
   *   ETag: "<etag of the src-directory>"
   * }
   *
   * Here's a sample:
   * {
   *  FormatVersion: "1.0",
   *  OperationUTCTime: "2014-07-01 23:50:35.572",
   *  OldFolderName: "user/ehans/folderToRename",
   *  NewFolderName: "user/ehans/renamedFolder"
   *  ETag: "ETag"
   * } }</pre>
   * @throws IOException Thrown when fail to write file.
   */
  public void preRename(final Boolean isCreateOperationOnBlobEndpoint,
      final String eTag) throws IOException {
    Path path = getRenamePendingFilePath();
    LOG.debug("Preparing to write atomic rename state to {}", path.toString());
    OutputStream output = null;

    String contents = makeRenamePendingFileContents(eTag);

    // Write file.
    try {
      output = azureBlobFileSystem.create(path, false);
      output.write(contents.getBytes(Charset.forName("UTF-8")));
      output.flush();
      output.close();
    } catch (IOException e) {
      /*
       * Scenario: file has been deleted by parallel thread before the RenameJSON
       * could be written and flushed.
       * ref: https://issues.apache.org/jira/browse/HADOOP-12678
       * On DFS endpoint, flush API is called. If file is not there, server returns
       * 404.
       * On blob endpoint, flush API is not there. PutBlockList is called with
       * if-match header. If file is not there, the conditional header will fail,
       * the server will return 412.
       */
      if ((!isCreateOperationOnBlobEndpoint
          && e instanceof FileNotFoundException) || (
          isCreateOperationOnBlobEndpoint && getWrappedException(
              e) instanceof AbfsRestOperationException &&
              ((AbfsRestOperationException) getWrappedException(
                  e)).getStatusCode()
                  == HttpURLConnection.HTTP_PRECON_FAILED)) {
        /*
         * In case listStatus done on directory before any content could be written,
         * that particular thread running on some worker-node of the cluster would
         * delete the RenamePending JSON file.
         * ref: https://issues.apache.org/jira/browse/HADOOP-12678.
         * To recover from parallel delete, we will give it a second try.
         */
        output = azureBlobFileSystem.create(path, false);
        output.write(contents.getBytes(Charset.forName("UTF-8")));
        output.flush();
        output.close();
        return;
      }
      throw new IOException(
          "Unable to write RenamePending file for folder rename from "
              + srcPath.toUri().getPath() + " to " + dstPath.toUri().getPath(),
          e);
    }
  }

  private Throwable getWrappedException(final IOException e) {
    if (e.getCause() != null) {
      return e.getCause().getCause();
    }
    return e;
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
    if(!eTag.startsWith("\"") && !eTag.endsWith("\"")) {
      eTag = quote(eTag);
    }

    // Make file contents as a string. Again, quote file names, escaping
    // characters as appropriate.
    String contents = "{\n"
        + "  FormatVersion: \"1.0\",\n"
        + "  OperationUTCTime: \"" + time + "\",\n"
        + "  OldFolderName: " + quote(srcPath.toUri().getPath()) + ",\n"
        + "  NewFolderName: " + quote(dstPath.toUri().getPath()) + ",\n"
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

  /** Clean up after execution of rename.
   * @throws IOException Thrown when fail to clean up.
   * */
  public void cleanup() throws IOException {

    // Remove RenamePending file
    azureBlobFileSystem.delete(getRenamePendingFilePath(), false);

    // Freeing source folder lease is not necessary since the source
    // folder file was deleted.
  }

  public void cleanup(Path redoFile) throws IOException {
    azureBlobFileSystem.delete(redoFile, false);
  }

  private Path getRenamePendingFilePath() {
    String fileName = srcPath.toUri().getPath() + SUFFIX;
    Path fileNamePath = new Path(fileName);
    return fileNamePath;
  }

  private static class RenamePendingFileInfo {
    public Path destination;
    public Path src;
    public String eTag;
  }

  public static interface RedoRenameInvocation {
    void redo(Path destination, Path src) throws
        AzureBlobFileSystemException;
  }

  public Boolean isRedone() {
    return isReDone;
  }
}
