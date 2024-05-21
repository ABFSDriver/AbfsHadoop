package org.apache.hadoop.fs.azurebfs.services;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
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
import com.jcraft.jsch.IO;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

public class RenameAtomicity {

  private final AzureBlobFileSystem.GetCreateCallback
      renameAtomicityCreateCallback;

  private final AzureBlobFileSystem.GetReadCallback renameAtomicityReadCallback;

  private final TracingContext tracingContext;

  private Path src, dst;

  private String srcEtag;

  private final AbfsBlobClient abfsClient;

  private final Boolean isNamespaceEnabled;

  private final Path renameJsonPath;

  public static final String SUFFIX = "-RenamePending.json";

  private int prenameRetryCount = 0;

  private static final ObjectReader READER = new ObjectMapper()
      .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
      .readerFor(JsonNode.class);

  public RenameAtomicity(final Path src, final Path dst,
      final Path renameJsonPath,
      final AzureBlobFileSystem.GetCreateCallback renameAtomicityCreateCallback,
      final AzureBlobFileSystem.GetReadCallback renameAtomicityReadCallback,
      TracingContext tracingContext, Boolean isNamespaceEnabled,
      final String srcEtag,
      final AbfsClient abfsClient) throws IOException {
    this.src = src;
    this.dst = dst;
    this.abfsClient = (AbfsBlobClient) abfsClient;
    this.renameJsonPath = renameJsonPath;
    this.renameAtomicityCreateCallback = renameAtomicityCreateCallback;
    this.renameAtomicityReadCallback = renameAtomicityReadCallback;
    this.tracingContext = tracingContext;
    this.isNamespaceEnabled = isNamespaceEnabled;
    this.srcEtag = srcEtag;
  }

  public RenameAtomicity(final Path renameJsonPath,
      final AzureBlobFileSystem.GetCreateCallback renameAtomicityCreateCallback,
      final AzureBlobFileSystem.GetReadCallback renameAtomicityReadCallback,
      TracingContext tracingContext, Boolean isNamespaceEnabled,
      final String srcEtag,
      final AbfsClient abfsClient) throws IOException {
    this.abfsClient = (AbfsBlobClient) abfsClient;
    this.renameJsonPath = renameJsonPath;
    this.renameAtomicityCreateCallback = renameAtomicityCreateCallback;
    this.renameAtomicityReadCallback = renameAtomicityReadCallback;
    this.tracingContext = tracingContext;
    this.isNamespaceEnabled = isNamespaceEnabled;
    this.srcEtag = srcEtag;
  }

  public void redo() throws IOException {
    final Path src;
    try (FSDataInputStream is = renameAtomicityReadCallback.get(
        renameJsonPath)) {
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

          BlobRenameHandler blobRenameHandler = new BlobRenameHandler(this.src.toUri().getPath(), dst.toUri().getPath(),
              abfsClient, srcEtag, true, true, tracingContext);
          try {
            blobRenameHandler.execute();
          } catch (AbfsRestOperationException e) {
            if (e.getStatusCode() == HTTP_NOT_FOUND || e.getStatusCode() == HTTP_CONFLICT) {
              return;
            }
            throw e;
          }
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

  public void preRename() throws IOException {
    String makeRenamePendingFileContents = makeRenamePendingFileContents(
        srcEtag);
    try (OutputStream os = renameAtomicityCreateCallback.get(renameJsonPath)) {
      os.write(makeRenamePendingFileContents.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      if(e instanceof FileNotFoundException || (e instanceof AbfsRestOperationException
          && ((AbfsRestOperationException) e).getStatusCode() == HTTP_NOT_FOUND)) {
        prenameRetryCount++;
        if(prenameRetryCount == 1) {
          preRename();
          return;
        }
      }
      throw e;
    }
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
    if (StringUtils.isNotEmpty(eTag) && !eTag.startsWith("\"") && !eTag.endsWith("\"")) {
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
