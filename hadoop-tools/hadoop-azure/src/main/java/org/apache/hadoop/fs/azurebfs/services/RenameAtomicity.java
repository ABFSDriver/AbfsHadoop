package org.apache.hadoop.fs.azurebfs.services;

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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class RenameAtomicity {

  private final AzureBlobFileSystem.GetRenameAtomicityCreateCallback
      renameAtomicityCreateCallback;

  private final AzureBlobFileSystem.GetRenameAtomicityReadCallback
      renameAtomicityReadCallback;

  private final TracingContext tracingContext;

  private final Path src, dst;

  private final String srcEtag;

  private final AbfsClient abfsClient;

  private final Boolean isNamespaceEnabled;

  private final Path renameJsonPath;

  public static final String SUFFIX = "-RenamePending.json";

  private static final ObjectReader READER = new ObjectMapper()
      .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
      .readerFor(JsonNode.class);

  public RenameAtomicity(final Path src, final Path dst,
      final AzureBlobFileSystem.GetRenameAtomicityCreateCallback renameAtomicityCreateCallback,
      final AzureBlobFileSystem.GetRenameAtomicityReadCallback renameAtomicityReadCallback,
      final TracingContext tracingContext,
      final String srcEtag,
      final Boolean isNamespaceEnabled,
      final AbfsClient abfsClient) {
    this.src = src;
    this.dst = dst;
    this.renameAtomicityCreateCallback = renameAtomicityCreateCallback;
    this.renameAtomicityReadCallback = renameAtomicityReadCallback;
    this.tracingContext = tracingContext;
    this.srcEtag = srcEtag;
    this.isNamespaceEnabled = isNamespaceEnabled;
    this.abfsClient = abfsClient;
    renameJsonPath = new Path(src.getParent(), src.getName() + SUFFIX);
  }

  public RenameAtomicity(final Path renameJsonPath,
      final AzureBlobFileSystem.GetRenameAtomicityCreateCallback renameAtomicityCreateCallback,
      final AzureBlobFileSystem.GetRenameAtomicityReadCallback renameAtomicityReadCallback,
      TracingContext tracingContext, Boolean isNamespaceEnabled,
      final AzureBlobFileSystemStore.GetFileStatusImpl getFileStatusImpl,
      final AbfsClient abfsClient) throws IOException {
    this.abfsClient = abfsClient;
    this.renameJsonPath = renameJsonPath;
    this.renameAtomicityCreateCallback = renameAtomicityCreateCallback;
    this.renameAtomicityReadCallback = renameAtomicityReadCallback;
    this.tracingContext = tracingContext;
    this.isNamespaceEnabled = isNamespaceEnabled;

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

          BlobRenameHandler blobRenameHandler = new BlobRenameHandler(src.toUri().getPath(), dst.toUri().getPath(),
              abfsClient, srcEtag, tracingContext);
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

  public void preRename() throws IOException {
    String makeRenamePendingFileContents = makeRenamePendingFileContents(
        srcEtag);
    try (OutputStream os = renameAtomicityCreateCallback.get(renameJsonPath)) {
      os.write(makeRenamePendingFileContents.getBytes(StandardCharsets.UTF_8));
    }
  }

  public void postRename() throws IOException {
    deleteRenamePendingJson();
  }

  private void deleteRenamePendingJson() throws AzureBlobFileSystemException {
    try {
      abfsClient.deletePath(renameJsonPath.toUri().getPath(), false, null,
          tracingContext, isNamespaceEnabled);
    } catch (IOException e) {
      throw new RuntimeException(e);
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
    if (!eTag.startsWith("\"") && !eTag.endsWith("\"")) {
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
