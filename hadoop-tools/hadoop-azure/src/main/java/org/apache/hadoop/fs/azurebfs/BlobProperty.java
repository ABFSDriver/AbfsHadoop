package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;

public class BlobProperty {
  private Boolean isDirectory = false;
  private Path path;
  private Boolean exist = false;
  private String url;

  private String copySourceUrl;
  private String copyId;
  private String copyStatus;
  private String statusDescription;
  private int contentLength = 0;

  private AzureBlobFileSystemException ex;

  BlobProperty() {

  }

  void setUrl(String url) {
    this.url = url;
  }

  void setIsDirectory(Boolean isDirectory) {
    this.isDirectory = isDirectory;
  }

  void setCopyId(String copyId) {
    this.copyId = copyId;
  }

  void setExist(Boolean exist) {
    this.exist = exist;
  }

  void setCopySourceUrl(String copySourceUrl) {
    this.copySourceUrl = copySourceUrl;
  }

  void setPath(Path path) {
    this.path = path;
  }

  void setCopyStatus(String copyStatus) {
    this.copyStatus = copyStatus;
  }

  void setStatusDescription(String statusDescription) {
    this.statusDescription = statusDescription;
  }

  void setContentLength(int length) {
    this.contentLength = length;
  }


  public Boolean getIsDirectory() {
    return isDirectory;
  }

  public Boolean exists() {
    return exist;
  }

  public AzureBlobFileSystemException getFailureException() {
    return ex;
  }

  public Path getPath() {
    return path;
  }

  public Path getBlobDstPath(Path dstBlobPath) {
    return null;
  }

  public String getUrl() {
    return url;
  }

  public String getCopySourceUrl() {
    return copySourceUrl;
  }

  public String getCopyId() {
    return copyId;
  }

  public String getCopyStatus() {
    return copyStatus;
  }

  public String getStatusDescription() {
    return statusDescription;
  }

  public int getContentLength() {
    return contentLength;
  }
}