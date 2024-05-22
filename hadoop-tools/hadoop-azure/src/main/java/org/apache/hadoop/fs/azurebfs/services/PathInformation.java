package org.apache.hadoop.fs.azurebfs.services;

public class PathInformation {
  private Boolean pathExists;
  private Boolean isDirectory;
  private String eTag;

  public PathInformation(Boolean pathExists, Boolean isDirectory, String eTag) {
    this.pathExists = pathExists;
    this.isDirectory = isDirectory;
    this.eTag = eTag;
  }

  public PathInformation() {
  }

  public void copy(PathInformation pathInformation) {
    this.pathExists = pathInformation.getPathExists();
    this.isDirectory = pathInformation.getIsDirectory();
    this.eTag = pathInformation.getETag();
  }

  public String getETag() {
    return eTag;
  }

  public Boolean getPathExists() {
    return pathExists;
  }

  public Boolean getIsDirectory() {
    return isDirectory;
  }
}
