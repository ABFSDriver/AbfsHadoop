package org.apache.hadoop.fs.azurebfs.services;

public class PathInformation {
  private Boolean pathExists;
  private Boolean isDirectory;

  public PathInformation(Boolean pathExists, Boolean isDirectory) {
    this.pathExists = pathExists;
    this.isDirectory = isDirectory;
  }

  public Boolean getPathExists() {
    return pathExists;
  }

  public Boolean getIsDirectory() {
    return isDirectory;
  }
}
