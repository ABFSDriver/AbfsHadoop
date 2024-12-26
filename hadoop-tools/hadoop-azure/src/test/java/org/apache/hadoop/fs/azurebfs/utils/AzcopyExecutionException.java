package org.apache.hadoop.fs.azurebfs.utils;

import java.io.IOException;

public class AzcopyExecutionException extends IOException {
  private static final String SUGGESTION = "Try deleting the following azcopy tool directory and rerun tests: ";

  public AzcopyExecutionException(String message, String azcopyPath) {
    super(message + SUGGESTION + azcopyPath);
  }

  public AzcopyExecutionException(String message, String azcopyPath, Throwable cause) {
    super(message + SUGGESTION + azcopyPath, cause);
  }
}
