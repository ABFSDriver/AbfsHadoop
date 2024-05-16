package org.apache.hadoop.fs.azurebfs.contracts.exceptions;

public class UnsupportedAbfsOperationException extends AzureBlobFileSystemException{

  public UnsupportedAbfsOperationException(final String message) {
    super(message);
  }
}
