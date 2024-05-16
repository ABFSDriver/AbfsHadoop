package org.apache.hadoop.fs.azurebfs.services;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;

public class ListBlobQueue {
  private final Queue<Path> pathQueue = new ArrayDeque<>();
  private final int maxSize;
  private final int consumeSetSize;
  private boolean isCompleted = false;
  private boolean isConsumptionFailed = false;
  private AzureBlobFileSystemException failureFromProducer;

  public ListBlobQueue(int maxSize, int consumeSetSize) {
    this.maxSize = maxSize;
    this.consumeSetSize = consumeSetSize;
  }

  void setFailed(AzureBlobFileSystemException failure) {
    failureFromProducer = failure;
  }

  public void complete() {
    isCompleted = true;
  }

  void consumptionFailed() {
    isConsumptionFailed = true;
  }

  Boolean getConsumptionFailed() {
    return isConsumptionFailed;
  }

  public Boolean getIsCompleted() {
    return isCompleted && size() == 0;
  }

  AzureBlobFileSystemException getException() {
    return failureFromProducer;
  }

  public void enqueue(List<Path> pathList) {
    pathQueue.addAll(pathList);
  }

  public List<Path> consume() throws AzureBlobFileSystemException {
    AzureBlobFileSystemException exception = getException();
    if(exception != null) {
      throw exception;
    }
    return dequeue();
  }

  private List<Path> dequeue() {
    List<Path> pathListForConsumption = new ArrayList<>();
    int counter = 0;
    while (counter < consumeSetSize && pathQueue.size() > 0) {
      pathListForConsumption.add(pathQueue.poll());
      counter++;
    }
    return pathListForConsumption;
  }

  public int size() {
    return pathQueue.size();
  }

  public int availableSize() {
    return maxSize - size();
  }

}
