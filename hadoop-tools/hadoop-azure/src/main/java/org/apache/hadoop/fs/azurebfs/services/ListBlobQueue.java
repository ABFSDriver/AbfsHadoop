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
