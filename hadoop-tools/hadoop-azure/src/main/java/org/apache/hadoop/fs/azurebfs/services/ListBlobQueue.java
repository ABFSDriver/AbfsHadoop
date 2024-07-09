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

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;

public class ListBlobQueue {

  private final Queue<BlobProperty> blobLists;

  private int totalProduced = 0;

  private int totalConsumed = 0;

  private Boolean isCompleted = false;
  private Boolean isConsumptionFailed = false;

  private AzureBlobFileSystemException failureFromProducer;

  /**
   * Since, Producer just spawns a thread and there are no public method for the
   * class. Keeping its address in this object will prevent accidental GC close
   * on the producer object.
   */
  private ListBlobProducer producer;

  private final int maxSize;
  private final int maxConsumedBlobCount;

  /**
   * @param maxSize maxSize of the queue.
   * @param maxConsumedBlobCount maximum number of blobs that would be returned
   * by {@link #dequeue()} method.
   */
  public ListBlobQueue(int maxSize, int maxConsumedBlobCount) {
    blobLists = new ArrayDeque<>(maxSize);
    this.maxSize = maxSize;
    this.maxConsumedBlobCount = maxConsumedBlobCount;
  }

  /**
   * @param initBlobList list of blobProperties to be enqueued in th queue
   * @param maxSize maxSize of the queue.
   * @param maxConsumedBlobCount maximum number of blobs that would be returned
   * by {@link #dequeue()} method.
   */
  public ListBlobQueue(List<BlobProperty> initBlobList, int maxSize, int maxConsumedBlobCount) {
    this(maxSize, maxConsumedBlobCount);
    if (initBlobList != null) {
      enqueue(initBlobList);
    }
  }

  void setProducer(ListBlobProducer producer) {
    if (this.producer == null) {
      this.producer = producer;
    }
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
    return isCompleted;
  }

  AzureBlobFileSystemException getException() {
    return failureFromProducer;
  }

  public void enqueue(List<BlobProperty> blobProperties) {
    blobLists.addAll(blobProperties);
  }

  public List<BlobProperty> dequeue() {
    List<BlobProperty> blobProperties = new ArrayList<>();
    int counter = 0;
    while (counter < maxConsumedBlobCount && blobLists.size() > 0) {
      blobProperties.add(blobLists.poll());
      counter++;
    }
    return blobProperties;
  }

  public int size() {
    return blobLists.size();
  }

  public int availableSize() {
    return maxSize - blobLists.size();
  }
}
