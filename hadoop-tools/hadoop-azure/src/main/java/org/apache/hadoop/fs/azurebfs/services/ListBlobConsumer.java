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

import java.util.List;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;

public class ListBlobConsumer {

  private final ListBlobQueue listBlobQueue;

  public ListBlobConsumer(final ListBlobQueue listBlobQueue) {
    this.listBlobQueue = listBlobQueue;
  }

  public List<BlobProperty> consume() throws AzureBlobFileSystemException {
    if (listBlobQueue.getException() != null) {
      throw listBlobQueue.getException();
    }
    return listBlobQueue.dequeue();
  }

  public Boolean isCompleted() {
    return listBlobQueue.getIsCompleted()
        && listBlobQueue.size() == 0;
  }

  /**
   * Register consumer failure.
   */
  public void fail() {
    listBlobQueue.consumptionFailed();
  }
}
