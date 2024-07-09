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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

/**
 * ListBlob API can give maximum of 5000 blobs. If there are (~n*5000) blobs, the
 * client would need to call the listBlob API n times. This would have two consequences:
 * <ol>
 *   <li>
 *     The consumer of the result of lists of blob would have to wait until all
 *     the blobs are received. The consumer could have used the time to start
 *     processing the blobs already in memory. The wait for receiving all the blobs
 *     would lead the processing more time. Lets say consumer need m time-units to process
 *     one blob. Lets assume that each set of blobs have x blobs. In total there
 *     have to be n sets Lets say that client needs t time to get all the blobs. If consumer
 *     wait for all the blobs to be received, the total time taken would be:
 *     <pre>t + (n * x * m)</pre>
 *     Now, lets assume that consumer in parallel work on the available the blobs,
 *     time taken would be:
 *     <pre>t + (((n * x) - t/m) * m)</pre>
 *   </li>
 *   <li>
 *     Since the information of the blobs have to be maintained in memory until the
 *     computation on the list is done. On low configuration machine, it may lead
 *     to OOM.
 *   </li>
 * </ol>
 * In this design, the producer on a parallel thread to the main thread, will call
 * ListBlob API and will populate {@link ListBlobQueue}, which would be dequeued by
 * the main thread which will run the computation on the available blobs.<br>
 *
 * How its different from {@link AbfsListStatusRemoteIterator}?<br>
 * It provides an iterator which on {@link AbfsListStatusRemoteIterator#hasNext()} checks
 * if there are blobs available in memory. If not it will call Listing API on server for
 * the next set of blobs. But here, it make the process sequential. As in, when the processing
 * on whole set of blobs available in memory are done, after that only next set of blobs are
 * fetched.
 */
public class ListBlobProducer {

  private final AbfsClient client;

  private final ListBlobQueue listBlobQueue;

  private final String src;

  private final TracingContext tracingContext;

  private String nextMarker;
  private final Thread thread;

  public ListBlobProducer(final String src,
      final AbfsClient abfsClient,
      final ListBlobQueue listBlobQueue,
      final String initNextMarker,
      TracingContext tracingContext) {
    this.src = src;
    this.client = abfsClient;
    this.tracingContext = tracingContext;
    this.listBlobQueue = listBlobQueue;
    listBlobQueue.setProducer(this);
    this.nextMarker = initNextMarker;
    thread = new Thread(() -> {
      do {
        int maxResult = listBlobQueue.availableSize();
        if (maxResult == 0) {
          continue;
        }
        AbfsRestOperation op = null;
        try {
          op = client.getListBlobs(nextMarker, src, null, maxResult, tracingContext);
        } catch (AzureBlobFileSystemException ex) {
          listBlobQueue.setFailed(ex);
          return;
        }
        BlobList blobList = op.getResult().getBlobList();
        nextMarker = blobList.getNextMarker();
        listBlobQueue.enqueue(blobList.getBlobPropertyList());
        if (nextMarker == null) {
          listBlobQueue.complete();
        }
      } while(nextMarker != null && !listBlobQueue.getConsumptionFailed());
    });
    thread.start();
  }

  @VisibleForTesting
  public void waitForProcessCompletion() throws InterruptedException {
    thread.join();
  }
}
