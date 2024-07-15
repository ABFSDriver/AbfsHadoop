/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;

public class AbfsListStatusRemoteIterator
    implements RemoteIterator<String> {

  private static final Logger LOG = LoggerFactory
      .getLogger(AbfsListStatusRemoteIterator.class);

  private static final boolean FETCH_ALL_FALSE = false;
  private static final int MAX_QUEUE_SIZE = 10;
  private static final long POLL_WAIT_TIME_IN_MS = 250;

  private final Path path;
  private ListingSupport listingSupport;
  private  AbfsClient abfsClient;
  private final ArrayBlockingQueue<AbfsListResult> listResultQueue;
  private final TracingContext tracingContext;

  private volatile boolean isAsyncInProgress = false;
  private boolean isIterationComplete = false;
  private String continuation;
  private Iterator<String> currIterator;

  public AbfsListStatusRemoteIterator(final Path path, ListingSupport listingSupport, TracingContext tracingContext)
      throws IOException {
    this.path = path;
    this.tracingContext = tracingContext;
    listResultQueue = new ArrayBlockingQueue<>(MAX_QUEUE_SIZE);
    currIterator = Collections.emptyIterator();
    this.listingSupport = listingSupport;
    addNextBatchIteratorToQueue();
    fetchBatchesAsync();
  }

  public AbfsListStatusRemoteIterator(final Path path, AbfsClient abfsClient, TracingContext tracingContext)
      throws IOException {
    this.path = path;
    this.tracingContext = tracingContext;
    listResultQueue = new ArrayBlockingQueue<>(MAX_QUEUE_SIZE);
    currIterator = Collections.emptyIterator();
    this.abfsClient = abfsClient;
    addNextBatchIteratorToQueue();
    fetchBatchesAsync();
  }

  @Override
  public boolean hasNext() throws IOException {
    if (currIterator.hasNext()) {
      return true;
    }
    currIterator = getNextIterator();
    return currIterator.hasNext();
  }

  @Override
  public String next() throws IOException {
    if (!this.hasNext()) {
      throw new NoSuchElementException();
    }
    return currIterator.next();
  }

  private Iterator<String> getNextIterator() throws IOException {
    fetchBatchesAsync();
    try {
      AbfsListResult listResult = null;
      while (listResult == null
          && (!isIterationComplete || !listResultQueue.isEmpty())) {
        listResult = listResultQueue.poll(POLL_WAIT_TIME_IN_MS, TimeUnit.MILLISECONDS);
      }
      if (listResult == null) {
        return Collections.emptyIterator();
      } else if (listResult.isFailedListing()) {
        throw listResult.getListingException();
      } else {
        return listResult.getFileStatusIterator();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Thread got interrupted: {}", e);
      throw new IOException(e);
    }
  }

  private void fetchBatchesAsync() {
    if (isAsyncInProgress || isIterationComplete) {
      return;
    }
    synchronized (this) {
      if (isAsyncInProgress || isIterationComplete) {
        return;
      }
      isAsyncInProgress = true;
    }
    CompletableFuture.runAsync(() -> asyncOp());
  }

  private void asyncOp() {
    try {
      while (!isIterationComplete && listResultQueue.size() <= MAX_QUEUE_SIZE) {
        addNextBatchIteratorToQueue();
      }
    } catch (IOException ioe) {
      LOG.error("Fetching filestatuses failed", ioe);
      try {
        listResultQueue.put(new AbfsListResult(ioe));
      } catch (InterruptedException interruptedException) {
        Thread.currentThread().interrupt();
        LOG.error("Thread got interrupted: {}", interruptedException);
      }
    } finally {
      synchronized (this) {
        isAsyncInProgress = false;
      }
    }
  }

  private synchronized void addNextBatchIteratorToQueue()
      throws IOException {
    List<String> fileStatuses = new ArrayList<>();
    try {
      try {
        AbfsRestOperation op = abfsClient.listPath(path.toUri().getPath(),
            true, 5000, continuation, tracingContext);
        continuation = abfsClient.getContinuationFromResponse(op.getResult());
        ListResultSchema listResultSchema = op.getResult().getListResultSchema();
        for(ListResultEntrySchema entry : listResultSchema.paths()) {
          fileStatuses.add(ROOT_PATH + entry.name());
        }

      } catch (AbfsRestOperationException ex) {
        AzureBlobFileSystem.checkException(path, ex);
      }
      if (!fileStatuses.isEmpty()) {
        listResultQueue.put(new AbfsListResult(fileStatuses.iterator()));
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      LOG.error("Thread interrupted", ie);
    }
    if (continuation == null || continuation.isEmpty()) {
      isIterationComplete = true;
    }
  }

}
