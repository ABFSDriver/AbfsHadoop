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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListResultSchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;

/**
 * ListActionTaker is an abstract class that provides a way to list the paths
 * recursively and take action on each path. The implementations of this class
 * should provide the action to be taken on each listed path.
 */
public abstract class ListActionTaker {

  protected final Path path;

  protected final AbfsBlobClient abfsClient;

  protected final TracingContext tracingContext;

  private final ExecutorService executorService;

  private final int maxConsumptionParallelism;

  private final AtomicBoolean producerThreadToBeStopped = new AtomicBoolean(
      false);

  public ListActionTaker(Path path,
      AbfsClient abfsClient,
      int maxConsumptionParallelism,
      TracingContext tracingContext) {
    this.path = path;
    this.abfsClient = (AbfsBlobClient) abfsClient;
    this.tracingContext = tracingContext;
    this.maxConsumptionParallelism = maxConsumptionParallelism;
    executorService = Executors.newFixedThreadPool(maxConsumptionParallelism);
  }

  abstract boolean takeAction(Path path) throws IOException;

  private boolean takeAction(List<Path> paths) throws IOException {
    List<Future<Boolean>> futureList = new ArrayList<>();
    for (Path path : paths) {
      Future<Boolean> future = executorService.submit(() -> {
        return takeAction(path);
      });
      futureList.add(future);
    }

    IOException executionException = null;
    boolean actionResult = true;
    for (Future<Boolean> future : futureList) {
      try {
        Boolean result = future.get();
        if (!result) {
          actionResult = false;
        }
      } catch (InterruptedException ignored) {

      } catch (ExecutionException e) {
        executionException = (IOException) e.getCause();
      }
    }
    if (executionException != null) {
      throw executionException;
    }
    return actionResult;
  }

  /**
   * Spawns a producer thread that list the children of the path recursively and queue
   * them in into {@link ListBlobQueue}. On the main thread, it dequeues the
   * path and supply them to parallel thread for relevant action which is defined
   * in {@link #takeAction(Path)}.
   */
  public boolean listRecursiveAndTakeAction() throws IOException {
    AbfsConfiguration configuration = abfsClient.getAbfsConfiguration();
    Thread producerThread = null;
    try {
      ListBlobQueue listBlobQueue = new ListBlobQueue(
          configuration.getProducerQueueMaxSize(), maxConsumptionParallelism);
      producerThread = new Thread(() -> {
        try {
          produceConsumableList(listBlobQueue);
        } catch (AzureBlobFileSystemException e) {
          listBlobQueue.markProducerFailure(e);
        }
      });
      producerThread.start();

      while (!listBlobQueue.getIsCompleted()) {
        List<Path> paths = listBlobQueue.consume();
        if (paths == null) {
          continue;
        }
        try {
          boolean resultOnPartAction = takeAction(paths);
          if (!resultOnPartAction) {
            return false;
          }
        } catch (IOException parallelConsumptionException) {
          listBlobQueue.markConsumptionFailed();
          throw parallelConsumptionException;
        }
      }
      return true;
    } finally {
      if (producerThread != null) {
        producerThreadToBeStopped.set(true);
      }
      executorService.shutdown();
    }
  }

  private void produceConsumableList(final ListBlobQueue listBlobQueue)
      throws AzureBlobFileSystemException {
    String continuationToken = null;
    do {
      List<Path> paths = new ArrayList<>();
      AbfsRestOperation op = null;
      final int queueAvailableSize = listBlobQueue.availableSize();
      if (queueAvailableSize <= 0) {
        continue;
      }
      op = abfsClient.listPath(path.toUri().getPath(), true,
          queueAvailableSize, continuationToken,
          tracingContext);

      ListResultSchema retrievedSchema = op.getResult().getListResultSchema();
      if (retrievedSchema == null) {
        continue;
      }
      continuationToken
          = ((BlobListResultSchema) retrievedSchema).getNextMarker();
      for (ListResultEntrySchema entry : retrievedSchema.paths()) {
        Path entryPath = new Path(ROOT_PATH, entry.name());
        if (!entryPath.equals(this.path)) {
          paths.add(entryPath);
        }
      }
      listBlobQueue.enqueue(paths);
    } while (!producerThreadToBeStopped.get() && continuationToken != null
        && !listBlobQueue.getConsumptionFailed());
    listBlobQueue.complete();
  }
}
