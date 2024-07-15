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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsDriverException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListResultSchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.util.functional.FutureIO;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_AZURE_LIST_MAX_RESULTS;

/**
 * ListActionTaker is an abstract class that provides a way to list the paths
 * recursively and take action on each path. The implementations of this class
 * should provide the action to be taken on each listed path.
 */
public abstract class ListActionTaker {

  private static final Logger LOG = LoggerFactory.getLogger(ListActionTaker.class);

  protected final Path path;

  protected final AbfsBlobClient abfsClient;

  protected final TracingContext tracingContext;

  private final ExecutorService executorService;

  private final AtomicBoolean producerThreadToBeStopped = new AtomicBoolean(
      false);

  public ListActionTaker(Path path,
      AbfsBlobClient abfsClient,
      TracingContext tracingContext) {
    this.path = path;
    this.abfsClient = abfsClient;
    this.tracingContext = tracingContext;
    executorService = Executors.newFixedThreadPool(
        getMaxConsumptionParallelism());
  }

  abstract int getMaxConsumptionParallelism();

  abstract boolean takeAction(Path path) throws AzureBlobFileSystemException;

  private boolean takeAction(List<Path> paths) throws AzureBlobFileSystemException {
    List<Future<Boolean>> futureList = new ArrayList<>();
    for (Path path : paths) {
      Future<Boolean> future = executorService.submit(() -> {
        return takeAction(path);
      });
      futureList.add(future);
    }

    AzureBlobFileSystemException executionException = null;
    boolean actionResult = true;
    for (Future<Boolean> future : futureList) {
      try {
        Boolean result = future.get();
        if (!result) {
          actionResult = false;
        }
      } catch (InterruptedException e) {
        LOG.debug("Thread interrupted while taking action on path: {}",
            path.toUri().getPath());
      } catch (ExecutionException e) {
        executionException = (AzureBlobFileSystemException) e.getCause();
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
  public boolean listRecursiveAndTakeAction() throws AzureBlobFileSystemException {
    AbfsConfiguration configuration = abfsClient.getAbfsConfiguration();
    Thread producerThread = null;
//      ListBlobQueue listBlobQueue = new ListBlobQueue(
//          configuration.getProducerQueueMaxSize(), getMaxConsumptionParallelism(),
//          configuration.getListingMaxConsumptionLag());
//      producerThread = new Thread(() -> {
//        try {
//          produceConsumableList(listBlobQueue);
//        } catch (AzureBlobFileSystemException e) {
//          listBlobQueue.markProducerFailure(e);
//        }
//      });
//      producerThread.start();
      try {
        AbfsListStatusRemoteIterator listStatusRemoteIterator
            = new AbfsListStatusRemoteIterator(
            path, abfsClient, tracingContext);
        while(listStatusRemoteIterator.hasNext()) {
          int thread = getMaxConsumptionParallelism();
          List<Future<Void>> futures = new ArrayList<>();
          while (thread > 0 && listStatusRemoteIterator.hasNext()) {
            thread--;
            Path path = new Path(listStatusRemoteIterator.next());
            Future future = executorService.submit(() -> {
              try {
                takeAction(path);
              } catch (AzureBlobFileSystemException e) {
                LOG.error("Error while taking action on path: {}",
                    path.toUri().getPath(), e);
              }
            });
            futures.add(future);
          }
          FutureIO.awaitAllFutures(futures);
        }
      } catch (IOException ioException) {
        throw new AbfsDriverException(ioException);
      }
      executorService.shutdownNow();
      return true;

//      while (!listBlobQueue.getIsCompleted()) {
//        List<Path> paths = listBlobQueue.consume();
//        if (paths == null) {
//          continue;
//        }
//        try {
//          boolean resultOnPartAction = takeAction(paths);
//          if (!resultOnPartAction) {
//            return false;
//          }
//        } catch (AzureBlobFileSystemException parallelConsumptionException) {
//          listBlobQueue.markConsumptionFailed();
//          throw parallelConsumptionException;
//        }
//      }
//      return true;
//    } finally {
//      if (producerThread != null) {
//        producerThreadToBeStopped.set(true);
//      }
//      executorService.shutdownNow();
//    }
  }

  private void produceConsumableList(final ListBlobQueue listBlobQueue)
      throws AzureBlobFileSystemException {
    String continuationToken = null;
    do {
      List<Path> paths = new ArrayList<>();
      final int queueAvailableSizeForProduction = Math.min(
          DEFAULT_AZURE_LIST_MAX_RESULTS,
          listBlobQueue.availableSizeForProduction());
      if (queueAvailableSizeForProduction == 0) {
        break;
      }
      final AbfsRestOperation op;
      try {
         op = abfsClient.listPath(path.toUri().getPath(),
            true,
             queueAvailableSizeForProduction, continuationToken,
            tracingContext);
      } catch (AzureBlobFileSystemException ex) {
        throw ex;
      } catch (IOException ex) {
        throw new AbfsRestOperationException(-1, null,
            "Unknown exception from listing: " + ex.getMessage(), ex);
      }

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
