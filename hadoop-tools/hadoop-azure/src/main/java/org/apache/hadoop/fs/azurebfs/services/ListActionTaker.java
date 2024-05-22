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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListResultSchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;

public abstract class ListActionTaker {

  final Path path;

  final AbfsBlobClient abfsClient;

  final TracingContext tracingContext;

  private final ExecutorService executorService;

  public ListActionTaker(Path path,
      AbfsClient abfsClient,
      TracingContext tracingContext) {
    this.path = path;
    this.abfsClient = (AbfsBlobClient) abfsClient;
    this.tracingContext = tracingContext;

    //TODO: take from abfsconfig
    executorService = Executors.newFixedThreadPool(
        2 * Runtime.getRuntime().availableProcessors());
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
    boolean actionResult = true;
    for (Future<Boolean> future : futureList) {
      try {
        Boolean result = future.get();
        if (!result) {
          actionResult = false;
        }
      } catch (InterruptedException ignored) {

      } catch (ExecutionException e) {
        throw (IOException) e.getCause();
      }
    }
    return actionResult;
  }

  public boolean listRecursiveAndTakeAction() throws IOException {
    AbfsConfiguration configuration = abfsClient.getAbfsConfiguration();
    try {
      ListBlobQueue listBlobQueue = new ListBlobQueue(
          configuration.getProducerQueueMaxSize(),
          configuration.getBlobListQueueMaxConsumptionThread());
      Thread producerThread = new Thread(() -> {
        try {
          produceConsumableList(listBlobQueue);
        } catch (AzureBlobFileSystemException e) {
          listBlobQueue.setFailed(e);
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
          listBlobQueue.consumptionFailed();
          throw parallelConsumptionException;
        }
      }
      return true;
    } finally {
      executorService.shutdown();
    }
  }

  private void produceConsumableList(final ListBlobQueue listBlobQueue)
      throws AzureBlobFileSystemException {
    String continuationToken = null;
    do {
      List<Path> paths = new ArrayList<>();
      AbfsRestOperation op = null;
      try {
        op = abfsClient.listPath(path.toUri().getPath(), true,
            abfsClient.abfsConfiguration.getListMaxResults(), continuationToken,
            tracingContext);
      } catch (Exception ex) {

        int a = 1;
        a++;
      }

      ListResultSchema retrievedSchema = op.getResult().getListResultSchema();
      if (retrievedSchema == null) {
        continue;
      }
      continuationToken = ((BlobListResultSchema)retrievedSchema).getNextMarker();
      for (ListResultEntrySchema entry : retrievedSchema.paths()) {
        Path entryPath = new Path(ROOT_PATH, entry.name());
        if (!entryPath.equals(this.path)) {
          paths.add(entryPath);
        }
      }
      listBlobQueue.enqueue(paths);
    } while (continuationToken != null
        && !listBlobQueue.getConsumptionFailed());
    listBlobQueue.complete();
  }
}
