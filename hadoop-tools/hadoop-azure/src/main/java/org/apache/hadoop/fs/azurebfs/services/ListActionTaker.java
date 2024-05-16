package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
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

  private boolean takeAction(List<Path> paths) {
    List<Future<Boolean>> futureList = new ArrayList<>();
    for (Path path : paths) {
      Future<Boolean> future = executorService.submit(() -> {
        try {
          return takeAction(path);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
      futureList.add(future);
    }
    for (Future<Boolean> future : futureList) {
      try {
        Boolean result = future.get();
        if (!result) {
          return false;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return true;
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
        } catch (RuntimeException parallelConsumptionException) {
          listBlobQueue.consumptionFailed();
          throw new IOException(parallelConsumptionException);
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
      AbfsRestOperation op = abfsClient.listPath(path.toUri().getPath(), true,
          abfsClient.abfsConfiguration.getListMaxResults(), continuationToken,
          tracingContext);
      //TODO: pranav: this would be changed to use blobList;
      continuationToken = op.getResult().getResponseHeader(
          HttpHeaderConfigurations.X_MS_CONTINUATION);
      ListResultSchema retrievedSchema = op.getResult().getListResultSchema();
      if (retrievedSchema == null) {
        continue;
      }
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
