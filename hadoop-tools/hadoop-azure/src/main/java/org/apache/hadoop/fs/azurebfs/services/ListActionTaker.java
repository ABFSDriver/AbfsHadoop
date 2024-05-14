package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;

public abstract class ListActionTaker {
  final Path path;
  final AbfsBlobClient abfsClient;
  final TracingContext tracingContext;

  public ListActionTaker(Path path, AbfsClient abfsClient, TracingContext tracingContext) {
    this.path = path;
    this.abfsClient = (AbfsBlobClient) abfsClient;
    this.tracingContext = tracingContext;
  }

  abstract boolean takeAction(Path path) throws IOException;

  private boolean takeAction(List<Path> paths) {
    //TODO: take from abfsconfig
    ExecutorService executorService = Executors.newFixedThreadPool(
        2 * Runtime.getRuntime().availableProcessors());
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
    for(Future<Boolean> future : futureList) {
      try {
        Boolean result = future.get();
        if(!result) {
          return false;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return true;
  }

  public boolean listNonRecursiveAndTakeAction() throws IOException {
    String continuationToken = null;
    List<Path> paths = new ArrayList<>();
    do {
      AbfsRestOperation op = abfsClient.listPath(path.toUri().getPath(), false, 1000, continuationToken, tracingContext);
      continuationToken = op.getResult().getResponseHeader(
          HttpHeaderConfigurations.X_MS_CONTINUATION);
      ListResultSchema retrievedSchema = op.getResult().getListResultSchema();
      if(retrievedSchema == null) {
        continue;
      }
      for (ListResultEntrySchema entry : retrievedSchema.paths()) {
        paths.add(new Path(ROOT_PATH, entry.name()));
      }
    } while (continuationToken != null);

    return takeAction(paths);
  }

  public boolean listRecursiveAndTakeAction() throws IOException {
    String continuationToken = null;
    do {
      List<Path> paths = new ArrayList<>();
      AbfsRestOperation op = abfsClient.listPath(path.toUri().getPath(), false, 1000, continuationToken, tracingContext);
      continuationToken = op.getResult().getResponseHeader(
          HttpHeaderConfigurations.X_MS_CONTINUATION);
      ListResultSchema retrievedSchema = op.getResult().getListResultSchema();
      if(retrievedSchema != null) {
        continue;
      }
      for (ListResultEntrySchema entry : retrievedSchema.paths()) {
        paths.add(new Path(ROOT_PATH, entry.name()));
      }
      Boolean resultOnPartAction = takeAction(paths);
      if(!resultOnPartAction) {
        return false;
      }
    } while (continuationToken != null);

    return true;
  }
}
