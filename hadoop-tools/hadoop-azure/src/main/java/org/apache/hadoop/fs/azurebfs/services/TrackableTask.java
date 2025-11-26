package org.apache.hadoop.fs.azurebfs.services;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.TaskState;

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;

public class TrackableTask implements Callable<Void> {
  private final Callable<Void> delegate;
  private volatile TaskState state = AbfsHttpConstants.TaskState.QUEUED;

  public TrackableTask(Callable<Void> delegate) {
    this.delegate = delegate;
  }

  @Override
  public Void call() throws Exception {
    state = TaskState.RUNNING;
    try {
      return delegate.call();
    } finally {
      state = TaskState.COMPLETED;
    }
  }

  public boolean isQueued() {
    return state == TaskState.QUEUED;
  }

  public boolean isRunning() {
    return state == TaskState.RUNNING;
  }

  public boolean isCompleted() {
    return state == TaskState.COMPLETED;
  }
}