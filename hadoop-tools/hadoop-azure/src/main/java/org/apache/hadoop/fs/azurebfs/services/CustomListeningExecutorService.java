package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CustomListeningExecutorService implements ListeningExecutorService {
    private final ExecutorService delegate;

    public CustomListeningExecutorService(ExecutorService delegate) {
        this.delegate = delegate;
    }

    @Override
    public ListenableFuture<?> submit(Runnable task) {
        return MoreExecutors.listeningDecorator(delegate).submit(task);
    }

    @Override
    public <T> ListenableFuture<T> submit(Runnable task, T result) {
        return MoreExecutors.listeningDecorator(delegate).submit(task, result);
    }

    @Override
    public <T> ListenableFuture<T> submit(Callable<T> task) {
        return MoreExecutors.listeningDecorator(delegate).submit(task);
    }

    @Override
    public void execute(Runnable command) {
        delegate.execute(command);
    }

    @Override
    public <T> List<java.util.concurrent.Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return delegate.invokeAll(tasks);
    }

    @Override
    public <T> List<java.util.concurrent.Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return delegate.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return delegate.invokeAny(tasks, timeout, unit);
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }

    public ExecutorService getDelegate() {
        return delegate;
    }
}
