package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.util.SemaphoredDelegatingExecutor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

public class CustomSemaphoredExecutor extends SemaphoredDelegatingExecutor {
    int maxPoolSize;
    public CustomSemaphoredExecutor(
            ExecutorService executorDelegatee,
            int permitCount,
            boolean fair) {
        super(executorDelegatee, permitCount, fair);
        this.maxPoolSize = ((ThreadPoolExecutor) executorDelegatee).getMaximumPoolSize();
    }

    protected ExecutorService getDelegateExecutor() {
        return super.delegate();
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }
}

