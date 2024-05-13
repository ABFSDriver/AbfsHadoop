package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class WriteThreadPoolSizeManager {
    private static WriteThreadPoolSizeManager instance;
    private final int maxPoolSize;
    private final Set<AbfsOutputStream> outputStreams;
    private final ScheduledExecutorService cpuMonitorExecutor;
    private final ExecutorService boundedThreadPool;
    private final double cpuThreshold = 0.50;
    private int concurrentWrites;
    private final Lock lock = new ReentrantLock();
    private final AtomicInteger pendingTasks;

    private WriteThreadPoolSizeManager(AbfsConfiguration abfsConfiguration) {
        concurrentWrites = (abfsConfiguration.getBlockOutputActiveBlocks());
        if (concurrentWrites < 1) {
            concurrentWrites = 1;
        }
        maxPoolSize = Math.min(60 * concurrentWrites, 6 * Runtime.getRuntime().availableProcessors());
        outputStreams = Collections.newSetFromMap(new ConcurrentHashMap<>());
        boundedThreadPool = Executors.newFixedThreadPool(maxPoolSize);
        cpuMonitorExecutor = Executors.newScheduledThreadPool(1);
        pendingTasks = new AtomicInteger(0);
    }

    public static synchronized WriteThreadPoolSizeManager getInstance(AbfsConfiguration abfsConfiguration) {
        if (instance == null) {
            instance = new WriteThreadPoolSizeManager(abfsConfiguration);
        }
        return instance;
    }

    public void adjustThreadPoolSize(int newMaxPoolSize) throws InterruptedException {
        synchronized (this) {
            ThreadPoolExecutor threadPoolExecutor = ((ThreadPoolExecutor) boundedThreadPool);
            int currentCorePoolSize = threadPoolExecutor.getCorePoolSize();
            int currentActiveThreads = threadPoolExecutor.getActiveCount();

            if (newMaxPoolSize < currentCorePoolSize && currentActiveThreads > 0) {
                System.out.println("Waiting for active threads to complete tasks before reducing thread count...");
                waitUntilTasksComplete(threadPoolExecutor);
            }
            if (newMaxPoolSize >= currentCorePoolSize) {
                if (newMaxPoolSize > 0) {
                    threadPoolExecutor.setMaximumPoolSize(newMaxPoolSize);
                    threadPoolExecutor.setCorePoolSize(newMaxPoolSize);
                    int tasksToSubmit = newMaxPoolSize - currentCorePoolSize;
                    for (int i = 0; i < tasksToSubmit; i++) {
                        System.out.println("The thread count is :" + threadPoolExecutor.getPoolSize());
                        submitPlaceholderTask();
                    }
                }
            } else {
                threadPoolExecutor.setCorePoolSize(newMaxPoolSize);
                threadPoolExecutor.setMaximumPoolSize(newMaxPoolSize);
            }
            System.out.println("The thread pool size is: " + newMaxPoolSize);
        }
        notifyAbfsOutputStreams(newMaxPoolSize);
    }

    private void waitUntilTasksComplete(ThreadPoolExecutor threadPoolExecutor) throws InterruptedException {
        while (threadPoolExecutor.getActiveCount() > 0) {
            Thread.sleep(100);
        }
    }

    public void registerAbfsOutputStream(AbfsOutputStream outputStream) {
        if (!outputStreams.contains(outputStream)) {
            outputStreams.add(outputStream);
            try {
                System.out.println("The number of outputstreams is " + getTotalOutputStreams());
                adjustThreadPoolSizeBasedOnOutputStreams();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void deRegisterAbfsOutputStream(AbfsOutputStream outputStream) {
        if (outputStreams.contains(outputStream)) {
            outputStreams.remove(outputStream);
            try {
                adjustThreadPoolSizeBasedOnOutputStreams();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public int getTotalOutputStreams() {
        return outputStreams.size();
    }

    private void notifyAbfsOutputStreams(int newPoolSize) throws InterruptedException {
        for (AbfsOutputStream outputStream : outputStreams) {
            outputStream.poolSizeChanged(newPoolSize);
        }
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    private void submitPlaceholderTask() {
        pendingTasks.incrementAndGet();
        boundedThreadPool.submit((Runnable) pendingTasks::decrementAndGet);
    }

    public void startCPUMonitoring() {
        cpuMonitorExecutor.scheduleAtFixedRate(() -> {
            double cpuUtilization = getCpuUtilization();
            System.out.println("Current CPU Utilization is this: " + cpuUtilization);
            try {
                adjustThreadPoolSizeBasedOnCPU(cpuUtilization);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, 0, 30, TimeUnit.SECONDS);
    }

    private double getCpuUtilization() {
        OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
            com.sun.management.OperatingSystemMXBean sunOsBean = (com.sun.management.OperatingSystemMXBean) osBean;
            double cpuLoad = sunOsBean.getProcessCpuLoad();
            if (cpuLoad >= 0) {
                return cpuLoad;
            }
        }
        return 0.0;
    }

    public void adjustThreadPoolSizeBasedOnCPU(double cpuUtilization) throws InterruptedException {
        int newMaxPoolSize;
        lock.lock();
        try {
            int currentPoolSize = ((ThreadPoolExecutor) boundedThreadPool).getMaximumPoolSize();
            if (cpuUtilization > cpuThreshold) {
                newMaxPoolSize = Math.max(1, currentPoolSize / 2);
            } else {
                newMaxPoolSize = Math.min(2 * currentPoolSize, Math.max(maxPoolSize, 55 * concurrentWrites));
            }
            System.out.println("The new max pool size: " + newMaxPoolSize);
        } finally {
            lock.unlock();
        }
        adjustThreadPoolSize(newMaxPoolSize);
    }

    public void adjustThreadPoolSizeBasedOnOutputStreams() throws InterruptedException {
        int newMaxPoolSize;
        lock.lock();
        try {
            int currentThreadPoolSize = ((ThreadPoolExecutor) boundedThreadPool).getMaximumPoolSize();
            int currentOutputStreams = outputStreams.size();
            int potentialSize = Math.max(currentOutputStreams * concurrentWrites, currentThreadPoolSize);
            System.out.println("The potential size is " + potentialSize);
            newMaxPoolSize = Math.min(55 * concurrentWrites, potentialSize);
        } finally {
            lock.unlock();
        }
        adjustThreadPoolSize(newMaxPoolSize);
    }

    public void shutdown() throws InterruptedException {
        instance = null;
        outputStreams.clear();
        cpuMonitorExecutor.shutdown();
        boundedThreadPool.shutdown();
        if (!boundedThreadPool.awaitTermination(30, TimeUnit.SECONDS)) {
            boundedThreadPool.shutdownNow();
        }
    }

    public ExecutorService getExecutorService() {
        return boundedThreadPool;
    }
}
