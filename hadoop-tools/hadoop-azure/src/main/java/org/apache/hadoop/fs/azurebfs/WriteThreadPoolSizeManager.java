package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class WriteThreadPoolSizeManager {
    private static WriteThreadPoolSizeManager instance;
    private final int maxPoolSize;
    private final List<AbfsOutputStream> outputStreams;
    private final ScheduledExecutorService cpuMonitorExecutor;
    private final ExecutorService boundedThreadPool;
    private final double cpuThreshold = 0.50;

    private WriteThreadPoolSizeManager() {
        maxPoolSize = 500; // Initial max pool size
        outputStreams = Collections.synchronizedList(new ArrayList<>());
        boundedThreadPool = Executors.newFixedThreadPool(maxPoolSize); // Create bounded thread pool
        cpuMonitorExecutor = Executors.newScheduledThreadPool(1);
        startCPUMonitoring();
    }

    public static synchronized WriteThreadPoolSizeManager getInstance() {
        if (instance == null) {
            instance = new WriteThreadPoolSizeManager();
        }
        return instance;
    }

    public synchronized void adjustThreadPoolSize(int newMaxPoolSize) throws InterruptedException {
        ((ThreadPoolExecutor) boundedThreadPool).setMaximumPoolSize(newMaxPoolSize); // Adjust max pool size
        notifyAbfsOutputStreams(newMaxPoolSize);
    }

    public void registerAbfsOutputStream(AbfsOutputStream outputStream) {
        outputStreams.add(outputStream);
    }

    public void deRegisterAbfsOutputStream(AbfsOutputStream outputStream) {
        outputStreams.remove(outputStream);
    }

    public int getTotalOutputStreams() {
        return outputStreams.size();
    }

    private synchronized void notifyAbfsOutputStreams(int newPoolSize) throws InterruptedException {
        for (AbfsOutputStream outputStream : outputStreams) {
            outputStream.poolSizeChanged(newPoolSize);
        }
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public synchronized void startCPUMonitoring() {
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

    private synchronized void adjustThreadPoolSizeBasedOnCPU(double cpuUtilization) throws InterruptedException {
        int newMaxPoolSize;
        int currentPoolSize = ((ThreadPoolExecutor) boundedThreadPool).getMaximumPoolSize();
        if (cpuUtilization > cpuThreshold) {
            newMaxPoolSize = Math.max(1, currentPoolSize / 2);
        } else {
            newMaxPoolSize = Math.min(2 * currentPoolSize, maxPoolSize);
        }
        System.out.println("The new max pool size: " + newMaxPoolSize);
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
