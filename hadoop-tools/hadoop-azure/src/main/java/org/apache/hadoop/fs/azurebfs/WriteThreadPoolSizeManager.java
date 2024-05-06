package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.ArrayList;
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
    private final double cpuThreshold = 0.15;

    private WriteThreadPoolSizeManager() {
        maxPoolSize = 64; // Initial max pool size
        outputStreams = new ArrayList<>();
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

    public synchronized int getTotalOutputStreams() {
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

    public void startCPUMonitoring() {
        cpuMonitorExecutor.scheduleAtFixedRate(() -> {
            double cpuUtilization = getCpuUtilization();
            System.out.println("Current CPU Utilization: " + cpuUtilization);
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
            double cpuLoad = sunOsBean.getSystemCpuLoad();
            if (cpuLoad >= 0) {
                return cpuLoad;
            }
        }
        return 0.0;
    }

    private void adjustThreadPoolSizeBasedOnCPU(double cpuUtilization) throws InterruptedException {
        int newMaxPoolSize;
        int currentPoolSize = ((ThreadPoolExecutor) boundedThreadPool).getMaximumPoolSize();
        if (cpuUtilization > cpuThreshold) {
            newMaxPoolSize = Math.max(1, currentPoolSize / 2);
        } else {
            newMaxPoolSize = Math.min(2 * currentPoolSize, maxPoolSize);
        }
        adjustThreadPoolSize(newMaxPoolSize);
    }

    public void shutdown() {
        instance = null;
        cpuMonitorExecutor.shutdown();
        boundedThreadPool.shutdown();
    }

    public ExecutorService getExecutorService() {
        return boundedThreadPool;
    }
}
