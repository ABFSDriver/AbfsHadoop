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
    private int maxPoolSize;
    private final List<AbfsOutputStream> outputStreams;
    private final ScheduledExecutorService cpuMonitorExecutor;
    private final ExecutorService boundedThreadPool;

    private WriteThreadPoolSizeManager() {
        maxPoolSize = 20; // Initial max pool size
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

    public synchronized void adjustThreadPoolSize(int newMaxPoolSize) {
        this.maxPoolSize = newMaxPoolSize;
        ((ThreadPoolExecutor) boundedThreadPool).setMaximumPoolSize(newMaxPoolSize); // Adjust max pool size
        //notifyAbfsOutputStreams(newMaxPoolSize);
    }

    public void registerAbfsOutputStream(AbfsOutputStream outputStream) {
        outputStreams.add(outputStream);
    }

    private void notifyAbfsOutputStreams(int newPoolSize) {
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
            adjustThreadPoolSizeBasedOnCPU(cpuUtilization);
        }, 0, 30, TimeUnit.SECONDS); // Adjust monitoring interval as needed
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

    private void adjustThreadPoolSizeBasedOnCPU(double cpuUtilization) {
        int newMaxPoolSize;
        if (cpuUtilization > 0.8) {
            newMaxPoolSize = maxPoolSize / 2; // Decrease pool size by half
        } else {
            newMaxPoolSize = maxPoolSize * 2; // Double pool size
        }
        adjustThreadPoolSize(newMaxPoolSize);
    }

    // Other methods and functionalities of PoolSizeManager

    public void shutdown() {
        cpuMonitorExecutor.shutdown();
        boundedThreadPool.shutdown(); // Shutdown bounded thread pool
        // Other shutdown logic
    }

    public ExecutorService getExecutorService() {
        return boundedThreadPool; // Return the bounded thread pool as ExecutorService
    }
}
