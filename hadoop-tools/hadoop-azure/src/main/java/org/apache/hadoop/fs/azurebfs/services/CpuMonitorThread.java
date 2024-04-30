package org.apache.hadoop.fs.azurebfs.services;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CpuMonitorThread implements Runnable {
    private final AbfsOutputStream abfsOutputStream;
    private final double cpuThreshold;
    private final ScheduledExecutorService scheduler;

    public CpuMonitorThread(AbfsOutputStream abfsOutputStream, double cpuThreshold) {
        this.abfsOutputStream = abfsOutputStream;
        this.cpuThreshold = cpuThreshold;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public void run() {
        scheduler.scheduleAtFixedRate(() -> {
            double cpuUtilization = getCpuUtilization();
            System.out.println("Current CPU Utilization: " + cpuUtilization);
            abfsOutputStream.adjustThreadPoolSize(!(cpuUtilization > cpuThreshold));
        }, 0, 2, TimeUnit.SECONDS); // Schedule every 30 seconds
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
}
