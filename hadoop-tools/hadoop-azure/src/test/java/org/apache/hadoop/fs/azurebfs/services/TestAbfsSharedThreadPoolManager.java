package org.apache.hadoop.fs.azurebfs.services;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_READ_CONCURRENT_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_WRITE_MAX_CONCURRENT_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_WRITE_MAX_REQUESTS_TO_QUEUE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SHARED_THREAD_POOL_DYNAMIC_SCALING_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SHARED_THREAD_POOL_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SHARED_THREAD_POOL_MAX_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SHARED_THREAD_POOL_MIN_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.TEST_CONFIGURATION_FILE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAbfsSharedThreadPoolManager {

  private static final Logger log = LoggerFactory.getLogger(
      TestAbfsSharedThreadPoolManager.class);

  private static final int SHORT_WAIT_MILLIS = 100;
  private static final int LONG_WAIT_MILLIS = 10000;

  @Test
  public void testSingletonPattern() throws Exception {
    Configuration conf = new Configuration();
    conf.addResource(TEST_CONFIGURATION_FILE_NAME);
    String accountName1 = conf.get(FS_AZURE_ACCOUNT_NAME);
    String accountName2 = "testAccount.dfs.core.windows.net";
    conf.set(FS_AZURE_ACCOUNT_NAME, accountName2);

    AbfsConfiguration abfsConfig1 = new AbfsConfiguration(conf, accountName1);
    AbfsConfiguration abfsConfig2 = new AbfsConfiguration(conf, accountName2);
    assertThat(AbfsSharedThreadPoolManager.returnInstance()).isNull();
    AbfsSharedThreadPoolManager instance1 = AbfsSharedThreadPoolManager.getInstance(abfsConfig1);
    AbfsSharedThreadPoolManager instance2 = AbfsSharedThreadPoolManager.getInstance(abfsConfig2);
    assertThat(instance1).isSameAs(instance2);
    assertThat(AbfsSharedThreadPoolManager.returnInstance()).isNotNull();
    AbfsSharedThreadPoolManager.testHardResetThreadPoolManager();
    assertThat(AbfsSharedThreadPoolManager.returnInstance()).isNull();
  }

  @Test
  public void testSharedThreadPoolUsageForWrites() throws Exception {
    Configuration conf = getTestConfiguration();
    String accountName = conf.get(FS_AZURE_ACCOUNT_NAME);

    AbfsConfiguration abfsConfig = new AbfsConfiguration(conf, accountName);
    AbfsSharedThreadPoolManager threadPoolManager = AbfsSharedThreadPoolManager.getInstance(abfsConfig);
    validateThreadPoolState(threadPoolManager,
        0,
        6,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0);

    /*
     * Submitting tasks less that write thread pool size.
     * Here all tasks should be submitted to write thread pool.
     * Shared pool size should remain empty.
     */
    for (int i = 0; i < 2; i++) {
      threadPoolManager.submitWriteTask(this::longRunningTask);
    }
    validateThreadPoolState(threadPoolManager,
        2,
        4,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0);
    /*
     * Submitting tasks more than write thread pool size but less than
     * combined write thread pool size and shared thread pool size.
     * Here excess tasks should be submitted to shared thread pool.
     */
    for (int i = 0; i < 2; i++) {
      threadPoolManager.submitWriteTask(this::longRunningTask);
    }
    validateThreadPoolState(threadPoolManager,
        2,
        4,
        0,
        0,
        0,
        0,
        0,
        2,
        0,
        2,
        0);
    /*
     * Submitting tasks more than write thread pool size and shared pool max size.
     * Here excess tasks should be submitted to write thread pool for waiting.
     */
    for (int i = 0; i < 2; i++) {
      threadPoolManager.submitWriteTask(this::longRunningTask);
    }
    validateThreadPoolState(threadPoolManager,
        2,
        2,
        0,
        0,
        0,
        0,
        0,
        2,
        0,
        2,
        0);
    /*
     * Submitting tasks more than write thread pool size and shared pool max size.
     * Here excess tasks should be submitted to write thread pool for waiting.
     */
    for (int i = 0; i < 2; i++) {
      threadPoolManager.submitWriteTask(this::longRunningTask);
    }
    validateThreadPoolState(threadPoolManager,
        2,
        0,
        0,
        0,
        0,
        0,
        0,
        2,
        0,
        2,
        0);

    /*
     * Submitting tasks more than write thread pool size and shared pool max size.
     * Here excess tasks should be submitted to write thread pool for waiting on semaphore.
     */
    Thread t = new Thread(() -> {for (int i = 0; i < 2; i++) {
      threadPoolManager.submitWriteTask(this::longRunningTask);
    }});
    t.start();
    Thread.sleep(SHORT_WAIT_MILLIS);
    validateThreadPoolState(threadPoolManager,
        2,
        0,
        1,
        0,
        0,
        0,
        0,
        2,
        0,
        2,
        0);
    t.interrupt();
    AbfsSharedThreadPoolManager.testHardResetThreadPoolManager();
  }

  @Test
  public void testSharedThreadPoolUsageForReads() throws Exception {
    Configuration conf = getTestConfiguration();
    String accountName = conf.get(FS_AZURE_ACCOUNT_NAME);
    String keyPrefix = "key";
    int key = 0;
    AbfsConfiguration abfsConfig = new AbfsConfiguration(conf, accountName);
    AbfsSharedThreadPoolManager threadPoolManager = AbfsSharedThreadPoolManager.getInstance(abfsConfig);
    validateThreadPoolState(threadPoolManager,
        0,
        6,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0);

    /*
     * Submitting tasks less that read thread pool size.
     * Here all tasks should be submitted to read thread pool.
     * Shared pool size should remain empty.
     */
    for (int i = 0; i < 2; i++) {
      threadPoolManager.submitReadTask(keyPrefix + key++, this::longRunningTask);
    }
    validateThreadPoolState(threadPoolManager,
        0,
        6,
        0,
        2,
        0,
        2,
        0,
        0,
        0,
        0,
        0);

    /*
     * Submitting tasks more than read thread pool size but less than
     * combined read thread pool size and shared thread pool size.
     * Here excess tasks should be submitted to shared thread pool.
     */
    for (int i = 0; i < 2; i++) {
      threadPoolManager.submitReadTask(keyPrefix + key++, this::longRunningTask);
    }
    validateThreadPoolState(threadPoolManager,
        0,
        6,
        0,
        2,
        0,
        2,
        0,
        2,
        0,
        2,
        0);

    /*
     * Submitting tasks more than read thread pool size and shared pool max size.
     * Here excess tasks should be submitted to shared thread pool for waiting.
     */
    for (int i = 0; i < 2; i++) {
      threadPoolManager.submitReadTask(keyPrefix + key++, this::longRunningTask);
    }
    validateThreadPoolState(threadPoolManager,
        0,
        6,
        0,
        2,
        0,
        2,
        0,
        2,
        0,
        4,
        2);

    // Letting All tasks complete
    Thread.sleep(2 * (LONG_WAIT_MILLIS + SHORT_WAIT_MILLIS));
    validateThreadPoolState(threadPoolManager,
        0,
        6,
        0,
        0,
        2,
        2,
        0,
        0,
        4,
        4,
        0);

    /*
     * Submitting tasks more than read thread pool size and shared pool max size.
     * Here after read thread pool got back idle threads should be able to get more tasks.
     */
    for (int i = 0; i < 2; i++) {
      threadPoolManager.submitReadTask(keyPrefix + key++, this::longRunningTask);
    }
    Thread.sleep(SHORT_WAIT_MILLIS);
    validateThreadPoolState(threadPoolManager,
        0,
        6,
        0,
        2,
        2,
        4,
        0,
        0,
        4,
        4,
        0);
    AbfsSharedThreadPoolManager.testHardResetThreadPoolManager();
  }

  @Test
  public void testSuccessfulReadTaskCancellation() throws Exception {
    Configuration conf = getTestConfiguration();
    String accountName = conf.get(FS_AZURE_ACCOUNT_NAME);
    String keyPrefix = "key";
    int key = 0;
    AbfsConfiguration abfsConfig = new AbfsConfiguration(conf, accountName);
    AbfsSharedThreadPoolManager threadPoolManager = AbfsSharedThreadPoolManager.getInstance(abfsConfig);
    for (int i = 0; i < 5; i++) {
      threadPoolManager.submitReadTask(keyPrefix + key++, this::longRunningTask);
    }
    validateThreadPoolState(threadPoolManager,
        0,
        6,
        0,
        2,
        0,
        2,
        0,
        2,
        0,
        3,
        1);
    assertThat(threadPoolManager.isReadTaskInProgress(keyPrefix + (key-1))).isFalse();
    assertThat(threadPoolManager.isReadTaskInQueue(keyPrefix + (key-1))).isTrue();
    boolean isRemoved = threadPoolManager.tryCancelReadTask(keyPrefix + (key-1));
    assertThat(isRemoved).isTrue();
    Thread.sleep(2 * (LONG_WAIT_MILLIS + SHORT_WAIT_MILLIS));
    validateThreadPoolState(threadPoolManager,
        0,
        6,
        0,
        0,
        2,
        2,
        0,
        0,
        3,
        3,
        0);
    AbfsSharedThreadPoolManager.testHardResetThreadPoolManager();
  }

  private Configuration getTestConfiguration() {
    Configuration conf = new Configuration();
    conf.addResource(TEST_CONFIGURATION_FILE_NAME);
    conf.setBoolean(FS_AZURE_SHARED_THREAD_POOL_ENABLED, true);
    conf.setBoolean(FS_AZURE_SHARED_THREAD_POOL_DYNAMIC_SCALING_ENABLED, true);
    conf.setInt(FS_AZURE_SHARED_THREAD_POOL_MIN_SIZE, 2);
    conf.setInt(FS_AZURE_SHARED_THREAD_POOL_MAX_SIZE, 4);

    conf.setInt(AZURE_WRITE_MAX_CONCURRENT_REQUESTS, 2);
    conf.setInt(AZURE_WRITE_MAX_REQUESTS_TO_QUEUE, 4);

    conf.setInt(AZURE_READ_CONCURRENT_REQUESTS, 2);
    return conf;
  }

  private Void longRunningTask() {
    try {
      Thread.sleep(LONG_WAIT_MILLIS);
      System.out.println("Long running task completed");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return null;
  }

  private void validateThreadPoolState(
      AbfsSharedThreadPoolManager threadPoolManager,
      int writeActiveTaskCount,
      int writeAvailablePermits,
      int writeWaitingPermits,
      int readActiveTaskCount,
      int readCompletedTaskCount,
      int readTotalTaskCount,
      int readQueueSize,
      int sharedActiveTaskCount,
      int sharedCompletedTaskCount,
      int sharedTotalTaskCount,
      int sharedQueueSize) {
    assertThat(threadPoolManager.getWriteThreadPoolActiveTaskCount()).isEqualTo(writeActiveTaskCount);
    assertThat(threadPoolManager.getWriteThreadPoolAvailablePermitsCount()).isEqualTo(writeAvailablePermits);
    assertThat(threadPoolManager.getWriteThreadPoolWaitingPermits()).isEqualTo(writeWaitingPermits);
    assertThat(threadPoolManager.getReadThreadPoolActiveTaskCount()).isEqualTo(readActiveTaskCount);
    assertThat(threadPoolManager.getReadThreadPoolCompletedTaskCount()).isEqualTo(readCompletedTaskCount);
    assertThat(threadPoolManager.getReadThreadPoolTotalTaskCount()).isEqualTo(readTotalTaskCount);
    assertThat(threadPoolManager.getReadThreadPoolQueueSize()).isEqualTo(readQueueSize);
    assertThat(threadPoolManager.getSharedThreadPoolActiveTaskCount()).isEqualTo(sharedActiveTaskCount);
    assertThat(threadPoolManager.getSharedThreadPoolCompletedTaskCount()).isEqualTo(sharedCompletedTaskCount);
    assertThat(threadPoolManager.getSharedThreadPoolTotalTaskCount()).isEqualTo(sharedTotalTaskCount);
    assertThat(threadPoolManager.getSharedThreadPoolQueueSize()).isEqualTo(sharedQueueSize);
  }
}
