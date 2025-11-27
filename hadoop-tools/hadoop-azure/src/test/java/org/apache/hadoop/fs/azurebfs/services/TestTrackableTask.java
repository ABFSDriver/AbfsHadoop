/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for verifying thet states of trackable task during different phases of execution.
 */
public class TestTrackableTask {

  private final int sleepDurationMs = 1000;

  @Test
  public void testTaskState() throws Exception {
    Callable<Void> task = () -> {
      Thread.sleep(2 * sleepDurationMs);
      return null;
    };
    TrackableTask trackableTask = new TrackableTask(task);
    // Assert initial state is QUEUED

    assertThat(trackableTask.isQueued()).isTrue();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(trackableTask);
    Thread.sleep(sleepDurationMs);

    // Assert state is RUNNING
    assertThat(trackableTask.isRunning()).isTrue();
    Thread.sleep(2 * sleepDurationMs);

    // Assert state is COMPLETED
    assertThat(trackableTask.isCompleted()).isTrue();
    executor.shutdown();
  }
}
