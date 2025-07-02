/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.azurebfs.contracts.services.ReadBufferStatus;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public interface ReadBufferManager {

  void queueReadAhead(final AbfsInputStream stream, final long requestedOffset,
      final int requestedLength, TracingContext tracingContext);

  int getBlock(final AbfsInputStream stream,
      final long position,
      final int length,
      final byte[] buffer) throws IOException;

  void purgeBuffersForStream(AbfsInputStream stream);

  ReadBuffer getNextBlockToRead() throws InterruptedException;

  void doneReading(final ReadBuffer buffer,
      final ReadBufferStatus result,
      final int bytesActuallyRead);

  void testResetReadBufferManager();

  int getThresholdAgeMilliseconds();

  int getCompletedReadListSize();

  void callTryEvict();

  void setThresholdAgeMilliseconds(int thresholdAgeMs);

  void testMimicFullUseAndAddFailedBuffer(ReadBuffer buf);
  int getNumBuffers();
  List<ReadBuffer> getInProgressCopiedList();
  List<ReadBuffer> getReadAheadQueueCopy();
  List<ReadBuffer> getCompletedReadListCopy();
  List<Integer> getFreeListCopy();
  int getReadAheadBlockSize();
  void testResetReadBufferManager(int readAheadBlockSize, int thresholdAgeMilliseconds);
}
