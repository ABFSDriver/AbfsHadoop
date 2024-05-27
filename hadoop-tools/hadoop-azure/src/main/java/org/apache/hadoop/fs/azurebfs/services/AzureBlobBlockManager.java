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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.store.DataBlocks;

public class AzureBlobBlockManager extends AzureBlockManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbfsOutputStream.class);

  /** The map to store blockId and Status **/
  private final LinkedHashMap<String, AbfsBlockStatus> blockStatusMap = new LinkedHashMap<>();

  /** The list of already committed blocks is stored in this list. */
  private List<String> committedBlockEntries = new ArrayList<>();

  /** The list of all blockId's for putBlockList. */
  private final Set<String> blockIdList = new LinkedHashSet<>();

  /** List to validate order. */
  private final UniqueArrayList<String> orderedBlockList = new UniqueArrayList<>();

  private final Lock lock = new ReentrantLock();

  public static class UniqueArrayList<T> extends ArrayList<T> {
    @Override
    public boolean add(T element) {
      if (!super.contains(element)) {
        return super.add(element);
      }
      return false;
    }
  }

  public AzureBlobBlockManager(AbfsOutputStream abfsOutputStream, DataBlocks.BlockFactory blockFactory, int bufferSize)
      throws AzureBlobFileSystemException {
    super(abfsOutputStream, blockFactory, bufferSize);
    if (abfsOutputStream.getPosition() > 0) {
      this.committedBlockEntries = getBlockList(abfsOutputStream.getTracingContext());
    }
  }

  @Override
  public AbfsBlock createBlock(final AzureIngressHandler ingressHandler,
      final long position) throws IOException {
    if (activeBlock == null) {
      blockCount++;
      activeBlock = new AbfsBlobBlock(abfsOutputStream, position);
    }
    return activeBlock;
  }

  /**
   * Returns block id's which are committed for the blob.
   * @param tracingContext Tracing context object.
   * @return list of committed block id's.
   * @throws AzureBlobFileSystemException
   */
  private List<String> getBlockList(TracingContext tracingContext) throws AzureBlobFileSystemException {
    List<String> committedBlockIdList;
    final AbfsRestOperation op = abfsOutputStream.getClient().getBlockList(abfsOutputStream.getPath(), tracingContext);
    committedBlockIdList = op.getResult().getBlockIdList();
    return committedBlockIdList;
  }

  // Put entry in map with status as NEW which is changed to SUCCESS when successfully appended.
  public void trackBlockWithData(AbfsBlobBlock block) {
    lock.lock();
    try {
      blockStatusMap.put(block.getBlockId(), AbfsBlockStatus.NEW);
      blockIdList.add(block.getBlockId());
      orderedBlockList.add(block.getBlockId());
    } finally {
      lock.unlock();
    }
  }

  public void updateBlockStatus(AbfsBlobBlock block, AbfsBlockStatus status)
      throws IOException {
    String key = block.getBlockId();
    lock.lock();
    try {
      if (!getBlockStatusMap().containsKey(key)) {
        throw new IOException("Block is missing with blockId " + key
            + " for offset " + block.getOffset()
            + " for path" + abfsOutputStream.getPath()
            + " with streamId "
            + abfsOutputStream.getStreamID());
      }
      else {
        blockStatusMap.put(key, status);
      }
    } finally {
      lock.unlock();
    }
  }

  public int prepareListToCommit(long offset) throws IOException {
    // Adds all the committed blocks if available to the list of blocks to be added in putBlockList.
    blockIdList.addAll(committedBlockEntries);
    boolean successValue = true;
    String failedBlockId = "";
    AbfsBlockStatus success = AbfsBlockStatus.SUCCESS;

    // No network calls needed for empty map.
    if (blockStatusMap.isEmpty()) {
      return 0;
    }

    int mapEntry = 0;
    // If any of the entry in the map doesn't have the status of SUCCESS, fail the flush.
    for (Map.Entry<String, AbfsBlockStatus> entry : getBlockStatusMap().entrySet()) {
      if (!success.equals(entry.getValue())) {
        failedBlockId = entry.getKey();
        LOG.debug(
            "A past append for the given offset {} with blockId {} and streamId {}"
                + " for the path {} was not successful", offset, failedBlockId,
            abfsOutputStream.getStreamID(), abfsOutputStream.getPath());
        throw new IOException(
            "A past append was not successful for blockId " + failedBlockId
                + " and offset " + offset
                + abfsOutputStream.getPath() + " with streamId "
                + abfsOutputStream.getStreamID());

      }
      else {
        if (!entry.getKey().equals(orderedBlockList.get(mapEntry))) {
          LOG.debug(
              "The order for the given offset {} with blockId {} and streamId {} "
                  + " for the path {} was not successful", offset, entry.getKey(),
              abfsOutputStream.getStreamID(), abfsOutputStream.getPath());
          throw new IOException(
              "The ordering in map is incorrect for blockId " + entry.getKey()
                  + " and offset " + offset + " for path "
                  + abfsOutputStream.getPath() + " with streamId "
                  + abfsOutputStream.getStreamID());
        }
        blockIdList.add(entry.getKey());
        mapEntry++;
      }
    }

    return mapEntry;
  }

  public LinkedHashMap<String, AbfsBlockStatus> getBlockStatusMap() {
    return blockStatusMap;
  }

  public Set<String> getBlockIdList() {
    return blockIdList;
  }

  void postCommitCleanup() {
    lock.lock();
    try {
      blockStatusMap.clear();
      orderedBlockList.clear();
    } finally {
      lock.unlock();
    }
  }
}
