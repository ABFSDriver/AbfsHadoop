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
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
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

/**
 * Manages Azure Blob blocks for append operations.
 */
public class AzureTryBlockManager extends AzureBlockManager {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsOutputStream.class);


 /** The list of already committed blocks is stored in this list. */
  private Set<String> committedBlockEntries = new LinkedHashSet<>();

  /** The list to store blockId, position, and status. */
  private final LinkedList<BlockEntry> blockEntryList = new LinkedList<>();

  /** Head pointer for traversing the block entries. */
  private BlockEntry head;

  /**
   * Constructs an AzureBlobBlockManager.
   *
   * @param abfsOutputStream the output stream
   * @param blockFactory the block factory
   * @param bufferSize the buffer size
   * @throws AzureBlobFileSystemException if an error occurs
   */
  public AzureTryBlockManager(AbfsOutputStream abfsOutputStream,
      DataBlocks.BlockFactory blockFactory,
      int bufferSize)
      throws AzureBlobFileSystemException {
    super(abfsOutputStream, blockFactory, bufferSize);
    if (abfsOutputStream.getPosition() > 0 && !abfsOutputStream.isAppendBlob()) {
      this.committedBlockEntries = getBlockList(
          abfsOutputStream.getTracingContext());
    }
    LOG.trace(
        "Created a new Blob Block Manager for AbfsOutputStream instance {} for path {}",
        abfsOutputStream.getStreamID(), abfsOutputStream.getPath());
  }

  /**
   * Creates a new block.
   *
   * @param position the position
   * @return the created block
   * @throws IOException if an I/O error occurs
   */
  @Override
  protected synchronized AbfsBlock createBlockInternal(final long position)
      throws IOException {
    if (activeBlock == null) {
      blockCount++;
      activeBlock = new AbfsBlobBlock(abfsOutputStream, position);
      ((AbfsBlobBlock) activeBlock).setBlockEntry(addNewEntry(activeBlock.getBlockId(),activeBlock.getOffset()));
    }
    return activeBlock;
  }

  /**
   * Returns block id's which are committed for the blob.
   *
   * @param tracingContext Tracing context object.
   * @return list of committed block id's.
   * @throws AzureBlobFileSystemException if an error occurs
   */
  private Set<String> getBlockList(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    Set<String> committedBlockIdList;
    AbfsBlobClient blobClient = abfsOutputStream.getClientHandler().getBlobClient();
    final AbfsRestOperation op = blobClient
        .getBlockList(abfsOutputStream.getPath(), tracingContext);
    committedBlockIdList = op.getResult().getBlockIdList();
    return committedBlockIdList;
  }

  public synchronized BlockEntry addNewEntry(String blockId, long position) {
    BlockEntry blockEntry = new BlockEntry(blockId, position, AbfsBlockStatus.NEW);
    blockEntryList.addLast(blockEntry);
    LOG.debug("Added block {} at position {} with status NEW.", blockId, position);
    return blockEntry;
  }

  public synchronized void updateEntry(AbfsBlobBlock block) {
      BlockEntry blockEntry = block.getBlockEntry();
      blockEntry.setStatus(AbfsBlockStatus.SUCCESS);
      LOG.debug("Added block {} at position {} with status SUCCESS.", block.getBlockId(), blockEntry.getPosition());
  }

  /**
   * Prepares the list of blocks to commit.
   *
   * @return the number of blocks to commit
   * @throws IOException if an I/O error occurs
   */
  //TODO: make boolean
  protected int prepareListToCommit() throws IOException {
    // Adds all the committed blocks if available to the list of blocks to be added in putBlockList.
    if (blockEntryList.isEmpty()) {
      return 0; // No entries to commit
    }
    while (!blockEntryList.isEmpty()) {
      BlockEntry current = blockEntryList.poll();
      if (current.getStatus() != AbfsBlockStatus.SUCCESS) {
        LOG.debug("Block {} with position {} has status {}, flush cannot proceed.",
            current.getBlockId(), current.getPosition(), current.getStatus());
        throw new IOException("Flush failed. Block " + current.getBlockId() +
            " with position " + current.getPosition() + " has status " + current.getStatus());
      }
      committedBlockEntries.add(current.getBlockId());
      LOG.debug("Block {} added to committed entries.", current.getBlockId());
    }
    return committedBlockEntries.size();
  }

  /**
   * Returns the block ID list.
   *
   * @return the block ID list
   */
  protected Set<String> getBlockIdList() {
    return committedBlockEntries;
  }
}
