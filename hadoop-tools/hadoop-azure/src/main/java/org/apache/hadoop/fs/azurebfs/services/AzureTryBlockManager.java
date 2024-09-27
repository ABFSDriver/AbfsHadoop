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
  private List<String> committedBlockEntries = new ArrayList<>();

  /** The list of all blockId's for putBlockList. */
  private final Set<String> orderedBlockList = new LinkedHashSet<>();

  /** The list to store blockId, position, and status. */
  private final LinkedList<BlockEntry> blockEntryList = new LinkedList<>();

  /**
   * Represents a block entry containing blockId, position, and status.
   */
  public static class BlockEntry {
    private final String blockId;
    private AbfsBlockStatus status;
    private final long position;

    public BlockEntry(String blockId, long position, AbfsBlockStatus status) {
      this.blockId = blockId;
      this.position = position;
      this.status = status;
    }

    public String getBlockId() {
      return blockId;
    }

    public long getPosition() {
      return position;
    }

    public AbfsBlockStatus getStatus() {
      return status;
    }

    public void setStatus(AbfsBlockStatus status) {
      this.status = status;
    }
  }


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
  private List<String> getBlockList(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    List<String> committedBlockIdList;
    AbfsBlobClient blobClient = abfsOutputStream.getClientHandler().getBlobClient();
    final AbfsRestOperation op = blobClient
        .getBlockList(abfsOutputStream.getPath(), tracingContext);
    committedBlockIdList = op.getResult().getBlockIdList();
    return committedBlockIdList;
  }

  public void addEntryDuringAppend(String blockId, long position) {
    BlockEntry blockEntry = new BlockEntry(blockId, position, AbfsBlockStatus.SUCCESS);
    blockEntryList.add(blockEntry);
    LOG.debug("Added block {} at position {} with status SUCCESS.", blockId, position);
  }


  /**
   * Prepares the list of blocks to commit.
   *
   * @return the number of blocks to commit
   * @throws IOException if an I/O error occurs
   */
  protected int prepareListToCommit() throws IOException {
    // Adds all the committed blocks if available to the list of blocks to be added in putBlockList.
    orderedBlockList.addAll(committedBlockEntries);
    if (blockEntryList.isEmpty()) {
      return 0;
    }
    // Sort the block entries by position
    blockEntryList.sort(Comparator.comparingLong(BlockEntry::getPosition));

    // Check that all blocks have SUCCESS status and that block positions are in valid order
    for (BlockEntry entry : blockEntryList) {
      // Validate that no block has a NEW status
      if (entry.getStatus() != AbfsBlockStatus.SUCCESS) {
        LOG.debug("Block {} with position {} has status {}, flush cannot proceed.",
            entry.getBlockId(), entry.getPosition(), entry.getStatus());
        throw new IOException("Flush failed. Block " + entry.getBlockId() +
            " with position " + entry.getPosition() + " has status " + entry.getStatus());
      }
      orderedBlockList.add(entry.getBlockId());
    }
    return orderedBlockList.size();
  }

  /**
   * Returns the block ID list.
   *
   * @return the block ID list
   */
  protected Set<String> getBlockIdList() {
    return orderedBlockList;
  }

  /**
   * Performs cleanup after committing blocks.
   */
  protected void postCommitCleanup() {
    blockEntryList.clear();
  }
}
