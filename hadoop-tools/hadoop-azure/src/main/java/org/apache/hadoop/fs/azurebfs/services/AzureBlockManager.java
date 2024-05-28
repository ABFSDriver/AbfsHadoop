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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.store.DataBlocks;

public abstract class AzureBlockManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbfsOutputStream.class);

  /** Factory for blocks. */
  protected final DataBlocks.BlockFactory blockFactory;

  /** Current data block. Null means none currently active. */
  protected AbfsBlock activeBlock;

  /** Count of blocks uploaded. */
  protected long blockCount = 0;

  /** The size of a single block. */
  protected final int blockSize;

  // todo: sneha: not looked at right access specifier, getter-setters
  // synchronized access to correct methods
  protected AbfsOutputStream abfsOutputStream;

  public AzureBlockManager(AbfsOutputStream abfsOutputStream,
      DataBlocks.BlockFactory blockFactory,
      final int blockSize) {
    this.abfsOutputStream = abfsOutputStream;
    this.blockFactory = blockFactory;
    this.blockSize = blockSize;
  }

  public abstract AbfsBlock createBlock(final AzureIngressHandler ingressHandler,
      final long position) throws IOException;

  public synchronized AbfsBlock getActiveBlock() {
    return activeBlock;
  }

  public synchronized boolean hasActiveBlock() {
    return activeBlock != null;
  }

  public DataBlocks.BlockFactory getBlockFactory() {
    return blockFactory;
  }

  public long getBlockCount() {
    return blockCount;
  }

  public int getBlockSize() {
    return blockSize;
  }

  /**
   * Clear the active block.
   */
  void clearActiveBlock() {
    if (activeBlock != null) {
      LOG.debug("Clearing active block");
    }
    synchronized (this) {
      activeBlock = null;
    }
  }
}
