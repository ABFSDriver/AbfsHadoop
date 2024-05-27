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

import org.apache.hadoop.fs.store.DataBlocks;

public class AzureDFSBlockManager extends AzureBlockManager {

  public AzureDFSBlockManager(AbfsOutputStream abfsOutputStream,
      DataBlocks.BlockFactory blockFactory,
      int blockSize) {
    super(abfsOutputStream, blockFactory, blockSize);
  }

  @Override
  public AbfsBlock createBlock(final AzureIngressHandler ingressHandler,
      final long position) throws IOException {
    if (activeBlock == null) {
      blockCount++;
      activeBlock = new AbfsBlock(abfsOutputStream, position);
    }
    return activeBlock;
  }


  @Override
  public synchronized AbfsBlock getActiveBlock() {
    return super.getActiveBlock();
  }

  @Override
  public synchronized boolean hasActiveBlock() {
    return super.hasActiveBlock();
  }
}
