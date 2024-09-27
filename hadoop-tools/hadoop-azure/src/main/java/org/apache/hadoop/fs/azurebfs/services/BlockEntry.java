package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.services.AbfsBlockStatus;
import org.apache.hadoop.fs.azurebfs.services.AzureTryBlockManager;

/**
 * Represents a block entry containing blockId, position, and status.
 */
public class BlockEntry {
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