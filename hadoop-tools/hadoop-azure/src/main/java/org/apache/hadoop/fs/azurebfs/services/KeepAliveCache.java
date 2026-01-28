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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.ClosedIOException;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.http.HttpClientConnection;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.KEEP_ALIVE_CACHE_CLOSED;

class KeepAliveCache implements Closeable {

  /**
   * Logger instance.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      KeepAliveCache.class);

  private static final class PooledConnection {

    final HttpClientConnection conn;

    final String cacheKey;

    final AtomicBoolean inUse = new AtomicBoolean(false); // New Flag

    PooledConnection(HttpClientConnection conn, String cacheKey) {
      this.conn = conn;
      this.cacheKey = cacheKey;
    }
  }

  private static final class HostQueue {

    final ArrayDeque<PooledConnection> queue = new ArrayDeque<>();

    final Object lock = new Object();
  }

  private final ConcurrentHashMap<String, HostQueue> hostCaches
      = new ConcurrentHashMap<>();

  private final ConcurrentLinkedQueue<PooledConnection> clusterQueue
      = new ConcurrentLinkedQueue<>();

  private final AtomicInteger defaultSize = new AtomicInteger(0);

  private final AtomicInteger clusterSize = new AtomicInteger(0);

  boolean isClosed() {
    return closed.get();
  }

  int getCachedDefaultSize() {
    return defaultSize.get();
  }

  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final int maxDefaultConnections;

  private final int maxClusterConnections;

  private final String accountNamePath;

  private ExecutorService singleThreadPool;

  ExecutorService getFixedThreadPool() {
    return fixedThreadPool;
  }

  ExecutorService getSingleThreadPool() {
    return singleThreadPool;
  }

  private ExecutorService fixedThreadPool;

  KeepAliveCache(AbfsConfiguration abfsConfiguration) {
    this.accountNamePath = abfsConfiguration.getAccountName();
    this.maxDefaultConnections = abfsConfiguration.getApacheMaxCacheSize();
    this.maxClusterConnections = abfsConfiguration.getApacheMaxCacheSize();

    // Fix: Always use Daemon threads to prevent JVM hang on exit
    if (abfsConfiguration.getApacheCacheRefreshCount() > 0) {
      this.singleThreadPool = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "CacheRefreshThread");
        t.setDaemon(true);
        return t;
      });
    }

    int warmup = abfsConfiguration.getApacheCacheWarmupCount();
    int refresh = abfsConfiguration.getApacheCacheRefreshCount();
    if (warmup > 0 || refresh > 0) {
      this.fixedThreadPool = Executors.newFixedThreadPool(
          Math.min(5, Math.max(warmup, refresh)), r -> {
            Thread t = new Thread(r, "AsyncCacheConnectionThread");
            t.setDaemon(true);
            return t;
          });
    }
  }

  public HttpClientConnection get(String host,
      boolean isDefaultHost) throws IOException {
    if (closed.get()) {
      LOG.debug("Attempt to get connection from closed cache for account: {}",
          accountNamePath);
      throw new ClosedIOException(accountNamePath, KEEP_ALIVE_CACHE_CLOSED);
    }
    HostQueue hq = hostCaches.get(host);
    if (hq == null) {
      return null;
    }

    synchronized (hq.lock) {
      PooledConnection pooled;
      while ((pooled = hq.queue.poll()) != null) {
        // Attempt to "claim" it. If already claimed by eviction thread, skip.
        if (!pooled.inUse.compareAndSet(false, true)) {
          continue;
        }

        if (!isDefaultHost) {
          clusterSize.decrementAndGet();
          // clusterQueue.remove(pooled) is REMOVED - No more O(N)!
        } else {
          defaultSize.decrementAndGet();
        }

        if (pooled.conn.isOpen() && !pooled.conn.isStale()) {
          return pooled.conn;
        }
        closeQuietly(pooled.conn);
      }
    }
    return null;
  }

  public boolean put(HttpClientConnection conn, boolean isDefaultHost) {
    if (conn == null || closed.get() || !conn.isOpen() || conn.isStale()) {
      closeQuietly(conn);
      return false;
    }

    // Defensive casting check
    if (!(conn instanceof AbfsManagedApacheHttpConnection)) {
      closeQuietly(conn);
      return false;
    }

    String host = ((AbfsManagedApacheHttpConnection) conn).getTargetHost()
        .toHostString();
    PooledConnection pooled = new PooledConnection(conn, host);
    HostQueue hq = hostCaches.computeIfAbsent(host, k -> new HostQueue());

    return isDefaultHost ? putDefault(hq, pooled) : putCluster(hq, pooled);
  }

  private boolean putDefault(HostQueue hq, PooledConnection pooled) {
    if (maxDefaultConnections <= 0) {
      closeQuietly(pooled.conn);
      return false;
    }

    synchronized (hq.lock) {
      hq.queue.offer(pooled);
      if (defaultSize.incrementAndGet() > maxDefaultConnections) {
        // Evict oldest from THIS queue
        PooledConnection evicted = hq.queue.poll();
        if (evicted != null) {
          defaultSize.decrementAndGet();
          closeQuietly(evicted.conn);
        }
      }
    }
    return true;
  }

  private boolean putCluster(HostQueue hq, PooledConnection pooled) {
    if (maxClusterConnections <= 0) {
      closeQuietly(pooled.conn);
      return false;
    }

    // 1. Handle Global Eviction First (No nested locks)
    while (clusterSize.get() >= maxClusterConnections) {
      PooledConnection candidate = clusterQueue.poll();
      if (candidate == null) {
        break;
      }

      // Try to "claim" for eviction. If get() already claimed it, skip.
      if (candidate.inUse.compareAndSet(false, true)) {
        clusterSize.decrementAndGet();
        HostQueue evictHq = hostCaches.get(candidate.cacheKey);
        if (evictHq != null) {
          synchronized (evictHq.lock) {
            evictHq.queue.remove(candidate);
          }
        }
        closeQuietly(candidate.conn);
      }
    }

    // 2. Add new connection
    synchronized (hq.lock) {
      hq.queue.offer(pooled);
      clusterQueue.offer(pooled);
      clusterSize.incrementAndGet();
    }
    return true;
  }

  /**
   * Close the cache and all connections in it.
   */
  private void closeQuietly(HttpClientConnection conn) {
    try {
      if (conn != null) {
        conn.close();
      }
    } catch (IOException ignored) {
      // Ignore
    }
  }

  /**
   * @return maximum number of connections that can be cached.
   */
  @VisibleForTesting
  public int getMaxCacheConnections() {
    return maxDefaultConnections;
  }

  /**
   * @return maximum number of connections that can be cached.
   */
  @VisibleForTesting
  public int getMaxCacheConnectionsForHost() {
    return maxClusterConnections;
  }

  /**
   * Close the cache and all connections within it.
   */
  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {return;}

    // Shutdown pools first to stop incoming tasks
    if (singleThreadPool != null) {singleThreadPool.shutdownNow();}
    if (fixedThreadPool != null) {fixedThreadPool.shutdownNow();}

    hostCaches.values().forEach(hq -> {
      synchronized (hq.lock) {
        hq.queue.forEach(p -> closeQuietly(p.conn));
        hq.queue.clear();
      }
    });
    hostCaches.clear();
    clusterQueue.clear();
    clusterSize.set(0);
    defaultSize.set(0);
  }

  /**
   * @return String representation of the KeepAliveCache instance.
   */
  @Override
  public String toString() {
    return String.format("KeepAliveCache[closed=%s, size=%d, max=%d]",
        closed.get(), defaultSize.get() + clusterSize.get(),
        getMaxCacheConnections() + getMaxCacheConnectionsForHost());
  }
}
