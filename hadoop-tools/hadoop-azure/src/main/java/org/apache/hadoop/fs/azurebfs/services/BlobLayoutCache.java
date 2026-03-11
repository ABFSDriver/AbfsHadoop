/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import org.jspecify.annotations.NonNull;

import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutResponse;

public class BlobLayoutCache {

  private static class LayoutEntry {

    final BlobLayout layout;

    AtomicBoolean isLayoutPresent = new AtomicBoolean(true);

    final AtomicInteger activeStreams = new AtomicInteger(0);

    final AtomicLong lastDeregisteredTime = new AtomicLong(Long.MAX_VALUE);

    LayoutEntry(long contentLength) {
      this.layout = new BlobLayout(contentLength);
    }

    boolean isLayoutPresent() {
      return isLayoutPresent.get();
    }
  }

  /**
   * Caffeine Cache instance replacing ConcurrentHashMap.
   * Uses weighted eviction based on the number of ranges stored in each layout.
   */
  private final Cache<String, LayoutEntry> cache;

  private static final long IDLE_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(5);

  private static final long MAX_CACHE_WEIGHT = 100_000;

  private static volatile BlobLayoutCache INSTANCE = null;

  public final ConcurrentHashMap<String, CopyOnWriteArrayList<InFlightPromise>>
      promiseRegistry = new ConcurrentHashMap<>();

  public record InFlightPromise(long start, long end,
                                CompletableFuture<Void> future) {}

  private BlobLayoutCache() {
    this.cache = Caffeine.newBuilder()
        .maximumWeight(MAX_CACHE_WEIGHT)
        // Weight is determined by the number of cached ranges in the layout
        .weigher(
            (String key, LayoutEntry entry) -> entry.layout.getRangeMapSize())
        .expireAfter(new Expiry<String, LayoutEntry>() {
          @Override
          public long expireAfterCreate(@NonNull String key,
              @NonNull LayoutEntry value,
              long currentTime) {
            return TimeUnit.MILLISECONDS.toNanos(IDLE_TIMEOUT_MS);
          }

          @Override
          public long expireAfterUpdate(@NonNull String key,
              @NonNull LayoutEntry value,
              long currentTime,
              long currentDuration) {
            return currentDuration;
          }

          @Override
          public long expireAfterRead(@NonNull String key,
              @NonNull LayoutEntry value,
              long currentTime,
              long currentDuration) {
            // LEASE LOGIC: If streams are active, the entry never expires.
            // Once activeStreams == 0, the 5-minute idle clock starts.
            return value.activeStreams.get() > 0
                ? Long.MAX_VALUE
                : TimeUnit.MILLISECONDS.toNanos(IDLE_TIMEOUT_MS);
          }
        })
        .removalListener((key, value, cause) -> {
          // Cleanup corresponding promises when an entry is evicted from cache
          if (key != null) {
            promiseRegistry.remove(key);
          }
        })
        .build();
  }

  public static BlobLayoutCache getInstance() {
    if (INSTANCE == null) {
      synchronized (BlobLayoutCache.class) {
        if (INSTANCE == null) {
          INSTANCE = new BlobLayoutCache();
        }
      }
    }
    return INSTANCE;
  }

  /**
   * Called by AbfsInputStream.init()
   */
  public void registerStream(String path, long contentLength) {
    cache.asMap().compute(path, (key, entry) -> {
      if (entry == null) {
        entry = new LayoutEntry(contentLength);
      }
      entry.activeStreams.incrementAndGet();
      entry.lastDeregisteredTime.set(Long.MAX_VALUE); // Block eviction logic
      return entry;
    });
  }

  /**
   * Called by AbfsInputStream.close()
   */
  public void deregisterStream(String path) {
    cache.asMap().computeIfPresent(path, (key, entry) -> {
      if (entry.activeStreams.decrementAndGet() <= 0) {
        entry.lastDeregisteredTime.set(System.currentTimeMillis());
      }
      return entry;
    });
  }

  public List<BlobLayout.BlobRange> getBlobLayout(final String key,
      final long start, final long end) {
    LayoutEntry layoutEntry = cache.getIfPresent(key);
    return (layoutEntry == null || !layoutEntry.isLayoutPresent())
        ? null : layoutEntry.layout.getRanges(start, end);
  }

  public List<BlobLayout.BlobRange> getGaps(final String key,
      final long start, final long end) {
    LayoutEntry layoutEntry = cache.getIfPresent(key);
    return layoutEntry == null || !layoutEntry.isLayoutPresent()
        ? null : layoutEntry.layout.getGaps(start, end);
  }

  public BlobLayout.BlobRange getBridgeGap(String path,
      long pos,
      long maxFetch) {
    LayoutEntry layoutEntry = cache.getIfPresent(path);
    if (layoutEntry == null || !layoutEntry.isLayoutPresent()) {
      return null;
    }

    long fetchStart = layoutEntry.layout.getFetchStart(pos);
    if (fetchStart == -1) {
      return null;
    }

    long nextPromisedStart = Long.MAX_VALUE;
    CopyOnWriteArrayList<InFlightPromise> filePromises = promiseRegistry.get(
        path);
    if (filePromises != null) {
      for (InFlightPromise p : filePromises) {
        if (p.start() > pos && p.start() < nextPromisedStart) {
          nextPromisedStart = p.start();
        }
      }
    }

    long nextCachedStart = layoutEntry.layout.getNextCachedStart(pos);
    long nextWall = Math.min(nextCachedStart, nextPromisedStart);

    long fetchEnd;
    if (nextWall != Long.MAX_VALUE && (nextWall - fetchStart <= maxFetch)) {
      fetchEnd = nextWall - 1;
    } else {
      fetchEnd = Math.min(layoutEntry.layout.getContentLength() - 1,
          fetchStart + maxFetch - 1);
    }

    return new BlobLayout.BlobRange(fetchStart, fetchEnd, null);
  }

  public void putBlobLayout(final String key,
      final BlobLayoutResponse layoutResponse,
      final long contentLength) {
    // Ensure entry exists and update the layout
    LayoutEntry layoutEntry = cache.asMap().computeIfAbsent(key,
        k -> new LayoutEntry(contentLength));
    if (layoutResponse == null) {
      layoutEntry.isLayoutPresent = new AtomicBoolean(false);
      return;
    }

    Map<Integer, String> endpointValueMap = layoutResponse.getEndpoints()
        .stream()
        .collect(Collectors.toMap(
            endpoint -> endpoint.index,
            endpoint -> endpoint.value,
            (existing, replacement) -> existing
        ));

    layoutEntry.layout.addRange(layoutResponse.getRanges(), endpointValueMap);
  }

  public void removePromise(String path, long start, long end) {
    promiseRegistry.computeIfPresent(path, (key, list) -> {
      list.removeIf(p -> p.start() == start && p.end() == end);
      return list.isEmpty() ? null : list;
    });
  }
}