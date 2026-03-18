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

import static org.apache.hadoop.fs.azurebfs.services.AbfsInputStream.LOG;

/**
 * BlobLayoutCache is a singleton class that manages caching of BlobLayout objects for Azure Blob File System (ABFS).
 * It uses a Caffeine cache to store layout information for blobs, optimizing range fetches and minimizing redundant network calls.
 *
 * <p>Key Features:</p>
 * <ul>
 *   <li>Evicts cache entries based on idle timeout and cache weight (number of cached ranges).</li>
 *   <li>Tracks active streams to prevent eviction while a stream is open.</li>
 *   <li>Manages in-flight promises for range fetches to avoid duplicate requests.</li>
 *   <li>Provides methods to register/deregister streams, retrieve cached layouts, compute gaps, and manage promises.</li>
 * </ul>
 *
 * <p>Usage:</p>
 * <ul>
 *   <li>Call {@link #registerStream(String, long)} when a stream is opened.</li>
 *   <li>Call {@link #deregisterStream(String)} when a stream is closed.</li>
 *   <li>Use {@link #getBlobLayout(String, long, long)} and {@link #getGaps(String, long, long)} to query cached layout info.</li>
 *   <li>Use {@link #putBlobLayout(String, BlobLayoutResponse, long)} to update the cache with new layout info.</li>
 *   <li>Use {@link #getBridgeGap(String, long, long)} to determine the next range to fetch, considering in-flight and cached ranges.</li>
 *   <li>Use {@link #removePromise(String, long, long)} to remove completed in-flight promises.</li>
 * </ul>
 */
public class BlobLayoutCache {

  /**
   * LayoutEntry represents a cache entry for a blob's layout.
   * It tracks the BlobLayout, active stream count, and last deregistered time for eviction logic.
   */
  private static class LayoutEntry {

    /** The BlobLayout instance for this entry. */
    final BlobLayout layout;

    /** Indicates if the layout is present and valid. */
    AtomicBoolean isLayoutPresent = new AtomicBoolean(true);

    /** Number of active streams using this layout. */
    final AtomicInteger activeStreams = new AtomicInteger(0);

    /** Last time (in nanoseconds) when all streams were deregistered. Used for idle eviction. */
    final AtomicLong lastDeregisteredTimeInNanos = new AtomicLong(
        Long.MAX_VALUE);

    /**
     * Constructs a LayoutEntry with the given content length.
     * @param contentLength the length of the blob content
     */
    LayoutEntry(long contentLength) {
      this.layout = new BlobLayout(contentLength);
    }

    /**
     * Checks if the layout is unavailable (not present).
     * @return true if layout is unavailable, false otherwise
     */
    boolean isLayoutUnavailable() {
      return !isLayoutPresent.get();
    }
  }

  /**
   * Caffeine Cache instance replacing ConcurrentHashMap.
   * Uses weighted eviction based on the number of ranges stored in each layout.
   */
  private final Cache<String, LayoutEntry> cache;

  /** Idle timeout in milliseconds for cache eviction. */
  private final long IDLE_TIMEOUT_MS;

  /** Maximum total weight (number of cached ranges) allowed in the cache. */
  private static final long MAX_CACHE_WEIGHT = 100_000;

  /** Singleton instance of BlobLayoutCache. */
  private static volatile BlobLayoutCache INSTANCE = null;

  /**
   * Registry of in-flight promises for range fetches, keyed by file ETag.
   * Each value is a thread-safe list of InFlightPromise objects.
   */
  public final ConcurrentHashMap<String, CopyOnWriteArrayList<InFlightPromise>>
      promiseRegistry = new ConcurrentHashMap<>();

  /**
   * InFlightPromise represents a pending fetch for a specific range.
   * @param start the start offset of the range
   * @param end the end offset of the range
   * @param future the CompletableFuture representing the fetch operation
   */
  public record InFlightPromise(long start, long end,
                                CompletableFuture<Void> future) {}

  /**
   * Private constructor for singleton pattern.
   * @param evictionTime the idle timeout in minutes for cache eviction
   */
  private BlobLayoutCache(Long evictionTime) {
    IDLE_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(evictionTime);
    this.cache = Caffeine.newBuilder()
        .maximumWeight(MAX_CACHE_WEIGHT)
        // Weight is determined by the number of cached ranges in the layout
        .weigher(
            (String key, LayoutEntry entry) -> entry.layout.getRangeMapSize())
        .expireAfter(new Expiry<String, LayoutEntry>() {
          /**
           * Calculates the expiry time for a cache entry based on active streams and idle time.
           * @param value the LayoutEntry
           * @param currentTime the current time in nanoseconds
           * @return the time in nanoseconds until expiry
           */
          private long calculateExpiry(LayoutEntry value, long currentTime) {
            if (value.activeStreams.get() > 0) {
              return Long.MAX_VALUE; // Do not expire while active
            }

            // How long has it been since the last stream closed?
            long elapsedNanos = currentTime
                - value.lastDeregisteredTimeInNanos.get();
            long thresholdNanos = TimeUnit.MILLISECONDS.toNanos(
                IDLE_TIMEOUT_MS);

            // If we are already past the threshold, expire immediately (return 0)
            // Otherwise, return the remaining time until the threshold is hit
            return Math.max(0, thresholdNanos - elapsedNanos);
          }

          @Override
          public long expireAfterCreate(@NonNull String key,
              @NonNull LayoutEntry value, long currentTime) {
            return calculateExpiry(value, currentTime);
          }

          @Override
          public long expireAfterUpdate(@NonNull String key,
              @NonNull LayoutEntry value, long currentTime,
              long currentDuration) {
            return calculateExpiry(value, currentTime);
          }

          @Override
          public long expireAfterRead(@NonNull String key,
              @NonNull LayoutEntry value, long currentTime,
              long currentDuration) {
            // LEASE LOGIC: If streams are active, the entry never expires.
            // Once activeStreams == 0, the 5-minute idle clock starts.
            return calculateExpiry(value, currentTime);
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

  /**
   * Returns the singleton instance of BlobLayoutCache, creating it if necessary.
   * @param evictionTime the idle timeout in minutes for cache eviction
   * @return the singleton BlobLayoutCache instance
   */
  public static BlobLayoutCache getInstance(long evictionTime) {
    if (INSTANCE == null) {
      synchronized (BlobLayoutCache.class) {
        if (INSTANCE == null) {
          INSTANCE = new BlobLayoutCache(evictionTime);
        }
      }
    }
    return INSTANCE;
  }

  /**
   * Registers a stream for the given file ETag and content length.
   * Increments the active stream count and blocks eviction.
   * @param fileETag the file ETag
   * @param contentLength the content length of the blob
   */
  public void registerStream(String fileETag, long contentLength) {
    cache.asMap().compute(fileETag, (key, entry) -> {
      if (entry == null) {
        entry = new LayoutEntry(contentLength);
      }
      entry.activeStreams.incrementAndGet();
      entry.lastDeregisteredTimeInNanos.set(
          Long.MAX_VALUE); // Block eviction logic
      return entry;
    });
  }

  /**
   * Deregisters a stream for the given file ETag.
   * Decrements the active stream count and updates the last deregistered time for eviction.
   * @param fileETag the file ETag
   */
  public void deregisterStream(String fileETag) {
    cache.asMap().computeIfPresent(fileETag, (key, entry) -> {
      if (entry.activeStreams.decrementAndGet() <= 0) {
        entry.lastDeregisteredTimeInNanos.set(System.nanoTime());
      }
      return entry;
    });
  }

  /**
   * Retrieves the cached blob layout ranges for the specified key and range.
   * @param key the cache key (file ETag)
   * @param start the start offset
   * @param end the end offset
   * @return a list of BlobRange objects, or null if not available
   */
  public List<BlobLayout.BlobRange> getBlobLayout(final String key,
      final long start, final long end) {
    LayoutEntry layoutEntry = cache.getIfPresent(key);
    return (layoutEntry == null || layoutEntry.isLayoutUnavailable())
        ? null : layoutEntry.layout.getRanges(start, end);
  }

  /**
   * Retrieves the gaps (unfetched ranges) in the cached layout for the specified key and range.
   * @param key the cache key (file ETag)
   * @param start the start offset
   * @param end the end offset
   * @return a list of BlobRange objects representing gaps, or null if not available
   */
  public List<BlobLayout.BlobRange> getGaps(final String key,
      final long start, final long end) {
    LayoutEntry layoutEntry = cache.getIfPresent(key);
    return layoutEntry == null || layoutEntry.isLayoutUnavailable()
        ? null : layoutEntry.layout.getGaps(start, end);
  }

  /**
   * Determines the next range to fetch (bridge gap) for the given file ETag, position, and max fetch size.
   * Considers both cached and in-flight (promised) ranges to avoid redundant fetches.
   * @param fileETag the file ETag
   * @param pos the current position
   * @param maxFetch the maximum fetch size
   * @return a BlobRange representing the next gap to fetch, or null if none
   */
  public BlobLayout.BlobRange getBridgeGap(String fileETag,
      long pos,
      long maxFetch) {
    LayoutEntry layoutEntry = cache.getIfPresent(fileETag);
    if (layoutEntry == null || layoutEntry.isLayoutUnavailable()) {
      return null;
    }

    long fetchStart = layoutEntry.layout.getFetchStart(pos, maxFetch);
    if (fetchStart == -1) {
      return null;
    }

    long nextPromisedStart = Long.MAX_VALUE;
    CopyOnWriteArrayList<InFlightPromise> filePromises = promiseRegistry.get(
        fileETag);
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

  /**
   * Updates the cache with a new BlobLayoutResponse for the specified key.
   * If the response is null, marks the layout as unavailable.
   * @param key the cache key (file ETag)
   * @param layoutResponse the BlobLayoutResponse containing new layout info
   * @param contentLength the content length of the blob
   */
  public void putBlobLayout(final String key,
      final BlobLayoutResponse layoutResponse,
      final long contentLength) {
    // Ensure entry exists and update the layout
    LayoutEntry layoutEntry = cache.asMap().computeIfAbsent(key,
        k -> new LayoutEntry(contentLength));
    if (layoutResponse == null) {
      LOG.debug("Layout response is null for key: {}. Skipping cache update.",
          key);
      layoutEntry.isLayoutPresent = new AtomicBoolean(false);
      return;
    }

    Map<Integer, String> endpointValueMap = layoutResponse.getEndpoints()
        .stream()
        .collect(Collectors.toMap(
            BlobLayoutResponse.Endpoint::index,
            BlobLayoutResponse.Endpoint::value,
            (existing, replacement) -> existing
        ));

    layoutEntry.layout.addRange(layoutResponse.getRanges(), endpointValueMap);
  }

  /**
   * Removes a completed in-flight promise for the specified file ETag and range.
   * Cleans up the promise registry if no promises remain for the file.
   * @param fileETag the file ETag
   * @param start the start offset of the range
   * @param end the end offset of the range
   */
  public void removePromise(String fileETag, long start, long end) {
    promiseRegistry.computeIfPresent(fileETag, (key, list) -> {
      list.removeIf(p -> p.start() == start && p.end() == end);
      return list.isEmpty() ? null : list;
    });
  }
}