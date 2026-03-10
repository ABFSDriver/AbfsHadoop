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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutResponse;

public class BlobLayoutCache {

  private final Map<String, BlobLayout> cache = new ConcurrentHashMap<>();

  private static BlobLayoutCache INSTANCE = null;

  public final ConcurrentHashMap<String, CopyOnWriteArrayList<InFlightPromise>>
      promiseRegistry = new ConcurrentHashMap<>();

  public record InFlightPromise(long start, long end,
                                CompletableFuture<Void> future) {}

  private BlobLayoutCache() {
  }

  public static synchronized BlobLayoutCache getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new BlobLayoutCache();
    }
    return INSTANCE;
  }

  public List<BlobLayout.BlobRange> getBlobLayout(final String key,
      final long start, final long end) {
    BlobLayout layout = cache.get(key);
    return (layout == null) ? null : layout.getRanges(start, end);
  }

  public List<BlobLayout.BlobRange> getGaps(final String key,
      final long start, final long end) {
    BlobLayout layout = cache.get(key);
    return layout == null ? null : layout.getGaps(start, end);
  }

  public BlobLayout.BlobRange getBridgeGap(String path,
      long pos,
      long maxFetch) {
    BlobLayout layout = cache.get(path);
    if (layout == null) {return null;}

    // 1. Get Fetch Start from the layout
    long fetchStart = layout.getFetchStart(pos);
    if (fetchStart == -1) {
      return null; // Already in memory
    }

    // 2. Find the "Wall" in the Promise Registry
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

    // 3. Find the "Wall" in the Cache
    long nextCachedStart = layout.getNextCachedStart(pos);

    // 4. Determine the closest Wall
    long nextWall = Math.min(nextCachedStart, nextPromisedStart);

    // 5. Calculate End
    long fetchEnd;
    if (nextWall != Long.MAX_VALUE && (nextWall - fetchStart <= maxFetch)) {
      fetchEnd = nextWall - 1; // Stitch perfectly
    } else {
      fetchEnd = Math.min(layout.getContentLength() - 1,
          fetchStart + maxFetch - 1);
    }

    return new BlobLayout.BlobRange(fetchStart, fetchEnd, null);
  }


  public void putBlobLayout(final String key,
      final BlobLayoutResponse layoutResponse,
      final long contentLength) {
    BlobLayout layout = cache.computeIfAbsent(key,
        k -> new BlobLayout(contentLength));

    Map<Integer, String> endpointValueMap = layoutResponse.getEndpoints()
        .stream()
        .collect(Collectors.toMap(
            endpoint -> endpoint.index,
            endpoint -> endpoint.value,
            (existing, replacement) -> existing
        ));

    // Batch update to keep the write-lock duration short
    layout.addRange(layoutResponse.getRanges(), endpointValueMap);
  }

  /**
   * Finds all futures that overlap with the requested range.
   */
  public List<CompletableFuture<Void>> getOverlappingFutures(String path,
      long start,
      long end) {
    List<CompletableFuture<Void>> overlapping = new ArrayList<>();
    CopyOnWriteArrayList<InFlightPromise> list = promiseRegistry.get(path);
    if (list != null) {
      for (InFlightPromise p : list) {
        if (start <= p.end() && end >= p.start()) {
          overlapping.add(p.future());
        }
      }
    }
    return overlapping;
  }

  /**
   * Checks if a specific position is already covered by a promise.
   */
  /**
   * Checks if a specific position is already covered by an in-flight promise.
   * * @param path The file identifier.
   * @param pos  The byte position to check.
   * @return true if an active fetch covers this position.
   */
  public boolean isPosPromised(String path, long pos) {
    CopyOnWriteArrayList<InFlightPromise> list = promiseRegistry.get(path);
    if (list == null || list.isEmpty()) {
      return false;
    }

    // Iterating over CopyOnWriteArrayList is thread-safe and more
    // performant than Stream.anyMatch in hot loops.
    for (InFlightPromise p : list) {
      if (pos >= p.start() && pos <= p.end()) {
        return true;
      }
    }
    return false;
  }

  public void removePromise(String path, long start, long end) {
    promiseRegistry.computeIfPresent(path, (key, list) -> {
      // Use removeIf for thread-safe removal from the CopyOnWriteArrayList
      list.removeIf(p -> p.start() == start && p.end() == end);

      // Return null if empty to remove the path from the ConcurrentHashMap entirely
      return list.isEmpty() ? null : list;
    });
  }
}