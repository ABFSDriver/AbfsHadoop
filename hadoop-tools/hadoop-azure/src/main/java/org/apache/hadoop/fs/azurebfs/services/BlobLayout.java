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
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutResponse;

public class BlobLayout {

  public record BlobRange(long start, long end, String host) {}

  private final ConcurrentSkipListMap<Long, BlobRange> rangeMap;

  private final long contentLength;

  public long getContentLength() {
    return contentLength;
  }

  public BlobLayout(final long contentLength) {
    this.rangeMap = new ConcurrentSkipListMap<>();
    this.contentLength = contentLength;
  }

  /**
   * Fast, lock-free write. Multiple threads can write different ranges
   * simultaneously.
   */
  public void addRange(List<BlobLayoutResponse.Range> ranges,
      final Map<Integer, String> endpointValueMap) {
    for (var range : ranges) {
      String host = endpointValueMap.get(range.endpointIndex());
      if (host != null) {
        // We use put() directly. If a range with the same start exists,
        // it is updated. Overlapping starts are handled during read.
        rangeMap.put(
            range.start(), new BlobRange(range.start(), range.end(), host));
      }
    }
  }

  /**
   * Merges fragmented data on-the-fly and clips the result to the
   * requested [start, end] window.
   */
  public List<BlobRange> getRanges(long start, long end) {
    List<BlobRange> merged = getMergedRangesInternal(start, end);
    List<BlobRange> clipped = new ArrayList<>();

    for (BlobRange range : merged) {
      // Clip the range to the requested window
      long clippedStart = Math.max(range.start(), start);
      long clippedEnd = Math.min(range.end(), end);

      if (clippedStart <= clippedEnd) {
        clipped.add(new BlobRange(clippedStart, clippedEnd, range.host()));
      }
    }
    return clipped;
  }

  /**
   * Identifies exact gaps for surgical fetching.
   */
  public List<BlobRange> getGaps(long start, long end) {
    List<BlobRange> gaps = new ArrayList<>();
    if (start >= contentLength) {return gaps;}

    long effectiveEnd = Math.min(end, contentLength - 1);
    long currentPos = start;

    // Use the merged view to find gaps accurately
    List<BlobRange> existing = getMergedRangesInternal(start, effectiveEnd);

    for (BlobRange range : existing) {
      if (range.start() > currentPos) {
        gaps.add(new BlobRange(currentPos, range.start() - 1, null));
      }
      currentPos = Math.max(currentPos, range.end() + 1);
    }

    if (currentPos <= effectiveEnd) {
      gaps.add(new BlobRange(currentPos, effectiveEnd, null));
    }
    return gaps;
  }

  public BlobRange getBridgeGap(long pos, long maxFetchSize) {
    // 1. Find the cached blocks surrounding the current position
    Map.Entry<Long, BlobRange> floorEntry = rangeMap.floorEntry(pos);
    Map.Entry<Long, BlobRange> ceilingEntry = rangeMap.higherEntry(pos);

    // If already cached, no fetch needed
    if (floorEntry != null && floorEntry.getValue().end() >= pos) {
      return null;
    }

    // 2. Determine Fetch Start: Right after the previous block
    long fetchStart = (floorEntry != null)
        ? floorEntry.getValue().end() + 1
        : 0;

    // 3. Determine Fetch End:
    long fetchEnd = getFetchEnd(maxFetchSize, ceilingEntry, fetchStart);

    return new BlobRange(fetchStart, fetchEnd, null);
  }

  public long getNextCachedStart(long pos) {
    Map.Entry<Long, BlobRange> ceilingEntry = rangeMap.higherEntry(pos);
    return (ceilingEntry != null)
        ? ceilingEntry.getValue().start()
        : Long.MAX_VALUE;
  }

  public long getFetchStart(long pos) {
    Map.Entry<Long, BlobRange> floorEntry = rangeMap.floorEntry(pos);
    if (floorEntry != null && floorEntry.getValue().end() >= pos) {
      return -1; // Already cached
    }
    return (floorEntry != null) ? floorEntry.getValue().end() + 1 : 0;
  }

  private long getFetchEnd(final long maxFetchSize,
      final Map.Entry<Long, BlobRange> ceilingEntry,
      final long fetchStart) {
    long fetchEnd;
    if (ceilingEntry != null) {
      // There is a block ahead. Try to bridge the gap entirely.
      long nextBlockStart = ceilingEntry.getValue().start();

      // If the hole is small (e.g., < 64MB), bridge it perfectly
      if (nextBlockStart - fetchStart <= maxFetchSize) {
        fetchEnd = nextBlockStart - 1;
      } else {
        // Hole is too big, just take a 64MB slice
        fetchEnd = fetchStart + maxFetchSize - 1;
      }
    } else {
      // No block ahead, fetch a 64MB slice or up to EOF
      fetchEnd = Math.min(contentLength - 1, fetchStart + maxFetchSize - 1);
    }
    return fetchEnd;
  }

  /**
   * Internal logic to combine overlapping/adjacent ranges from the map.
   */
  private List<BlobRange> getMergedRangesInternal(long start, long end) {
    Map.Entry<Long, BlobRange> floorEntry = rangeMap.floorEntry(start);
    long searchStart = (floorEntry != null) ? floorEntry.getKey() : start;

    var potentialMatches = rangeMap.subMap(searchStart, true, end, true)
        .values();
    List<BlobRange> merged = new ArrayList<>();
    BlobRange current = null;

    for (BlobRange next : potentialMatches) {
      if (next.end() < start) {
        continue; // Skip ranges that end before our window
      }

      if (current == null) {
        current = next;
      } else {
        // Merge if overlapping or adjacent AND same host
        if (next.start() <= current.end() + 1 && next.host()
            .equals(current.host())) {
          current = new BlobRange(current.start(),
              Math.max(current.end(), next.end()), current.host());
        } else {
          merged.add(current);
          current = next;
        }
      }
    }
    if (current != null) {merged.add(current);}
    return merged;
  }

  public int getRangeMapSize() {
    return rangeMap.size();
  }
}