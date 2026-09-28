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

import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutResponse;
import org.apache.hadoop.fs.azurebfs.services.BlobLayout.BlobRange;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link BlobLayout} class, focusing on the merging and clipping of overlapping blob ranges.
 * <p>
 * These tests verify that the BlobLayout correctly merges overlapping ranges and clips the output
 * to the requested range, preserving the host information. The test scenarios are based on a specific
 * set of overlapping input ranges and validate both edge and mid-block clipping behavior.
 */
public class BlobLayoutTest {

  /**
   * Verifies that overlapping and fragmented blob ranges are merged correctly and that the
   * returned range is clipped to the requested bounds. Also checks that the host information
   * is preserved in the merged result.
   *
   * Scenario:
   * - Adds overlapping ranges to the BlobLayout.
   * - Queries a sub-range (0-2) and expects a single merged and clipped range.
   * - Queries a mid-block range (5-9) and expects a single merged and clipped range.
   */
  @Test
  public void testOverlappingFragmentedMerging() {
    // Content length doesn't matter much for this specific test
    BlobLayout layout = getBlobLayout();

    // 2. Query exactly 0-2
    List<BlobRange> result = layout.getRanges(0, 2);

    // Verification
    Assertions.assertThat(result.size())
        .describedAs("Expected exactly one merged range")
        .isEqualTo(1);
    BlobRange range = result.get(0);

    // Even though the internal merged block is 0-10, the output must be clipped to the request
    Assertions.assertThat(range.start())
        .describedAs("Start should be clipped to 0")
        .isEqualTo(0);
    Assertions.assertThat(range.end())
        .describedAs("End should be clipped to 2")
        .isEqualTo(2);
    Assertions.assertThat(range.host())
        .describedAs("Host should be preserved")
        .isEqualTo("host-a");

    // 3. Query 5-9 (to check mid-block clipping)
    List<BlobRange> result2 = layout.getRanges(5, 9);
    Assertions.assertThat(result2.size())
        .describedAs("Expected exactly one merged range")
        .isEqualTo(1);
    Assertions.assertThat(result2.get(0).start())
        .describedAs("Start should be clipped to 5")
        .isEqualTo(5);
    Assertions.assertThat(result2.get(0).end())
        .describedAs("End should be clipped to 9")
        .isEqualTo(9);
  }

  /**
   * Helper method to create a {@link BlobLayout} instance pre-populated with a specific set of
   * overlapping ranges and a dummy host map. The ranges are designed to test merging and clipping logic.
   *
   * @return a BlobLayout instance with predefined ranges and host mapping
   */
  private static @NonNull BlobLayout getBlobLayout() {
    BlobLayout layout = new BlobLayout(100);

    // Define a dummy host map
    Map<Integer, String> hostMap = new HashMap<>();
    hostMap.put(1, "host-a");

    // 1. Populate the cache with your specific overlapping scenario
    // 0-4 -> h, 0-8 -> h, 4-8 -> h, 6-10 -> h
    List<BlobLayoutResponse.Range> input = List.of(
        new BlobLayoutResponse.Range(0, 4, 1, null, 0L),
        new BlobLayoutResponse.Range(0, 8, 1, null, 0L),
        new BlobLayoutResponse.Range(4, 8, 1, null, 0L),
        new BlobLayoutResponse.Range(6, 10, 1, null, 0L)
    );

    layout.addRange(input, hostMap);
    return layout;
  }

  /**
   * Verifies that invalidating part of a stored range removes the whole
   * stored range, because its handle was issued for the full range.
   */
  @Test
  public void testInvalidateRangeRemovesWholeOverlappingRange() {
    BlobLayout layout = createTwoRangeLayout();

    layout.invalidateRange(10, 20);

    Assertions.assertThat(layout.getRanges(0, 49))
        .describedAs("The 0-49 range overlaps 10-20 and should be removed")
        .isEmpty();
    Assertions.assertThat(layout.getRanges(50, 99))
        .describedAs("The 50-99 range does not overlap and should remain")
        .hasSize(1);
    Assertions.assertThat(layout.getRangeMapSize())
        .describedAs("Only one stored range should remain")
        .isEqualTo(1);
  }

  /**
   * Verifies that range boundaries are inclusive when invalidating.
   */
  @Test
  public void testInvalidateRangeBoundariesAreInclusive() {
    BlobLayout layout = createTwoRangeLayout();

    // The last byte of the first range.
    layout.invalidateRange(49, 49);
    Assertions.assertThat(layout.getRanges(0, 49))
        .describedAs("Byte 49 is inside 0-49, so that range should be removed")
        .isEmpty();
    Assertions.assertThat(layout.getRanges(50, 99))
        .describedAs("Byte 49 is outside 50-99, so that range should remain")
        .hasSize(1);

    // The first byte of the second range.
    layout.invalidateRange(50, 50);
    Assertions.assertThat(layout.getRangeMapSize())
        .describedAs("Byte 50 is inside 50-99, so that range should be removed")
        .isZero();
  }

  /**
   * Verifies that an interval spanning several stored ranges removes all of
   * them.
   */
  @Test
  public void testInvalidateRangeSpanningMultipleRanges() {
    BlobLayout layout = createTwoRangeLayout();

    layout.invalidateRange(40, 60);

    Assertions.assertThat(layout.getRangeMapSize())
        .describedAs("Both ranges overlap 40-60 and should be removed")
        .isZero();
  }

  /**
   * Verifies that invalidating an interval with no cached ranges is a no-op.
   */
  @Test
  public void testInvalidateRangeWithNoOverlapIsNoOp() {
    BlobLayout emptyLayout = new BlobLayout(100);
    emptyLayout.invalidateRange(0, 99);
    Assertions.assertThat(emptyLayout.getRangeMapSize())
        .describedAs("Invalidating an empty layout should not fail")
        .isZero();

    BlobLayout layout = new BlobLayout(200);
    layout.addRange(
        List.of(new BlobLayoutResponse.Range(0, 49, 0, "handle-1",
            futureExpiry())),
        Map.of(0, "host-a"));

    layout.invalidateRange(100, 199);

    Assertions.assertThat(layout.getRangeMapSize())
        .describedAs("A non-overlapping interval should not remove anything")
        .isEqualTo(1);
  }

  /**
   * Verifies that an invalidated range is reported as a gap, which is what
   * makes getBlobRanges() fetch a fresh layout.
   */
  @Test
  public void testInvalidatedRangeIsReportedAsGap() {
    BlobLayout layout = createTwoRangeLayout();
    Assertions.assertThat(layout.getGaps(0, 99))
        .describedAs("A fully cached layout should have no gaps")
        .isEmpty();

    layout.invalidateRange(0, 10);

    List<BlobRange> gaps = layout.getGaps(0, 99);
    Assertions.assertThat(gaps)
        .describedAs("The invalidated range should become a single gap")
        .containsExactly(new BlobRange(0, 49, null));
  }

  /**
   * Verifies that merging adjacent ranges keeps the handle and expiry. The
   * handle refresh check in AbfsInputStream depends on expiresAt surviving
   * the merge.
   */
  @Test
  public void testMergedRangeKeepsHandleAndExpiry() {
    long expiry = futureExpiry();
    BlobLayout layout = new BlobLayout(100);
    layout.addRange(
        List.of(
            new BlobLayoutResponse.Range(0, 49, 0, "handle-1", expiry),
            new BlobLayoutResponse.Range(50, 99, 0, "handle-1", expiry)),
        Map.of(0, "host-a"));

    List<BlobRange> ranges = layout.getRanges(0, 99);

    Assertions.assertThat(ranges)
        .describedAs("Adjacent ranges with the same target should merge")
        .hasSize(1);
    Assertions.assertThat(ranges.get(0).handle())
        .describedAs("The merged range should keep the handle")
        .isEqualTo("handle-1");
    Assertions.assertThat(ranges.get(0).expiresAt())
        .describedAs("The merged range should keep the expiry")
        .isEqualTo(expiry);
  }

  /**
   * Creates a 100-byte layout with two ranges on the same host, each with
   * its own handle, so they are not merged:
   * 0-49 carries handle-1 and 50-99 carries handle-2.
   */
  private static BlobLayout createTwoRangeLayout() {
    BlobLayout layout = new BlobLayout(100);
    layout.addRange(
        List.of(
            new BlobLayoutResponse.Range(0, 49, 0, "handle-1", futureExpiry()),
            new BlobLayoutResponse.Range(50, 99, 0, "handle-2", futureExpiry())),
        Map.of(0, "host-a"));
    return layout;
  }

  private static long futureExpiry() {
    return System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5);
  }
}
