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
        new BlobLayoutResponse.Range(0, 4, 1),
        new BlobLayoutResponse.Range(0, 8, 1),
        new BlobLayoutResponse.Range(4, 8, 1),
        new BlobLayoutResponse.Range(6, 10, 1)
    );

    layout.addRange(input, hostMap);
    return layout;
  }
}
