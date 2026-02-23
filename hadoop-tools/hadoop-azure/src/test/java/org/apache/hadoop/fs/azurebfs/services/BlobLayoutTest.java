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

import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BlobLayoutTest {

  @Test
  public void testOverlappingFragmentedMerging() {
    // Content length doesn't matter much for this specific test
    BlobLayout layout = getBlobLayout();

    // 2. Query exactly 0-2
    List<BlobRange> result = layout.getRanges(0, 2);

    // Verification
    Assertions.assertEquals(1, result.size(),
        "Expected exactly one merged range");
    BlobRange range = result.get(0);

    // Even though the internal merged block is 0-10, the output must be clipped to the request
    Assertions.assertEquals(0, range.start(), "Start should be clipped to 0");
    Assertions.assertEquals(2, range.end(), "End should be clipped to 2");
    Assertions.assertEquals("host-a", range.host(), "Host should be preserved");

    // 3. Query 5-9 (to check mid-block clipping)
    List<BlobRange> result2 = layout.getRanges(5, 9);
    Assertions.assertEquals(1, result2.size());
    Assertions.assertEquals(5, result2.get(0).start());
    Assertions.assertEquals(9, result2.get(0).end());
  }

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