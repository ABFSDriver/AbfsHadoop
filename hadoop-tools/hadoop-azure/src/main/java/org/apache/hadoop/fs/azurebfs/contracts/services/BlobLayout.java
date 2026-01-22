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

package org.apache.hadoop.fs.azurebfs.contracts.services;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BlobLayout {

  List<Range> ranges;
  List<Endpoint> endpoints;
  List<ReadKey> readKeys;

  public BlobLayout() {
    ranges = new ArrayList<>(Arrays.asList(
        new Range(0, 1000000, 0, null),
        new Range(1000001, 2124270, 0, null),
        new Range(2124271, 2924270, 0, null),
        new Range(2924271, 4340031, 0, null),
        new Range(4340032, 5340031, 0, null),
        new Range(5340032, 6340031, 0, null),
        new Range(6340032, 7340031, 0, null),
        new Range(7340032, 10000000000L, 0, null)
    ));
    endpoints = new ArrayList<>(Arrays.asList(
        new Endpoint(0, "https://example.blob.core.windows.net/container/blob")
    ));
    readKeys = new ArrayList<>(Arrays.asList(
        new ReadKey(0, "readKeyExample")
    ));
  }

  public List<Range> getRanges() {
    return ranges;
  }

  public void setRanges(final List<Range> ranges) {
    this.ranges = ranges;
  }

  public List<Endpoint> getEndpoints() {
    return endpoints;
  }

  public void setEndpoints(final List<Endpoint> endpoints) {
    this.endpoints = endpoints;
  }

  public static class Range {

    public long start;
    public long end;
    public int endpointIndex;
    List<Integer> readKeyIds;

    public Range(long start, long end, int endpointIndex, List<Integer> readKeyIds) {
      this.start = start;
      this.end = end;
      this.endpointIndex = endpointIndex;
      this.readKeyIds = readKeyIds;
    }
  }

  public static class Endpoint {

    public int index;
    public String endpoint;

    public Endpoint(int index, String endpoint) {
      this.index = index;
      this.endpoint = endpoint;
    }
  }

  public static class ReadKey {

    public int id;
    public String readKey;

    public ReadKey(int id, String readKey) {
      this.id = id;
      this.readKey = readKey;
    }
  }
}
