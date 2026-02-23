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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.VisibleForTesting;

public class BlobLayoutResponse {

  public List<Range> ranges = new ArrayList<>();
  public Set<Endpoint> endpoints = new HashSet<>();

  // TODO: Add Support for Read Keys Based on Data View.

  public String nextMarker;
  public String maxResults;

  public static class Range {
    public long start;
    public long end;
    public int endpointIndex;

    public Range() {
      super();
    }

    @VisibleForTesting
    public Range(long start, long end, int endpointIndex) {
      this.start = start;
      this.end = end;
      this.endpointIndex = endpointIndex;
    }

    @Override
    public String toString() {
      return "Range{" +
          "Start=" + start +
          ", End=" + end +
          ", EndpointIndex=" + endpointIndex +
          '}';
    }
  }

  public static class Endpoint {
    public int index;
    public String value;

    @Override
    public String toString() {
      return "Endpoint{" +
          "Index=" + index +
          ", Value='" + value + '\'' +
          '}';
    }
  }

  public Set<Endpoint> getEndpoints() {
    return endpoints;
  }

  public void setEndpoints(final Set<Endpoint> endpoints) {
    this.endpoints = endpoints;
  }

  public String getNextMarker() {
    return nextMarker;
  }

  public void setNextMarker(final String nextMarker) {
    this.nextMarker = nextMarker;
  }

  public String getMaxResults() {
    return maxResults;
  }

  public void setMaxResults(final String maxResults) {
    this.maxResults = maxResults;
  }

  public List<Range> getRanges() {
    return ranges;
  }

  public void setRanges(final List<Range> ranges) {
    this.ranges = ranges;
  }

  public String getReadEndpoint(int index) {
    for (Endpoint endpoint : endpoints) {
      if (endpoint.index == index) {
        return endpoint.value;
      }
    }
    return null;
  }

  public void addBlobLayoutResponse(BlobLayoutResponse newResp) {
    // Merge ranges (allow duplicates)
    this.ranges.addAll(newResp.getRanges());

    // Merge endpoints (remove duplicates by index)
    this.endpoints.addAll(newResp.getEndpoints());

    // Overwrite nextMarker and maxResults with newResp's values
    this.nextMarker = newResp.getNextMarker();
    this.maxResults = newResp.getMaxResults();
  }
}
