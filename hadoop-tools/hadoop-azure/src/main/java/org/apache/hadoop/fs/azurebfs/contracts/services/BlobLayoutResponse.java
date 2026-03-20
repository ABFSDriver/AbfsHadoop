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


public class BlobLayoutResponse {

  private List<Range> ranges = new ArrayList<>();

  private Set<Endpoint> endpoints = new HashSet<>();

  private String nextMarker;

  private String maxResults;

  public record Range(long start, long end, int endpointIndex) {

    @Override
    public String toString() {
      return "Range{" +
          "Start=" + start +
          ", End=" + end +
          ", EndpointIndex=" + endpointIndex +
          '}';
    }
  }

  public record Endpoint(int index, String value) {

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

  public void addEndpoint(final Endpoint endpoint) {
    this.endpoints.add(endpoint);
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

  public void addRange(final Range range) {
    this.ranges.add(range);
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
    setRanges(newResp.getRanges());

    // Merge endpoints (remove duplicates by index)
    setEndpoints(newResp.getEndpoints());

    // Overwrite nextMarker and maxResults with newResp's values
    this.nextMarker = newResp.getNextMarker();
    this.maxResults = newResp.getMaxResults();
  }
}
