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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * Parses the DFS endpoint's JSON layout response into a
 * {@link BlobLayoutResponse}.
 * <p>
 * The DFS endpoint returns {@code action=getLayout} as compact
 * {@code application/json;charset=utf-8} with this shape:
 * </p>
 * <pre>
 * {
 *   "ranges": [{"start": 0, "end": 1023, "endpointIndex": 0}],
 *   "endpoints": [{"index": 0, "value": "https://{dfs-data-endpoint}/"}],
 *   "dataHandle": "&lt;opaque&gt;",
 *   "dataHandleExpiry": "2026-08-06T07:34:49Z",
 *   "nextMarker": ""
 * }
 * </pre>
 * <p>
 * {@code marker} and {@code maxResults} may precede {@code ranges} but the DFS
 * processor does not currently populate them. {@code dataHandle} and
 * {@code dataHandleExpiry} are emitted together or both omitted, and are
 * top-level siblings rather than properties of a range - see
 * {@link LayoutResponseParser#applyDataHandle} for how the single handle is
 * fanned out across the ranges.
 * </p>
 * <p>
 * Parsing is order-independent: although the service emits properties in a
 * fixed order, this parser accepts them in any order so that a future field
 * addition does not break it.
 * </p>
 */
public class BlobLayoutJsonParser implements LayoutResponseParser {

  private static final String MARKER = "marker";

  private static final String MAX_RESULTS = "maxResults";

  private static final String RANGES = "ranges";

  private static final String RANGE_START = "start";

  private static final String RANGE_END = "end";

  private static final String RANGE_ENDPOINT_INDEX = "endpointIndex";

  private static final String ENDPOINTS = "endpoints";

  private static final String ENDPOINT_INDEX = "index";

  private static final String ENDPOINT_VALUE = "value";

  private static final String DATA_HANDLE = "dataHandle";

  private static final String DATA_HANDLE_EXPIRY = "dataHandleExpiry";

  private static final String NEXT_MARKER = "nextMarker";

  /** Shared, thread-safe factory for streaming parsers. */
  private static final JsonFactory JSON_FACTORY = new JsonFactory();

  /**
   * {@inheritDoc}
   */
  @Override
  public BlobLayoutResponse parse(final InputStream stream)
      throws IOException {
    if (stream == null) {
      return null;
    }

    final BlobLayoutResponse response = new BlobLayoutResponse();
    String dataHandle = null;
    long dataHandleExpiry = 0L;

    try (JsonParser parser = JSON_FACTORY.createParser(stream)) {
      JsonToken token = parser.nextToken();
      if (token == null) {
        // Empty body: the service reports "no layout available" this way.
        return null;
      }
      expect(parser, token, JsonToken.START_OBJECT);

      while (parser.nextToken() != JsonToken.END_OBJECT) {
        final String field = parser.currentName();
        parser.nextToken();

        switch (field) {
        case RANGES -> readRanges(parser, response);
        case ENDPOINTS -> readEndpoints(parser, response);
        case NEXT_MARKER -> response.setNextMarker(parser.getValueAsString());
        case MARKER -> {
          // Reserved by the shared serializer; retained for completeness.
        }
        case MAX_RESULTS ->
          // Numeric on the wire, String on the model.
            response.setMaxResults(parser.getValueAsString());
        case DATA_HANDLE -> dataHandle = parser.getValueAsString();
        case DATA_HANDLE_EXPIRY ->
            dataHandleExpiry = LayoutResponseParser.parseExpiry(
                parser.getValueAsString());
        default -> parser.skipChildren(); // Forward compatibility.
        }
      }
    } catch (EOFException e) {
      throw new IOException("Truncated blob layout JSON response", e);
    }

    // The handle is response-level and may arrive after the ranges, so it is
    // applied once the whole body has been consumed.
    LayoutResponseParser.applyDataHandle(response, dataHandle,
        dataHandleExpiry);

    return response;
  }

  /**
   * Reads the {@code ranges} array into the response.
   *
   * @param parser positioned on the array's START_ARRAY token
   * @param response response being populated
   * @throws IOException if the array is malformed
   */
  private void readRanges(final JsonParser parser,
      final BlobLayoutResponse response) throws IOException {
    expect(parser, parser.currentToken(), JsonToken.START_ARRAY);

    while (parser.nextToken() != JsonToken.END_ARRAY) {
      expect(parser, parser.currentToken(), JsonToken.START_OBJECT);

      long start = 0L;
      long end = -1L;
      int endpointIndex = -1;

      while (parser.nextToken() != JsonToken.END_OBJECT) {
        final String field = parser.currentName();
        parser.nextToken();

        switch (field) {
        case RANGE_START -> start = parser.getLongValue();
        case RANGE_END -> end = parser.getLongValue();
        case RANGE_ENDPOINT_INDEX -> endpointIndex = parser.getIntValue();
        default -> parser.skipChildren();
        }
      }

      // The data handle is attached later; see applyDataHandle.
      response.addRange(new BlobLayoutResponse.Range(start, end, endpointIndex,
          null, 0L));
    }
  }

  /**
   * Reads the {@code endpoints} array into the response.
   *
   * @param parser positioned on the array's START_ARRAY token
   * @param response response being populated
   * @throws IOException if the array is malformed
   */
  private void readEndpoints(final JsonParser parser,
      final BlobLayoutResponse response) throws IOException {
    expect(parser, parser.currentToken(), JsonToken.START_ARRAY);

    while (parser.nextToken() != JsonToken.END_ARRAY) {
      expect(parser, parser.currentToken(), JsonToken.START_OBJECT);

      int index = -1;
      String value = null;

      while (parser.nextToken() != JsonToken.END_OBJECT) {
        final String field = parser.currentName();
        parser.nextToken();

        switch (field) {
        case ENDPOINT_INDEX -> index = parser.getIntValue();
        case ENDPOINT_VALUE -> value = parser.getValueAsString();
        default -> parser.skipChildren();
        }
      }

      response.addEndpoint(new BlobLayoutResponse.Endpoint(index, value));
    }
  }

  /**
   * Asserts that the parser is positioned on the expected token.
   *
   * @param parser the parser, used for location reporting
   * @param actual the token found
   * @param expected the token required
   * @throws IOException if the tokens do not match
   */
  private void expect(final JsonParser parser,
      final JsonToken actual,
      final JsonToken expected) throws IOException {
    if (actual != expected) {
      throw new IOException(String.format(
          "Malformed blob layout JSON: expected %s but found %s at %s",
          expected, actual, parser.currentLocation()));
    }
  }
}
