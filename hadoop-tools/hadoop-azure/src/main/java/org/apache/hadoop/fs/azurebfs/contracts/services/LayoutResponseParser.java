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

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses a layout response body into a {@link BlobLayoutResponse}.
 * <p>
 * The Blob endpoint returns the layout as XML and the DFS endpoint returns it
 * as JSON. Both encodings describe the same logical response, so callers work
 * against this interface and stay independent of the wire format.
 * </p>
 * <p>
 * <b>Data handle scope:</b> the service issues at most one Direct Read data
 * handle per layout <em>response</em>, not one per range. The handle authorises
 * the union of the ranges returned in that response. {@link #applyDataHandle}
 * fans that single handle across every range so that
 * {@link BlobLayoutResponse.Range#dataHandle()} is populated uniformly,
 * regardless of which parser produced it.
 * </p>
 */
public interface LayoutResponseParser {

  /**
   * Parses a layout response body.
   *
   * @param stream response body; the parser reads it but does not close it
   * @return the parsed response, or {@code null} when the body is empty,
   *         which the service uses to signal that no layout is available
   *         (for example a {@code 204 No Content} reply). Returning
   *         {@code null} lets callers tell "no layout" apart from a genuine
   *         parse failure, which is signalled with {@link IOException}.
   * @throws IOException if the body is present but cannot be parsed
   */
  BlobLayoutResponse parse(InputStream stream) throws IOException;

  /**
   * Replaces every range in {@code response} with an equivalent range carrying
   * the supplied response-level data handle and expiry.
   * <p>
   * Called by implementations once the whole body has been read, since the
   * handle may be emitted after the ranges on the wire. When {@code handle} is
   * null or empty the response is left untouched.
   * </p>
   *
   * @param response the response whose ranges should be rewritten
   * @param handle the opaque Direct Read handle, may be null
   * @param expiresAt handle expiry in epoch milliseconds, 0 if unknown
   */
  static void applyDataHandle(final BlobLayoutResponse response,
      final String handle,
      final long expiresAt) {
    if (response == null || handle == null || handle.isEmpty()) {
      return;
    }

    List<BlobLayoutResponse.Range> rewritten
        = new ArrayList<>(response.getRanges().size());
    for (BlobLayoutResponse.Range range : response.getRanges()) {
      rewritten.add(new BlobLayoutResponse.Range(
          range.start(),
          range.end(),
          range.endpointIndex(),
          handle,
          expiresAt));
    }
    response.setRanges(rewritten);
  }

  /**
   * Converts a handle expiry timestamp to epoch milliseconds.
   * <p>
   * The DFS endpoint emits the expiry as an ISO-8601 instant in the form
   * {@code yyyy-MM-dd'T'HH:mm:ss'Z'}. A numeric value is also accepted so that
   * either endpoint can reuse this helper. A value that cannot be interpreted
   * yields 0, which callers treat as "expiry unknown" rather than "expired".
   * </p>
   *
   * @param value the raw expiry value, may be null
   * @return epoch milliseconds, or 0 when absent or unparseable
   */
  static long parseExpiry(final String value) {
    if (value == null || value.isEmpty()) {
      return 0L;
    }

    String trimmed = value.trim();
    try {
      return Instant.parse(trimmed).toEpochMilli();
    } catch (DateTimeParseException ignored) {
      // Fall through and try a plain epoch-millis value.
    }

    try {
      return Long.parseLong(trimmed);
    } catch (NumberFormatException ignored) {
      return 0L;
    }
  }
}
