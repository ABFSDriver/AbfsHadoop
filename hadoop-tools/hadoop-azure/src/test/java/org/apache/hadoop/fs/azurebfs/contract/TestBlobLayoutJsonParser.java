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

package org.apache.hadoop.fs.azurebfs.contract;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutJsonParser;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestBlobLayoutJsonParser {

  @Test
  public void testJsonParser() throws Exception {
    String jsonResponseWithContinuation = ""
        + "{\"marker\":\"marker-1\",\"maxResults\":100,"
        + "\"ranges\":["
        + "{\"start\":0,\"end\":511,\"endpointIndex\":0},"
        + "{\"start\":512,\"end\":1023,\"endpointIndex\":1}"
        + "],"
        + "\"endpoints\":["
        + "{\"index\":0,\"value\":\"http://127.0.0.1:10009/\"},"
        + "{\"index\":1,\"value\":\"http://127.0.0.1:11009/\"}"
        + "],"
        + "\"nextMarker\":\"marker-2\"}";

    BlobLayoutResponse layoutResponse = getLayoutResponse(jsonResponseWithContinuation);
    List<BlobLayoutResponse.Range> ranges = layoutResponse.getRanges();

    assertThat(ranges.size()).isEqualTo(2);
    assertThat(ranges.get(0).start()).isEqualTo(0L);
    assertThat(ranges.get(0).end()).isEqualTo(511L);
    assertThat(ranges.get(0).endpointIndex()).isEqualTo(0);
    assertThat(ranges.get(1).endpointIndex()).isEqualTo(1);
    assertThat(layoutResponse.getEndpoints().size()).isEqualTo(2);
    assertThat(layoutResponse.getMaxResults()).isEqualTo("100");
    assertThat(layoutResponse.getNextMarker()).isEqualTo("marker-2");
  }

  @Test
  public void testEmptyLayoutReturnsNull() throws Exception {
    BlobLayoutResponse layoutResponse = getLayoutResponse("");
    assertThat(layoutResponse).isNull();
  }

  @Test
  public void testLayoutWithNoDataHandle() throws Exception {
    String jsonResponse = ""
        + "{\"ranges\":[{\"start\":0,\"end\":1023,\"endpointIndex\":0}],"
        + "\"endpoints\":[{\"index\":0,\"value\":\"http://127.0.0.1:10009/\"}],"
        + "\"nextMarker\":\"\"}";

    BlobLayoutResponse layoutResponse = getLayoutResponse(jsonResponse);
    List<BlobLayoutResponse.Range> ranges = layoutResponse.getRanges();

    assertThat(ranges.size()).isEqualTo(1);
    assertThat(ranges.get(0).hasDataHandle()).isEqualTo(false);
    assertThat(layoutResponse.getNextMarker()).isEmpty();
  }

  @Test
  public void testLayoutWithDataHandleAppliedToEveryRange() throws Exception {
    String jsonResponse = ""
        + "{\"ranges\":["
        + "{\"start\":0,\"end\":511,\"endpointIndex\":0},"
        + "{\"start\":512,\"end\":1023,\"endpointIndex\":1}"
        + "],"
        + "\"endpoints\":["
        + "{\"index\":0,\"value\":\"http://127.0.0.1:10009/\"},"
        + "{\"index\":1,\"value\":\"http://127.0.0.1:11009/\"}"
        + "],"
        + "\"dataHandle\":\"opaque-handle-value\","
        + "\"dataHandleExpiry\":\"2026-08-06T07:34:49Z\","
        + "\"nextMarker\":\"\"}";

    BlobLayoutResponse layoutResponse = getLayoutResponse(jsonResponse);
    List<BlobLayoutResponse.Range> ranges = layoutResponse.getRanges();

    assertThat(ranges.get(0).dataHandle()).isEqualTo("opaque-handle-value");
    assertThat(ranges.get(1).dataHandle()).isEqualTo("opaque-handle-value");
    assertThat(ranges.get(0).expiresAt())
        .isEqualTo(Instant.parse("2026-08-06T07:34:49Z").toEpochMilli());
  }

  @Test
  public void testLayoutWithUnpairedDataHandleHasZeroExpiry() throws Exception {
    // BlobLayoutJsonParser does not enforce that dataHandle and
    // dataHandleExpiry arrive together - that pairing rule only exists in
    // the service's own emission-side validation. The client parser accepts
    // an unpaired handle and simply leaves the expiry at its 0L default.
    String jsonResponse = ""
        + "{\"ranges\":[{\"start\":0,\"end\":511,\"endpointIndex\":0}],"
        + "\"endpoints\":[{\"index\":0,\"value\":\"http://127.0.0.1:10009/\"}],"
        + "\"dataHandle\":\"opaque-handle-value\","
        + "\"nextMarker\":\"\"}";

    BlobLayoutResponse.Range range =
        getLayoutResponse(jsonResponse).getRanges().get(0);
    assertThat(range.hasDataHandle()).isEqualTo(true);
    assertThat(range.dataHandle()).isEqualTo("opaque-handle-value");
    assertThat(range.expiresAt()).isEqualTo(0L);
  }

  @Test
  public void testLayoutWithMalformedExpiryFallsBackToZero() throws Exception {
    String jsonResponse = ""
        + "{\"ranges\":[{\"start\":0,\"end\":511,\"endpointIndex\":0}],"
        + "\"endpoints\":[{\"index\":0,\"value\":\"http://127.0.0.1:10009/\"}],"
        + "\"dataHandle\":\"opaque-handle-value\","
        + "\"dataHandleExpiry\":\"not-a-timestamp\","
        + "\"nextMarker\":\"\"}";

    BlobLayoutResponse layoutResponse = getLayoutResponse(jsonResponse);
    BlobLayoutResponse.Range range = layoutResponse.getRanges().get(0);

    assertThat(range.dataHandle()).isEqualTo("opaque-handle-value");
    assertThat(range.expiresAt()).isEqualTo(0L);
  }

  @Test
  public void testLayoutIgnoresPropertyOrder() throws Exception {
    String jsonResponse = ""
        + "{\"nextMarker\":\"\","
        + "\"endpoints\":[{\"value\":\"http://127.0.0.1:10009/\",\"index\":0}],"
        + "\"ranges\":[{\"endpointIndex\":0,\"end\":1023,\"start\":0}]}";

    BlobLayoutResponse layoutResponse = getLayoutResponse(jsonResponse);
    assertThat(layoutResponse.getRanges().size()).isEqualTo(1);
    assertThat(layoutResponse.getRanges().get(0).end()).isEqualTo(1023L);
  }

  @Test
  public void testLayoutSkipsUnknownFields() throws Exception {
    String jsonResponse = ""
        + "{\"someFutureField\":{\"a\":[1,2,3]},"
        + "\"ranges\":[{\"start\":0,\"end\":1023,\"endpointIndex\":0}],"
        + "\"endpoints\":[{\"index\":0,\"value\":\"http://127.0.0.1:10009/\"}],"
        + "\"nextMarker\":\"\"}";

    BlobLayoutResponse layoutResponse = getLayoutResponse(jsonResponse);
    assertThat(layoutResponse.getRanges().size()).isEqualTo(1);
  }

  @Test
  public void testLayoutMissingNumericFieldsFallBackToDefaults() throws Exception {
    String jsonResponse = ""
        + "{\"ranges\":[{\"start\":0}],"
        + "\"endpoints\":[{\"index\":0,\"value\":\"http://127.0.0.1:10009/\"}],"
        + "\"nextMarker\":\"\"}";

    BlobLayoutResponse.Range range = getLayoutResponse(jsonResponse).getRanges().get(0);
    assertThat(range.end()).isEqualTo(-1L);
    assertThat(range.endpointIndex()).isEqualTo(-1);
  }

  @Test
  public void testTruncatedBodyThrows() {
    String truncated = "{\"ranges\":[{\"start\":0,\"end\"";
    assertThrows(Exception.class, () -> getLayoutResponse(truncated));
  }

  @Test
  public void testNonObjectBodyThrows() {
    assertThrows(Exception.class, () -> getLayoutResponse("[1,2,3]"));
  }

  private BlobLayoutResponse getLayoutResponse(String jsonResponse) throws Exception {
    byte[] bytes = jsonResponse.getBytes(StandardCharsets.UTF_8);
    final InputStream stream = new ByteArrayInputStream(bytes);
    return new BlobLayoutJsonParser().parse(stream);
  }
}
