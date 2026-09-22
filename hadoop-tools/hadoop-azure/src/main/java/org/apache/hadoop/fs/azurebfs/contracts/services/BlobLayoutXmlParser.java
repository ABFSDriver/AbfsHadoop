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

import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.InputStream;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * SAX handler and {@link LayoutResponseParser} for the Blob endpoint's XML
 * layout response.
 *
 * <p>Recognises {@code Range}, {@code Endpoint}, {@code DataHandle},
 * {@code NextMarker} and {@code MaxResults}; unknown elements are ignored.
 * Malformed numeric values fall back to defaults rather than failing the
 * parse.</p>
 *
 * <p>The Direct Read data handle is issued once per response, not per range,
 * so it is captured from the top-level {@code DataHandle} element and applied
 * to every range in {@link #endDocument()}.</p>
 *
 * <p>Use {@link #parse(InputStream)} in production code, which owns the parse
 * loop and keeps callers independent of the response format. The class can
 * also be passed to a caller-supplied {@link SAXParser}, in which case parse
 * state is held in fields and instances are not reusable.</p>
 *
 * @see BlobLayoutJsonParser
 */
public class BlobLayoutXmlParser extends DefaultHandler
    implements LayoutResponseParser {

  private static final String RANGE = "Range";

  private static final String RANGE_START = "Start";

  private static final String RANGE_END = "End";

  private static final String RANGE_ENDPOINT_INDEX = "EndpointIndex";

  private static final String ENDPOINT = "Endpoint";

  private static final String ENDPOINT_INDEX = "Index";

  private static final String ENDPOINT_VALUE = "Value";

  private static final String NEXT_MARKER = "NextMarker";

  private static final String MAX_RESULTS = "MaxResults";

  /**
   * Element carrying the Direct Read data handle. The service emits the handle
   * as the element's own text with the expiry as a child element, both at the
   * top level of the response rather than as attributes of a range.
   */
  private static final String DATA_HANDLE = "DataHandle";

  private static final String DATA_HANDLE_EXPIRY = "DataHandleExpiry";

  /**
   * The BlobLayoutResponse object that will be populated by this parser.
   */
  private final BlobLayoutResponse response = new BlobLayoutResponse();

  /**
   * Buffer for accumulating character data between XML tags.
   */
  private final StringBuilder textBuffer = new StringBuilder();

  /**
   * Response-level Direct Read data handle, null when the service did not
   * issue one.
   */
  private String dataHandle;

  /**
   * Expiry of {@link #dataHandle} in epoch milliseconds, 0 when unknown.
   */
  private long dataHandleExpiry;

  /**
   * True while inside the &lt;DataHandle&gt; element, so that its text can be
   * captured separately from the text of its child elements.
   */
  private boolean inDataHandle;

  /**
   * Returns the parsed BlobLayoutResponse after XML parsing is complete.
   *
   * @return the populated BlobLayoutResponse
   */
  public BlobLayoutResponse getResponse() {
    return response;
  }

  /**
   * {@inheritDoc}
   * <p>
   * A fresh handler is used for each call so that this instance stays
   * reusable and thread-safe when acting as a {@link LayoutResponseParser}.
   * </p>
   */
  @Override
  public BlobLayoutResponse parse(final InputStream stream)
      throws IOException {
    if (stream == null) {
      return null;
    }

    final BlobLayoutXmlParser handler = new BlobLayoutXmlParser();
    try {
      final SAXParserFactory factory = SAXParserFactory.newInstance();
      factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
      factory.setNamespaceAware(false);
      final SAXParser saxParser = factory.newSAXParser();
      saxParser.parse(stream, handler);
    } catch (ParserConfigurationException | SAXException e) {
      throw new IOException("Failed to parse blob layout XML response", e);
    }

    final BlobLayoutResponse parsed = handler.getResponse();
    if (parsed.getRanges().isEmpty() && parsed.getEndpoints().isEmpty()) {
      // Empty body: the service reports "no layout available" this way.
      return null;
    }
    return parsed;
  }

  /**
   * Handles the start of an XML element. Resets the text buffer and processes
   * &lt;Range&gt; and &lt;Endpoint&gt; elements by extracting their attributes
   * and adding them to the response. Missing or malformed numeric attributes
   * fall back to default values rather than failing the parse.
   *
   * @param uri the Namespace URI
   * @param localName the local name (without prefix)
   * @param qName the qualified name (with prefix)
   * @param attributes the attributes attached to the element
   */
  @Override
  public void startElement(String uri,
      String localName,
      String qName,
      Attributes attributes) {

    if (DATA_HANDLE_EXPIRY.equals(qName) && inDataHandle) {
      // Text accumulated so far belongs to the enclosing DataHandle element.
      captureDataHandleText();
    }

    textBuffer.setLength(0); // reset text buffer

    switch (qName) {
    case RANGE -> {
      // The data handle is response-level and is attached once the document
      // ends; see endDocument.
      BlobLayoutResponse.Range r = new BlobLayoutResponse.Range(
          parseLong(attributes.getValue(RANGE_START), 0L),
          parseLong(attributes.getValue(RANGE_END), -1L),
          parseInt(attributes.getValue(RANGE_ENDPOINT_INDEX), -1),
          null,
          0L
      );
      response.addRange(r);
    }
    case ENDPOINT -> {
      BlobLayoutResponse.Endpoint e = new BlobLayoutResponse.Endpoint(
          parseInt(attributes.getValue(ENDPOINT_INDEX), -1),
          attributes.getValue(ENDPOINT_VALUE));
      response.addEndpoint(e);
    }
    case DATA_HANDLE -> {
      inDataHandle = true;
      dataHandle = null;
    }
    default -> {
      // No attribute handling required for other elements.
    }
    }
  }

  /**
   * Handles character data between XML tags. Appends the characters to the
   * text buffer.
   *
   * @param ch the characters
   * @param start the start position in the character array
   * @param length the number of characters to use from the character array
   */
  @Override
  public void characters(char[] ch, int start, int length) {
    textBuffer.append(ch, start, length);
  }

  /**
   * Handles the end of an XML element. Sets the marker, page size and Direct
   * Read handle values from the accumulated text buffer.
   *
   * @param uri the Namespace URI
   * @param localName the local name (without prefix)
   * @param qName the qualified name (with prefix)
   */
  @Override
  public void endElement(String uri, String localName, String qName) {

    String text = textBuffer.toString().trim();

    switch (qName) {
    case NEXT_MARKER -> response.setNextMarker(text); // will be "" if empty
    case MAX_RESULTS -> response.setMaxResults(text);
    case DATA_HANDLE_EXPIRY ->
        dataHandleExpiry = LayoutResponseParser.parseExpiry(text);
    case DATA_HANDLE -> {
      captureDataHandleText();
      inDataHandle = false;
    }
    default -> {
      // No text handling required for other elements.
    }
    }
  }

  /**
   * Attaches the response-level data handle to every parsed range once the
   * whole document has been read.
   */
  @Override
  public void endDocument() {
    LayoutResponseParser.applyDataHandle(response, dataHandle,
        dataHandleExpiry);
  }

  /**
   * Moves the buffered text into {@link #dataHandle} if it has not already
   * been captured. Called both when a child element interrupts the handle text
   * and when the handle element closes.
   */
  private void captureDataHandleText() {
    if (dataHandle == null) {
      String text = textBuffer.toString().trim();
      if (!text.isEmpty()) {
        dataHandle = text;
      }
    }
  }

  /**
   * Parses a long attribute value, returning a default if absent or malformed.
   *
   * @param value the raw attribute value, may be null
   * @param defaultValue value to return when parsing is not possible
   * @return the parsed value or {@code defaultValue}
   */
  private static long parseLong(String value, long defaultValue) {
    if (value == null || value.isEmpty()) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /**
   * Parses an int attribute value, returning a default if absent or malformed.
   *
   * @param value the raw attribute value, may be null
   * @param defaultValue value to return when parsing is not possible
   * @return the parsed value or {@code defaultValue}
   */
  private static int parseInt(String value, int defaultValue) {
    if (value == null || value.isEmpty()) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }
}
