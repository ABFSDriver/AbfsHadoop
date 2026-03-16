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

import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

public class BlobLayoutXmlParser extends DefaultHandler {

  private final BlobLayoutResponse response = new BlobLayoutResponse();

  private final StringBuilder textBuffer = new StringBuilder();

  public BlobLayoutResponse getResponse() {
    return response;
  }

  @Override
  public void startElement(String uri,
      String localName,
      String qName,
      Attributes attributes) {

    textBuffer.setLength(0); // reset text buffer

    switch (qName) {
    case "Range" -> {
      BlobLayoutResponse.Range r = new BlobLayoutResponse.Range(
          Long.parseLong(attributes.getValue("Start")),
          Long.parseLong(attributes.getValue("End")),
          Integer.parseInt(attributes.getValue("EndpointIndex"))
      );
      response.addRange(r);
    }
    case "Endpoint" -> {
      BlobLayoutResponse.Endpoint e = new BlobLayoutResponse.Endpoint(
          Integer.parseInt(attributes.getValue("Index")),
          attributes.getValue("Value"));
      response.addEndpoint(e);
    }

    // TODO: Add Support for Read Keys Based on Data View.
    }
  }

  @Override
  public void characters(char[] ch, int start, int length) {
    textBuffer.append(ch, start, length);
  }

  @Override
  public void endElement(String uri, String localName, String qName) {

    String text = textBuffer.toString().trim();

    switch (qName) {
    case "NextMarker" -> response.setNextMarker(text);   // will be "" if empty
    case "MaxResults" -> response.setMaxResults(text);
    }
  }
}
