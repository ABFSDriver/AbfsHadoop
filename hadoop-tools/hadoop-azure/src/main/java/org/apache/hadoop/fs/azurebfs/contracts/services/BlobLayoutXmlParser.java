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

  private BlobLayoutResponse response = new BlobLayoutResponse();
  private StringBuilder textBuffer = new StringBuilder();

  public BlobLayoutResponse getResponse() {
    return response;
  }

  @Override
  public void startElement(String uri, String localName, String qName, Attributes attributes) {

    textBuffer.setLength(0); // reset text buffer

    switch (qName) {
      case "Range" -> {
        BlobLayoutResponse.Range r = new BlobLayoutResponse.Range();
        r.start = Long.parseLong(attributes.getValue("Start"));
        r.end = Long.parseLong(attributes.getValue("End"));
        r.endpointIndex = Integer.parseInt(attributes.getValue("EndpointIndex"));
        response.ranges.add(r);
      }
      case "Endpoint" -> {
        BlobLayoutResponse.Endpoint e = new BlobLayoutResponse.Endpoint();
        e.index = Integer.parseInt(attributes.getValue("Index"));
        e.value = attributes.getValue("Value");
        response.endpoints.add(e);
      }
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
    case "NextMarker" -> response.nextMarker = text;   // will be "" if empty
    case "MaxResults" -> response.maxResults = text;
    }
  }
}
