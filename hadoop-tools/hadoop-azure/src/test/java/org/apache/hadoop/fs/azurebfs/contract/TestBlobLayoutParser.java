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

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.ByteArrayInputStream;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutResponse;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutXmlParser;

public class TestBlobLayoutParser {
  @Test
  public void testXMLParser() throws Exception {
    String xml =
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
        + "<SAMPLE TO BE ADDED FOR TESTS";
    SAXParserFactory factory = SAXParserFactory.newInstance();
    SAXParser parser = factory.newSAXParser();

    BlobLayoutXmlParser handler = new BlobLayoutXmlParser();
    parser.parse(new ByteArrayInputStream(xml.getBytes()), handler);
    BlobLayoutResponse resp = handler.getResponse();
  }
}
