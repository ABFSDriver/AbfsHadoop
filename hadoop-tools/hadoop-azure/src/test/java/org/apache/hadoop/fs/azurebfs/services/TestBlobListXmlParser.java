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

package org.apache.hadoop.fs.azurebfs.services;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.assertj.core.api.Assertions;
import java.util.List;

import org.junit.Test;
import org.xml.sax.SAXException;

public class TestBlobListXmlParser {
  @Test
  public void testXMLParser() throws Exception {
    String xmlResponse = ""
        + "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
        + "<EnumerationResults ServiceEndpoint=\"https://accountName.blob.core.windows.net/\" ContainerName=\"testContainer\">"
          + "<Delimiter>/</Delimiter>"
          + "<Blobs>"
            + "<Blob>"
              + "<Name>Splitting Example.txt</Name>"
              + "<Properties>"
                + "<Creation-Time>Tue, 06 Jun 2023 08:33:51 GMT</Creation-Time>"
                + "<Last-Modified>Tue, 06 Jun 2023 08:33:51 GMT</Last-Modified>"
                + "<Etag>0x8DB6668C17D3B76</Etag>"
                + "<Content-Length>3844</Content-Length>"
                + "<ResourceType>file</ResourceType>"
                + "<Owner>BlockBlob</Owner>"
                + "<Group>Hot</Group>"
                + "<Permissions>true</Permissions>"
                + "<Acl>unlocked</Acl>"
                + "<CopyId>available</CopyId>"
                + "<CopyStatus>true</CopyStatus>"
                + "<CopySource>true</CopySource>"
                + "<CopyProgress>true</CopyProgress>"
                + "<CopyCompletionTime>Tue, 06 Jun 2023 08:33:51 GMT</CopyCompletionTime>"
                + "<CopyStatusDescription>true</CopyStatusDescription>"
                + "<UnknownXmL>true</UnknownXmL>"
              + "</Properties>"
              + "<Metadata />"
              + "<OrMetadata />"
            + "</Blob>"
            + "<BlobPrefix>"
              + "<Name>bye/</Name>"
            + "</BlobPrefix>"
          + "</Blobs>"
          + "<NextMarker />"
        + "</EnumerationResults>";
    byte[] bytes = xmlResponse.getBytes();
    final InputStream stream = new ByteArrayInputStream(bytes);;
    final SAXParser saxParser = saxParserThreadLocal.get();
    saxParser.reset();
    BlobList blobList = new BlobList();
    saxParser.parse(stream, new BlobListXmlParser(blobList, "https://sample.url"));
    List<BlobProperty> prop = blobList.getBlobPropertyList();
    Assertions.assertThat(prop.size()).isEqualTo(2);
    Assertions.assertThat(prop.get(0).getIsDirectory()).isEqualTo(false);
    Assertions.assertThat(prop.get(1).getIsDirectory()).isEqualTo(true);
  }

  @Test
  public void testEmptyBlobList() throws Exception {
    String xmlResponse = ""
        + "<?xml version=\"1.0\" encoding=\"utf-8\"?><"
        + "EnumerationResults ServiceEndpoint=\"https://anujtestfns.blob.core.windows.net/\" ContainerName=\"manualtest\">"
          + "<Prefix>abc/</Prefix>"
          + "<Delimiter>/</Delimiter>"
          + "<Blobs /><NextMarker />"
        + "</EnumerationResults>";
    byte[] bytes = xmlResponse.getBytes();
    final InputStream stream = new ByteArrayInputStream(bytes);;
    final SAXParser saxParser = saxParserThreadLocal.get();
    saxParser.reset();
    BlobList blobList = new BlobList();
    saxParser.parse(stream, new BlobListXmlParser(blobList, "https://sample.url"));
    List<BlobProperty> prop = blobList.getBlobPropertyList();
  }

  private static final ThreadLocal<SAXParser> saxParserThreadLocal
      = new ThreadLocal<SAXParser>() {
    @Override
    public SAXParser initialValue() {
      SAXParserFactory factory = SAXParserFactory.newInstance();
      factory.setNamespaceAware(true);
      try {
        return factory.newSAXParser();
      } catch (SAXException e) {
        throw new RuntimeException("Unable to create SAXParser", e);
      } catch (ParserConfigurationException e) {
        throw new RuntimeException("Check parser configuration", e);
      }
    }
  };
}
