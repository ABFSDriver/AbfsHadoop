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

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;

import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutSchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListResultSchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListXmlParser;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBlobLayoutParser {
  @Test
  public void testXMLParser() throws Exception {
    String xml =
        "<BlobLayout>"
        + "<DataView Id=\"0\" Expiry=\"1234545\">"
        + " <ReadKeys>"
        + "   <ReadKey Id=\"0\">key0</ReadKey>"
        + "   <ReadKey Id=\"1\">key1</ReadKey>"
        + "   <ReadKey Id=\"2\">key2</ReadKey>"
        + " </ReadKeys>"
        + "</DataView>"
        + "<Ranges>"
        + " <Range Start=\"0\" End=\"999999\" Endpoint=\"0\" ReadKeys=\"0,1\" />"
        + " <Range Start=\"1000000\" End=\"1999999\" Endpoint=\"1\" ReadKeys=\"1,2\" />"
        + "</Ranges>"
        + "<Endpoints>"
        + " <Endpoint Id=\"0\" Value=\"blob.stampA.store.core.windows.net:443\" />"
        + " <Endpoint Id=\"1\" Value=\"blob.stampB.store.core.windows.net:443\" />"
        + "</Endpoints>"
        + "<NextMarker />"
        + "</BlobLayout>";
    JAXBContext context = JAXBContext.newInstance(BlobLayoutSchema.class);
    Unmarshaller unmarshaller = context.createUnmarshaller();
    BlobLayoutSchema schema = (BlobLayoutSchema) unmarshaller.unmarshal(new StringReader(xml));
    assertThat(schema).isNotNull();
  }

  @Test
  public void testEmptyBlobListNullCT() throws Exception {
    String xmlResponse = ""
        + "<?xml version=\"1.0\" encoding=\"utf-8\"?><"
        + "EnumerationResults ServiceEndpoint=\"https://anujtestfns.blob.core.windows.net/\" ContainerName=\"manualtest\">"
        + "<Prefix>abc/</Prefix>"
        + "<Delimiter>/</Delimiter>"
        + "<Blobs /><NextMarker />"
        + "</EnumerationResults>";
    BlobListResultSchema listResultSchema = getResultSchema(xmlResponse);
    List<BlobListResultEntrySchema> paths = listResultSchema.paths();
    assertThat(paths.size()).isEqualTo(0);
    assertThat(listResultSchema.getNextMarker()).isNull();
  }

  @Test
  public void testEmptyBlobListValidCT() throws Exception {
    String xmlResponse = ""
        + "<?xml version=\"1.0\" encoding=\"utf-8\"?><"
        + "EnumerationResults ServiceEndpoint=\"https://anujtestfns.blob.core.windows.net/\" ContainerName=\"manualtest\">"
        + "<Prefix>abc/</Prefix>"
        + "<Delimiter>/</Delimiter>"
        + "<Blobs />"
        + "<NextMarker>TEST_CONTINUATION_TOKEN</NextMarker>"
        + "</EnumerationResults>";
    BlobListResultSchema listResultSchema = getResultSchema(xmlResponse);
    List<BlobListResultEntrySchema> paths = listResultSchema.paths();
    assertThat(paths.size()).isEqualTo(0);
    assertThat(listResultSchema.getNextMarker()).isNotNull();
  }

  @Test
  public void testNonEmptyBlobListNullCT() throws Exception {
    String xmlResponse = ""
        + "<?xml version=\"1.0\" encoding=\"utf-8\"?><"
        + "EnumerationResults ServiceEndpoint=\"https://anujtestfns.blob.core.windows.net/\" ContainerName=\"manualtest\">"
        + "<Prefix>abc/</Prefix>"
        + "<Delimiter>/</Delimiter>"
        + "<Blobs>"
        + "<BlobPrefix>"
        + "<Name>bye/</Name>"
        + "</BlobPrefix>"
        + "</Blobs>"
        + "<NextMarker />"
        + "</EnumerationResults>";
    BlobListResultSchema listResultSchema = getResultSchema(xmlResponse);
    List<BlobListResultEntrySchema> paths = listResultSchema.paths();
    assertThat(paths.size()).isEqualTo(1);
    assertThat(listResultSchema.getNextMarker()).isNull();
  }

  private static final ThreadLocal<SAXParser> SAX_PARSER_THREAD_LOCAL
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

  private BlobListResultSchema getResultSchema(String xmlResponse) throws Exception {
    byte[] bytes = xmlResponse.getBytes();
    final InputStream stream = new ByteArrayInputStream(bytes);
    final SAXParser saxParser = SAX_PARSER_THREAD_LOCAL.get();
    saxParser.reset();
    BlobListResultSchema listResultSchema = new BlobListResultSchema();
    saxParser.parse(stream, new BlobListXmlParser(listResultSchema, "https://sample.url"));
    return listResultSchema;
  }
}
