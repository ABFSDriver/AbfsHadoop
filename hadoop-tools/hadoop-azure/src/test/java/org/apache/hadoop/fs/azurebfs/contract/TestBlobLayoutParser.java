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
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;

import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutResponse;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutXmlParser;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListResultSchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListXmlParser;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_READAHEAD_V2;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBlobLayoutParser extends AbstractAbfsIntegrationTest {
  protected TestBlobLayoutParser() throws Exception {
  }

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

  @Test
  public void testGBL() throws IOException {
    Configuration conf = getRawConfiguration();
    conf.set(FS_AZURE_ENABLE_READAHEAD_V2, "true");
    AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(conf);
    Path testFile = new Path("/txtfile.txt");
    fs.create(testFile).close();
    byte[] writeData = new byte[64 * ONE_MB];
    for (int i = 0; i < writeData.length; i++) {
      writeData[i] = (byte) (i % 256);
    }
    FSDataOutputStream out = fs.append(testFile);
    out.write(writeData);
    out.close();
    try (FSDataInputStream iStream = fs.open(testFile)) {
      AbfsInputStream stream = (AbfsInputStream) iStream.getWrappedStream(); // System.out.print(stream.getBlobLayoutResult());
      long startNs = System.nanoTime();
      byte[] readData = new byte[64 * ONE_MB];
      int bytes = stream.read(readData);
      long endNs = System.nanoTime();
      long durationNs = endNs - startNs;
      long durationMs = TimeUnit.NANOSECONDS.toMillis(durationNs);
      System.out.println("Read bytes = " + bytes);
      System.out.println("Read time = " + durationMs + " ms)");

      int mismatch = Arrays.mismatch(readData, writeData);
      System.out.println("Mismatch index = " + mismatch);
      if(mismatch !=-1){
        stream.read(readData);
      }

      assertThat(readData).containsExactly(writeData);
      // System.out.print("BYTES"+bytes);
      }
  }
}
