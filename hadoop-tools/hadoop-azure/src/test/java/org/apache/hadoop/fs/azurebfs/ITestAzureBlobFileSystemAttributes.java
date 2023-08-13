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

package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.EnumSet;

import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.azurebfs.services.PrefixMode;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test attribute operations.
 */
public class ITestAzureBlobFileSystemAttributes extends AbstractAbfsIntegrationTest {
  private static final EnumSet<XAttrSetFlag> CREATE_FLAG = EnumSet.of(XAttrSetFlag.CREATE);
  private static final EnumSet<XAttrSetFlag> REPLACE_FLAG = EnumSet.of(XAttrSetFlag.REPLACE);

  public ITestAzureBlobFileSystemAttributes() throws Exception {
    super();
  }

  /**
   * Test GetXAttr() and SetXAttr() with Unicode Attribute Values.
   * DFS does not support Unicode characters in user-defined metadata properties.
   * Blob Endpoint supports Unicode encoded in UTF_8 character encoding.
   * @throws Exception
   */
  @Test
  public void testGetSetXAttr() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    final Path testPath = new Path("a/b");
    fs.create(testPath);
    testGetSetXAttrHelper(fs, testPath, testPath);
  }

  @Test
  public void testGetSetXAttrOnRoot() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    // TODO: Support SetXAttr() on root on DFS endpoint
    Assume.assumeTrue(fs.getAbfsStore().getPrefixMode() == PrefixMode.BLOB);
    final Path filePath = new Path("a/b");
    final Path testPath = new Path("/");
    fs.create(filePath);
    testGetSetXAttrHelper(fs, filePath, testPath);
  }

  @Test
  public void testGetSetXAttrOnImplicitDir() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    final Path testPath = new Path("a/b");
    AzcopyHelper azcopyHelper = new AzcopyHelper(
        getAccountName(),
        getFileSystemName(),
        getRawConfiguration(),
        fs.getAbfsStore().getPrefixMode()
    );

    azcopyHelper.createFolderUsingAzcopy(fs.makeQualified(testPath).toUri().getPath().substring(1));
    // Assert that the folder is implicit
    BlobDirectoryStateHelper.isExplicitDirectory(testPath, fs, getTestTracingContext(fs, true));
    testGetSetXAttrHelper(fs, testPath, testPath);

    // Assert that the folder is now explicit
    BlobDirectoryStateHelper.isExplicitDirectory(testPath, fs, getTestTracingContext(fs, true));
  }

  /**
   * Test that setting metadata over marker blob do not override
   * x-ms-meta-hdi_IsFolder
   * @throws Exception
   */
  @Test
  public void testSetXAttrOverMarkerBlob() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    final Path testPath = new Path("ab");
    fs.mkdirs(testPath);
    testGetSetXAttrHelper(fs, testPath, testPath);

    // Assert that the folder is now explicit
    BlobDirectoryStateHelper.isExplicitDirectory(testPath, fs, getTestTracingContext(fs, true));
  }

  private void testGetSetXAttrHelper(final AzureBlobFileSystem fs,
      final Path filePath, final Path testPath) throws Exception {

    String attributeName1 = "user.attribute1";
    String attributeName2 = "user.attribute2";
    String decodedAttributeValue1;
    String decodedAttributeValue2;
    byte[] attributeValue1;
    byte[] attributeValue2;

    if(fs.getAbfsStore().getPrefixMode() == PrefixMode.BLOB) {
      Assume.assumeTrue(!getIsNamespaceEnabled(fs)); // Blob endpoint Currently Supports FNS only
      decodedAttributeValue1 = "hi";
      decodedAttributeValue2 = "hello"; //Блюз //你好
      // TODO: Modify them to unicode characters when support is added
      attributeValue1 = fs.getAbfsStore().encodeAttribute(decodedAttributeValue1);
      attributeValue2 = fs.getAbfsStore().encodeAttribute(decodedAttributeValue2);
    }
    else {
      decodedAttributeValue1 = "hi";
      decodedAttributeValue2 = "hello"; // DFS Endpoint only Supports ASCII
      attributeValue1 = fs.getAbfsStore().encodeAttribute(decodedAttributeValue1);
      attributeValue2 = fs.getAbfsStore().encodeAttribute(decodedAttributeValue2);
    }

    // Attribute not present initially
    assertNull(fs.getXAttr(testPath, attributeName1));
    assertNull(fs.getXAttr(testPath, attributeName2));

    // Set the Attributes
    fs.setXAttr(testPath, attributeName1, attributeValue1);

    // Check if the attribute is retrievable
    byte[] rv = fs.getXAttr(testPath, attributeName1);
    assertTrue(Arrays.equals(rv, attributeValue1));
    assertEquals(new String(rv, StandardCharsets.UTF_8), decodedAttributeValue1);

    // Set the second Attribute
    fs.setXAttr(testPath, attributeName2, attributeValue2);

    // Check all the attributes present and previous Attribute not overridden
    rv = fs.getXAttr(testPath, attributeName1);
    assertTrue(Arrays.equals(rv, attributeValue1));
    assertEquals(new String(rv, StandardCharsets.UTF_8), decodedAttributeValue1);
    rv = fs.getXAttr(testPath, attributeName2);
    assertTrue(Arrays.equals(rv, attributeValue2));
    assertEquals(new String(rv, StandardCharsets.UTF_8), decodedAttributeValue2);
  }

  /**
   * Trying to set same attribute multiple times should result in no failure
   * @throws Exception
   */
  @Test
  public void testSetXAttrMultipleOperations() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    final Path path = new Path("a/b");
    fs.create(path);

    String attributeName1 = "user.attribute1";
    byte[] attributeValue1;
    String decodedAttributeValue1 = "hi";

    if(fs.getAbfsStore().getPrefixMode() == PrefixMode.BLOB) {
      Assume.assumeTrue(!getIsNamespaceEnabled(fs));
      // TODO: Modify them to unicode characters when support is added
      attributeValue1 = fs.getAbfsStore().encodeAttribute("hi");
    }
    else {
      attributeValue1 = fs.getAbfsStore().encodeAttribute("hi");
    }

    // Attribute not present initially
    assertNull(fs.getXAttr(path, attributeName1));

    // Set the Attributes Multiple times
    // Filesystem internally adds create and replace flags
    fs.setXAttr(path, attributeName1, attributeValue1);
    fs.setXAttr(path, attributeName1, attributeValue1);

    // Check if the attribute is retrievable
    byte[] rv = fs.getXAttr(path, attributeName1);
    assertTrue(Arrays.equals(rv, attributeValue1));
    assertEquals(new String(rv, StandardCharsets.UTF_8), decodedAttributeValue1);
  }

  @Test
  public void testSetGetXAttrCreateReplace() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    final Path testFile = new Path("a/b");

    String attributeName = "user.attribute1";
    String decodedAttributeValue1;
    byte[] attributeValue;

    if(fs.getAbfsStore().getPrefixMode() == PrefixMode.BLOB) {
      Assume.assumeTrue(!getIsNamespaceEnabled(fs)); // Blob endpoint Currently Supports FNS only
      decodedAttributeValue1 = "hi";
      attributeValue = decodedAttributeValue1.getBytes(StandardCharsets.UTF_8);
    }
    else {
      decodedAttributeValue1 = "hi";
      attributeValue = fs.getAbfsStore().encodeAttribute(decodedAttributeValue1);
    }

    // after creating a file, it must be possible to create a new xAttr
    fs.create(testFile);
    fs.setXAttr(testFile, attributeName, attributeValue, CREATE_FLAG);
    assertArrayEquals(attributeValue, fs.getXAttr(testFile, attributeName));

    // however, after the xAttr is created, creating it again must fail
    intercept(IOException.class, () -> fs.setXAttr(testFile, attributeName, attributeValue, CREATE_FLAG));
  }

  @Test
  public void testSetGetXAttrReplace() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    final Path testFile = new Path("a/b");

    String attributeName = "user.attribute1";
    String decodedAttributeValue1 = "one";
    String decodedAttributeValue2 = "two";

    byte[] attributeValue1;
    byte[] attributeValue2;

    if(fs.getAbfsStore().getPrefixMode() == PrefixMode.BLOB) {
      Assume.assumeTrue(!getIsNamespaceEnabled(fs)); // Blob endpoint Currently Supports FNS only
      attributeValue1 = decodedAttributeValue1.getBytes(StandardCharsets.UTF_8);
      attributeValue2 = decodedAttributeValue2.getBytes(StandardCharsets.UTF_8);
    }
    else {
      attributeValue1 = fs.getAbfsStore().encodeAttribute(decodedAttributeValue1);
      attributeValue2 = fs.getAbfsStore().encodeAttribute(decodedAttributeValue2);
    }

    // after creating a file, it must not be possible to replace an xAttr
    intercept(IOException.class, () -> {
      fs.create(testFile);
      fs.setXAttr(testFile, attributeName, attributeValue1, REPLACE_FLAG);
    });

    // however, after the xAttr is created, replacing it must succeed
    fs.setXAttr(testFile, attributeName, attributeValue1, CREATE_FLAG);
    fs.setXAttr(testFile, attributeName, attributeValue2, REPLACE_FLAG);
    assertArrayEquals(attributeValue2, fs.getXAttr(testFile, attributeName));
  }
}
