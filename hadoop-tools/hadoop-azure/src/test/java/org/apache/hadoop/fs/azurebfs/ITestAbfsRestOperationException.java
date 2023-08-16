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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.fs.azurebfs.services.OperativeEndpoint;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.oauth2.RetryTestTokenProvider;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.PrefixMode;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import org.mockito.Mockito;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DOT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_ACCOUNT_NAME;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Verify the AbfsRestOperationException error message format.
 * */
public class ITestAbfsRestOperationException extends AbstractAbfsIntegrationTest{
  private static final String RETRY_TEST_TOKEN_PROVIDER = "org.apache.hadoop.fs.azurebfs.oauth2.RetryTestTokenProvider";

  boolean useBlobEndpoint;
  public ITestAbfsRestOperationException() throws Exception {
    super.setup();
    AzureBlobFileSystemStore abfsStore = getAbfsStore(getFileSystem());
    PrefixMode prefixMode = abfsStore.getPrefixMode();
    AbfsConfiguration abfsConfiguration = abfsStore.getAbfsConfiguration();
    useBlobEndpoint = !(OperativeEndpoint.isIngressEnabledOnDFS(prefixMode, abfsConfiguration) ||
            OperativeEndpoint.isMkdirEnabledOnDFS(abfsConfiguration) ||
            OperativeEndpoint.isReadEnabledOnDFS(abfsConfiguration));
  }

  @Test
  public void testAbfsRestOperationExceptionFormat() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    Path nonExistedFilePath1 = new Path("nonExistedPath1");
    Path nonExistedFilePath2 = new Path("nonExistedPath2");
    try {
      FileStatus fileStatus = fs.getFileStatus(nonExistedFilePath1);
    } catch (Exception ex) {
      String errorMessage = ex.getLocalizedMessage();
      String[] errorFields = errorMessage.split(",");

      // Expected Fields are: Message, StatusCode, Method, URL, ActivityId(rId)
      Assert.assertEquals(5, errorFields.length);
      // Check status message, status code, HTTP Request Type and URL.
      if (useBlobEndpoint) {
        Assert.assertEquals("Operation failed: \"The specified blob does not exist.\"", errorFields[0].trim());
      }
      else {
        Assert.assertEquals("Operation failed: \"The specified path does not exist.\"", errorFields[0].trim());
      }
      Assert.assertEquals("404", errorFields[1].trim());
      Assert.assertEquals("HEAD", errorFields[2].trim());
      Assert.assertTrue(errorFields[3].trim().startsWith("http"));
      Assert.assertTrue(errorFields[4].trim().startsWith("rId:"));
    }

    try {
      fs.listFiles(nonExistedFilePath2, false);
    } catch (Exception ex) {
      // verify its format
      String errorMessage = ex.getLocalizedMessage();
      String[] errorFields = errorMessage.split(",");
      // Flow is different for listStatusIterator being enabled or not.
      if (!getAbfsStore(fs).getAbfsConfiguration().enableAbfsListIterator()) {
        Assert.assertEquals(7, errorFields.length);
        // Check status message, status code, HTTP Request Type and URL.
        if (useBlobEndpoint) {
          Assert.assertEquals("Operation failed: \"The specified blob does not exist.\"", errorFields[0].trim());
        }
        else {
          Assert.assertEquals("Operation failed: \"The specified path does not exist.\"", errorFields[0].trim());
        }
        Assert.assertEquals("404", errorFields[1].trim());
        Assert.assertEquals("GET", errorFields[2].trim());
        Assert.assertTrue(errorFields[3].trim().startsWith("http"));
        Assert.assertTrue(errorFields[4].trim().startsWith("rId:"));
        // Check storage error code and storage error message.
        Assert.assertEquals("PathNotFound", errorFields[4].trim());
        Assert.assertTrue(errorFields[5].contains("RequestId")
                && errorFields[5].contains("Time"));
      } else {
        Assert.assertEquals(5, errorFields.length);
        // Check status message, status code, HTTP Request Type and URL.
        if (useBlobEndpoint) {
          Assert.assertEquals("Operation failed: \"The specified blob does not exist.\"", errorFields[0].trim());
        }
        else {
          Assert.assertEquals("Operation failed: \"The specified path does not exist.\"", errorFields[0].trim());
        }
        Assert.assertEquals("404", errorFields[1].trim());
        Assert.assertEquals("HEAD", errorFields[2].trim());
        Assert.assertTrue(errorFields[3].trim().startsWith("http"));
        Assert.assertTrue(errorFields[4].trim().startsWith("rId:"));
      }
    }
    // Check Exception Format For Put Method
    try {
      if (useBlobEndpoint) {
        Hashtable<String, String> metadata = new Hashtable<>();
        metadata.put("hi", "hello");
        fs.getAbfsStore().setBlobMetadata(fs.makeQualified(nonExistedFilePath1),
            metadata, getTestTracingContext(fs, true));
      }
    } catch (AbfsRestOperationException ex) {
      String errorMessage = ex.getLocalizedMessage();
      String[] errorFields = errorMessage.split(",");

      Assert.assertEquals(7, errorFields.length);
      // Check status message, status code, HTTP Request Type and URL.
      if (useBlobEndpoint) {
        Assert.assertEquals("Operation failed: \"The specified blob does not exist.\"", errorFields[0].trim());
      }
      else {
        Assert.assertEquals("Operation failed: \"The specified path does not exist.\"", errorFields[0].trim());
      }
      Assert.assertEquals("404", errorFields[1].trim());
      Assert.assertEquals("PUT", errorFields[2].trim());
      Assert.assertTrue(errorFields[3].trim().startsWith("http"));
      Assert.assertTrue(errorFields[4].trim().startsWith("rId:"));
    }
  }

  @Test
  public void testCustomTokenFetchRetryCount() throws Exception {
    testWithDifferentCustomTokenFetchRetry(0);
    testWithDifferentCustomTokenFetchRetry(3);
    testWithDifferentCustomTokenFetchRetry(5);
  }

  public void testWithDifferentCustomTokenFetchRetry(int numOfRetries) throws Exception {
    AzureBlobFileSystem fs = this.getFileSystem();

    Configuration config = Mockito.spy(new Configuration(this.getRawConfiguration()));
    String accountName = config.get("fs.azure.abfs.account.name");
    // Setup to configure custom token provider
    config.set("fs.azure.account.auth.type", "Custom");
    config.set("fs.azure.account.auth.type." + accountName, "Custom");
    config.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs"
            + ".azurebfs.oauth2.RetryTestTokenProvider");
    config.set("fs.azure.account.oauth.provider.type." + accountName, "org.apache.hadoop.fs"
        + ".azurebfs.oauth2.RetryTestTokenProvider");
    config.set("fs.azure.custom.token.fetch.retry.count", Integer.toString(numOfRetries));
    // Stop filesystem creation as it will lead to calls to store.
    config.set("fs.azure.createRemoteFileSystemDuringInitialization", "false");

    intercept(Exception.class,
            ()-> {
              final AzureBlobFileSystem fs1 =
                      (AzureBlobFileSystem) FileSystem.newInstance(fs.getUri(),
                              config);
            });

    // Number of retries done should be as configured
    Assert.assertEquals("Number of token fetch retries (" + RetryTestTokenProvider.reTryCount
            + ") done, does not match with fs.azure.custom.token.fetch.retry.count configured (" + numOfRetries
            + ")", RetryTestTokenProvider.reTryCount, numOfRetries);

    RetryTestTokenProvider.ResetStatusToFirstTokenFetch();
  }

  @Test
  public void testAuthFailException() throws Exception {
    Configuration config = Mockito.spy(new Configuration(getRawConfiguration()));
    String accountName = config
        .get(FS_AZURE_ABFS_ACCOUNT_NAME);
    // Setup to configure custom token provider
    config.set(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, "Custom");
    config.set(FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME,
            RETRY_TEST_TOKEN_PROVIDER);
    config.set(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME + DOT
        + accountName, "Custom");
    config.set(
        FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME + DOT + accountName,
        RETRY_TEST_TOKEN_PROVIDER);
    // Stop filesystem creation as it will lead to calls to store.
    config.set(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, "false");

    try {
      final AzureBlobFileSystem fs = getFileSystem(config);
      fail("Should fail at auth token fetch call");
    } catch (AbfsRestOperationException e) {
      String errorDesc = "Should throw RestOp exception on AAD failure";
      Assertions.assertThat(e.getStatusCode())
          .describedAs("Incorrect status code. " + errorDesc).isEqualTo(-1);
      Assertions.assertThat(e.getErrorCode())
          .describedAs("Incorrect error code. " + errorDesc)
          .isEqualTo(AzureServiceErrorCode.UNKNOWN);
      Assertions.assertThat(e.getErrorMessage())
          .describedAs("Incorrect error message. " + errorDesc)
          .contains("Auth failure: ");
    }
  }
}