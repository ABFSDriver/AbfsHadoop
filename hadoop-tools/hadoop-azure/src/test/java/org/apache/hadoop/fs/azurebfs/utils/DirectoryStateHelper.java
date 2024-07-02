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

package org.apache.hadoop.fs.azurebfs.utils;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

/**
 * Helper class to check the state of a directory as implicit or explicit.
 * With Blob Endpoint support, driver need to handle implicit paths in store at client side.
 * On DFS Endpoint, this handling is done on server side and driver works seamlessly.
 * This toll will be used by tests classes to assert that HDFS APIs work
 * seamlessly on implicit paths even with Blob Endpoint.
 */
public class DirectoryStateHelper {

  /**
   * DFS Endpoint abstracts nature of directory from user and hence there is no
   * way to detect implicit directory using DFS Endpoint APIs.
   * To assert that a path exists as implicit directory we need two things to assert.
   * 1. Blob Endpoint Listing on the path should return some entries.
   * 2. GetBlobProperties on path should fail on Blob Endpoint.
   * @param path to be checked. Can be relative or absolute.
   * @param fs AzureBlobFileSystem for API calls
   * @return boolean whether the path exists as Implicit directory or not
   */
  public static boolean isImplicitDirectory(Path path, AzureBlobFileSystem fs,
      TracingContext testTracingContext) throws Exception {
    path = new Path(fs.makeQualified(path).toUri().getPath());
    String relativePath = fs.getAbfsStore().getRelativePath(path);

    // Implicit nature can be checked only on Blob Endpoint.
    AbfsBlobClient client = fs.getAbfsStore().getClientHandler().getBlobClient();
    AbfsRestOperation op = null;
    boolean isNotFound, isemptyList = true;

    // 1st condition: listPaths should return some entries.
    op = client.listPath(relativePath, false, 1, null, testTracingContext);
    if (op != null && op.getResult() != null) {
      int listSize = op.getResult().getListResultSchema().paths().size();
      if (listSize == 0) {
        isemptyList = true;
      } else if (listSize > 0) {
        isemptyList = false;
      }
    }

    // 2nd Condition: getPathStatus fails with 404.
    try {
      op = client.getPathStatus(relativePath, testTracingContext, null, false);
      isNotFound = false;
    } catch (AbfsRestOperationException ex) {
      if (ex.getStatusCode() == HTTP_NOT_FOUND) {
        isNotFound = true;
      } else {
        isNotFound = false;
      }
    }

    return !isemptyList && isNotFound;
  }

  /**
   * To assert that a path exists as explicit directory, we need to assert that
   * marker blob exists on the path for both DFS and Blob Endpoint.
   * @param path to be checked
   * @param fs AzureBlobFileSystem for API calls
   * @return boolean whether the path exists as Implicit directory or not
   */
  public static boolean isExplicitDirectory(Path path, AzureBlobFileSystem fs,
      TracingContext testTracingContext) throws Exception {
    path = new Path(fs.makeQualified(path).toUri().getPath());
    AbfsClient client = fs.getAbfsStore().getClient();
    AbfsRestOperation op = null;
    try {
      op = client.getPathStatus(fs.getAbfsStore().getRelativePath(path), true, testTracingContext, null);
      return client.checkIsDir(op.getResult());
    } catch (Exception ex) {
      return false;
    }
  }
}
