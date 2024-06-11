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

import org.junit.Assume;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.utils.PathUtils;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.TRUE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_META_HDI_ISFOLDER;

public class BlobDirectoryStateHelper {

  /**
   * To assert that a path exists as implicit directory we need two things to assert.
   * 1. List blobs on the path should return some entries.
   * 2. GetBlobProperties on path should fail.
   * @param path to be checked
   * @param fs AzureBlobFileSystem for API calls
   * @return boolean whether the path exists as Implicit directory or not
   */
  public static boolean isImplicitDirectory(Path path, AzureBlobFileSystem fs,
      TracingContext testTracingContext) throws Exception {
    path = new Path(fs.makeQualified(path).toUri().getPath());
    String relativePath = PathUtils.getRelativePath(path);
    Assume.assumeTrue(fs.getAbfsStore().getAbfsConfiguration().getFsConfiguredServiceType() == AbfsServiceType.BLOB);
    AbfsBlobClient client = fs.getAbfsStore().getClientHandler().getBlobClient();
    AbfsRestOperation op = null;
    boolean isNotFound = false, isemptyList = false;

    // 1st condition
    op = client.listPath(relativePath, false, 1, null, testTracingContext);
    int listSize = op.getResult().getListResultSchema().paths().size();
    if (listSize == 0) {
      isemptyList = true;
    } else if (listSize > 0) {
      isemptyList = false;
    }

    // 2nd Condition
    try {
      op = client.getPathStatus(relativePath, testTracingContext, null, false);
      isNotFound = false;
    } catch (AbfsRestOperationException ex) {
      if (op != null && op.getResult().getStatusCode() == HTTP_NOT_FOUND) {
        isNotFound = true;
      } else {
        isNotFound = false;
      }
    }

    return !isemptyList && isNotFound;
  }

  /**
   * To assert that a path exists as explicit directory
   * For PrefixMode Blob: GetBlobProperties on path should succeed and marker should be present
   * For PrefixMode DFS: GetFileStatus on path should succeed and marker should be present
   * For Root on Blob: GetContainerProperty
   * @param path to be checked
   * @param fs AzureBlobFileSystem for API calls
   * @return boolean whether the path exists as Implicit directory or not
   */
  public static boolean isExplicitDirectory(Path path, AzureBlobFileSystem fs,
      TracingContext testTracingContext) throws Exception {
    path = new Path(fs.makeQualified(path).toUri().getPath());
    AbfsServiceType serviceType = fs.getAbfsStore().getAbfsConfiguration().getFsConfiguredServiceType();
    if (serviceType == AbfsServiceType.BLOB) {
      AbfsBlobClient client = fs.getAbfsStore().getClientHandler().getBlobClient();
      AbfsRestOperation op = null;
      try {
        op = client.getPathStatus(PathUtils.getRelativePath(path), testTracingContext, null, false);
        return op.getResult().getResponseHeader(X_MS_META_HDI_ISFOLDER) == TRUE;
      } catch (Exception ex) {
        return false;
      }
    } else {
      try {
        FileStatus fileStatus = fs.getFileStatus(path);
        return fileStatus.isDirectory();
      } catch (Exception ex) {
        return false;
      }
    }
  }
}