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

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class AbfsDfsLease extends AbfsLease {

  public AbfsDfsLease(final AbfsClient client,
      final String path,
      final int acquireMaxRetries,
      final int acquireRetryInterval,
      final Integer leaseDuration,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    super(client, path, acquireMaxRetries, acquireRetryInterval, leaseDuration,
        tracingContext);
  }

  public AbfsDfsLease(final AbfsClient client,
      final String path,
      final Integer leaseDuration,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    super(client, path, leaseDuration, tracingContext);
  }

  @Override
  String callRenewLeaseAPI(final String path,
      final String leaseId,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    AbfsRestOperation op = client.renewLease(path, leaseId, tracingContext);
    return op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_LEASE_ID);
  }

  @Override
  AbfsRestOperation callAcquireLeaseAPI(final String path, final Integer leaseDuration,
      final TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    return client.acquireLease(path,
        leaseDuration, tracingContext);
  }

  @Override
  void callReleaseLeaseAPI(final String path, final String leaseID, final TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    client.releaseLease(path, leaseID, tracingContext);
  }
}
