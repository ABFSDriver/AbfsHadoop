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

/**
 * Describes the endpoint and Direct Read handle that should be used
 * for a read request.
 *
 * @param endpoint endpoint selected from BlobLayout; null if no
 *                 endpoint-specific routing is required
 * @param handle Direct Read data handle; null if unavailable
 * @param maxLength maximum number of bytes that may be read using
 *                  this target
 */
public record ReadTarget(
    String endpoint,
    String handle,
    int maxLength) {

  /**
   * @return true if this target contains a Direct Read handle.
   */
  public boolean hasHandle() {
    return handle != null;
  }

  /**
   * @return true if this target contains an endpoint override.
   */
  public boolean hasEndpoint() {
    return endpoint != null;
  }
}
