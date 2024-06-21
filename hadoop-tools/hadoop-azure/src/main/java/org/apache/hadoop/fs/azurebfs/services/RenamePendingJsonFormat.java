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

import com.fasterxml.jackson.annotation.JsonProperty;

class RenamePendingJsonFormat {

  @JsonProperty(value = "FormatVersion")
  private final String formatVersion = "1.0";

  @JsonProperty(value = "OperationUTCTime")
  private final String operationUTCTime;

  @JsonProperty(value = "OldFolderName")
  private final String oldFolderName;

  @JsonProperty(value = "NewFolderName")
  private final String newFolderName;

  @JsonProperty(value = "ETag")
  private final String eTag;

  RenamePendingJsonFormat(
      String oldFolderName,
      String newFolderName,
      String operationUTCTime,
      String eTag) {
    this.operationUTCTime = operationUTCTime;
    this.oldFolderName = oldFolderName;
    this.newFolderName = newFolderName;
    this.eTag = eTag;
  }
}
