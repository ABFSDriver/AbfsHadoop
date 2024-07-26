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

import java.io.Closeable;

import org.apache.hadoop.fs.Path;

/**
 * Maintains resources taken in pre-check for a createNonRecursive to happen, and
 * own the release of the resources after createNonRecursive is done.
 */
public class CreateNonRecursiveCheckActionTaker implements Closeable {
  private final AbfsClient abfsClient;
  private final Path path;
  private final AbfsLease abfsLease;

  public CreateNonRecursiveCheckActionTaker(AbfsClient abfsClient, Path path, AbfsLease abfsLease) {
    this.abfsClient = abfsClient;
    this.path = path;
    this.abfsLease = abfsLease;
  }

  /**
   * Release the resources taken in pre-check for a createNonRecursive to happen.
   */
  @Override
  public void close() {
   if(abfsLease != null) {
      abfsLease.free();
    }
  }
}
