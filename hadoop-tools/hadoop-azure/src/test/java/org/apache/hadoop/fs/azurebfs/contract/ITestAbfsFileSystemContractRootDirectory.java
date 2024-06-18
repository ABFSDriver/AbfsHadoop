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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractRootDirectoryTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.junit.Ignore;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_CONTRACT_TEST_URI;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_SECURE_AZURE_CONTRACT_TEST_URI;

/**
 * Contract test for root directory operation.
 */
public class ITestAbfsFileSystemContractRootDirectory extends AbstractContractRootDirectoryTest {
  private final boolean isSecure;
  private final ABFSContractTestBinding binding;

  public ITestAbfsFileSystemContractRootDirectory() throws Exception {
    binding = new ABFSContractTestBinding();
    this.isSecure = binding.isSecureMode();
  }

  @Override
  public void setup() throws Exception {
    binding.setup();
    super.setup();
  }

  @Override
  protected Configuration createConfiguration() {
    return binding.getRawConfiguration();
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    conf = new Configuration(conf);
    String fsKey = conf.get(FS_DEFAULT_NAME_KEY);
    conf.set(FS_DEFAULT_NAME_KEY, addRootDirToken(fsKey));

    String contractTestFs = conf.get(FS_AZURE_CONTRACT_TEST_URI);
    conf.set(FS_AZURE_CONTRACT_TEST_URI, addRootDirToken(contractTestFs));

    contractTestFs = conf.get(FS_SECURE_AZURE_CONTRACT_TEST_URI);
    conf.set(FS_SECURE_AZURE_CONTRACT_TEST_URI, addRootDirToken(contractTestFs));

//    conf.get(FS_AZURE_CONTRACT_TEST_URI);
    return new AbfsFileSystemContract(conf, isSecure);
  }

  private String addRootDirToken(String fsKey) {
    String[] splits = fsKey.split("@");
    fsKey = splits[0] + "-dir@" + splits[1];
    return fsKey;
  }

  @Override
  @Ignore("ABFS always return false when non-recursively remove root dir")
  public void testRmNonEmptyRootDirNonRecursive() throws Throwable {
  }
}
