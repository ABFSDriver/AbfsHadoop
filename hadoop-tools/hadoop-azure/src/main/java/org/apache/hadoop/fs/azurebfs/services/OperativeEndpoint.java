/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;

/**
 * This class is mainly to unify the fallback for all API's to DFS endpoint at a single spot.
 */
public class OperativeEndpoint {
    private final AbfsConfiguration abfsConfiguration;

    public OperativeEndpoint(AbfsConfiguration configuration) {
        this.abfsConfiguration = configuration;
    }

    public boolean isOperationEnabledOnDFS(FSOperationType operationType, PrefixMode... prefixModes) {
        PrefixMode prefixMode = (prefixModes.length > 0) ? prefixModes[0] : abfsConfiguration.getPrefixMode();
        switch (operationType) {
            case MKDIR:
                return prefixMode != PrefixMode.BLOB || abfsConfiguration.shouldMkdirFallbackToDfs();
            case CREATE:
            case APPEND:
                return prefixMode != PrefixMode.BLOB || abfsConfiguration.shouldIngressFallbackToDfs();
            case READ:
                return prefixMode != PrefixMode.BLOB || abfsConfiguration.shouldReadFallbackToDfs();
            default:
                throw new IllegalArgumentException("Invalid operation type");
        }
    }
}
