/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eventmesh.dashboard.console.function.report.collect.padding;

import org.apache.eventmesh.dashboard.common.model.metadata.RuntimeMetadata;
import org.apache.eventmesh.dashboard.console.function.report.model.base.RuntimeId;

import java.util.Objects;


public class RuntimePadding extends AbstractPadding<RuntimeId, RuntimeMetadata> {


    {
        this.setIdFunction(RuntimeMetadata::getClusterId);
        this.setKeyFunction(RuntimeMetadata::getName);
    }

    @Override
    public Class<?>[] clazz() {
        return new Class[] {RuntimeId.class};
    }

    @Override
    public void padding(RuntimeId runtimeId) {
        if (Objects.nonNull(runtimeId.getRuntimeId())) {
            return;
        }
        RuntimeMetadata runtimeMetadata = this.getData(runtimeId.getClustersId(), runtimeId.getRuntimeName());
        if (Objects.isNull(runtimeMetadata)) {
            return;
        }
        runtimeId.setRuntimeId(runtimeMetadata.getId());
    }
}
