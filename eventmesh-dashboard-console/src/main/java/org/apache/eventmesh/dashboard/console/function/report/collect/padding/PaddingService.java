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

import org.apache.eventmesh.dashboard.common.model.metadata.ClusterMetadata;
import org.apache.eventmesh.dashboard.common.model.metadata.GroupMetadata;
import org.apache.eventmesh.dashboard.common.model.metadata.RuntimeMetadata;
import org.apache.eventmesh.dashboard.common.model.metadata.TopicMetadata;
import org.apache.eventmesh.dashboard.console.function.report.model.base.ClusterId;
import org.apache.eventmesh.dashboard.console.function.report.model.base.RuntimeId;

import java.util.List;


public class PaddingService implements Padding<ClusterId> {

    private final ClusterIdPadding clusterIdPadding = new ClusterIdPadding();

    private final RuntimePadding runtimePadding = new RuntimePadding();

    private final TopicIdPadding topicIdPadding = new TopicIdPadding();

    private final GroupIdPadding groupIdPadding = new GroupIdPadding();


    public void setClusterMetadata(List<ClusterMetadata> clusters) {
        clusterIdPadding.put(clusters);
    }

    public void setRuntimeMetadata(List<RuntimeMetadata> runtimeMetadata) {
        runtimePadding.put(runtimeMetadata);
    }

    public void setTopicMetadata(List<TopicMetadata> topicMetadata) {
        topicIdPadding.put(topicMetadata);
    }

    public void setGroupMetadata(List<GroupMetadata> groupMetadata) {
        groupIdPadding.put(groupMetadata);
    }

    @Override
    public Class<?>[] clazz() {
        return new Class[0];
    }

    @Override
    public void padding(ClusterId data) {
        clusterIdPadding.padding(data);
        if (data instanceof RuntimeId runtimeId) {
            runtimePadding.padding(runtimeId);
        }
        topicIdPadding.padding(data);
        groupIdPadding.padding(data);
    }

}
