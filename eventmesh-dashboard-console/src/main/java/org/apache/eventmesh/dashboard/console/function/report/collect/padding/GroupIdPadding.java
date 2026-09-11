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

import org.apache.eventmesh.dashboard.common.enums.ClusterFramework;
import org.apache.eventmesh.dashboard.common.enums.ClusterSyncMetadataEnum;
import org.apache.eventmesh.dashboard.common.model.metadata.GroupMetadata;
import org.apache.eventmesh.dashboard.console.function.report.model.base.ClusterId;
import org.apache.eventmesh.dashboard.console.function.report.model.base.cap.CapSubscribeId;
import org.apache.eventmesh.dashboard.console.function.report.model.base.cap.GapGroupId;
import org.apache.eventmesh.dashboard.console.function.report.model.base.not.GroupId;
import org.apache.eventmesh.dashboard.console.function.report.model.base.not.SubscribeId;


public class GroupIdPadding extends AbstractPadding<ClusterId, GroupMetadata> {

    {
        this.setIdFunction((data) -> {
            ClusterFramework clusterFramework = ClusterSyncMetadataEnum.getClusterFramework(data.getClusterType());
            if (clusterFramework.isCAP()) {
                return data.getClusterId();
            }
            return data.getRuntimeId();
        });
        this.setKeyFunction(GroupMetadata::getName);
    }


    @Override
    public Class<?>[] clazz() {
        return new Class[] {GroupId.class, SubscribeId.class, GapGroupId.class, CapSubscribeId.class};
    }

    @Override
    public void padding(ClusterId data) {
        if (data instanceof GroupId groupId) {
            groupId.setGroupId(this.getData(groupId.getRuntimeId(), groupId.getGroupName()).getId());
        } else if (data instanceof SubscribeId subscribeId) {
            GroupMetadata groupMetadata = this.getData(subscribeId.getRuntimeId(), subscribeId.getGroupName());
            subscribeId.setGroupId(groupMetadata.getId());
        } else if (data instanceof GapGroupId capGroupId) {
            GroupMetadata groupMetadata = this.getData(capGroupId.getClustersId(), capGroupId.getGroupName());
            capGroupId.setGroupId(groupMetadata.getId());
        } else if (data instanceof CapSubscribeId capSubscribeId) {
            GroupMetadata groupMetadata = this.getData(capSubscribeId.getClustersId(), capSubscribeId.getGroupName());
            capSubscribeId.setGroupId(groupMetadata.getId());
        }
    }
}
