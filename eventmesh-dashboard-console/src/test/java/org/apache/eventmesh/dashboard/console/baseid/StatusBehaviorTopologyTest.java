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

package org.apache.eventmesh.dashboard.console.baseid;

import org.apache.eventmesh.dashboard.common.enums.ClusterType;
import org.apache.eventmesh.dashboard.common.model.metadata.ClusterMetadata;
import org.apache.eventmesh.dashboard.common.model.metadata.RuntimeMetadata;
import org.apache.eventmesh.dashboard.core.cluster.ClusterDO;
import org.apache.eventmesh.dashboard.core.cluster.ColonyDO;
import org.apache.eventmesh.dashboard.core.cluster.RuntimeDO;
import org.apache.eventmesh.dashboard.core.function.SDK.config.AbstractMultiCreateSDKConfig;
import org.apache.eventmesh.dashboard.core.function.SDK.config.NetAddress;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class StatusBehaviorTopologyTest {

    @Test
    public void replacingClusterKeepsRuntimeAndAddressStateAndRefreshesMetadata() {
        long clusterId = 9601L;
        long runtimeId = 9602L;
        RuntimeDO runtime = new RuntimeDO();
        RuntimeMetadata runtimeMetadata = new RuntimeMetadata();
        runtimeMetadata.setId(runtimeId);
        runtime.setRuntimeMetadata(runtimeMetadata);
        Map<Long, RuntimeDO> runtimeMap = new ConcurrentHashMap<>();
        runtimeMap.put(runtimeId, runtime);

        AbstractMultiCreateSDKConfig config = new AbstractMultiCreateSDKConfig() {
            @Override
            protected String uniqueKey() {
                return "test-";
            }
        };
        config.addNetAddress(NetAddress.create("127.0.0.1", 19091));

        ClusterMetadata oldMetadata = new ClusterMetadata();
        oldMetadata.setId(clusterId);
        ClusterDO oldCluster = new ClusterDO();
        oldCluster.setClusterInfo(oldMetadata);
        oldCluster.setRuntimeMap(runtimeMap);
        oldCluster.setMultiCreateSDKConfig(config);

        ColonyDO<ClusterDO> root = new ColonyDO<>();
        Map<Long, ColonyDO<ClusterDO>> colonies = new ConcurrentHashMap<>();
        root.setAllColonyDO(colonies);
        root.setClusterBaseDOClass(ClusterDO.class);
        ColonyDO<ClusterDO> current = new ColonyDO<>();
        current.setClusterId(clusterId);
        current.setClusterType(ClusterType.STORAGE_KAFKA_BROKER);
        current.setClusterDO(oldCluster);
        colonies.put(clusterId, current);

        ClusterMetadata refreshedMetadata = new ClusterMetadata();
        refreshedMetadata.setId(clusterId);
        ClusterDO replacement = new ClusterDO();
        replacement.setClusterInfo(refreshedMetadata);

        ColonyDO<ClusterDO> result = root.register(clusterId, ClusterType.STORAGE_KAFKA_BROKER, replacement);

        assertSame(current, result);
        assertSame(replacement, current.getClusterDO());
        assertSame(refreshedMetadata, current.getClusterDO().getClusterInfo());
        assertSame(runtimeMap, current.getClusterDO().getRuntimeMap());
        assertSame(runtime, current.getClusterDO().getRuntimeMap().get(runtimeId));
        assertSame(config, current.getClusterDO().getMultiCreateSDKConfig());
        assertEquals("127.0.0.1:19091", current.getClusterDO().getMultiCreateSDKConfig().getNetAddresses()[0]);

        long otherClusterId = 9603L;
        ColonyDO<ClusterDO> other = new ColonyDO<>();
        other.setClusterId(otherClusterId);
        other.setClusterDO(new ClusterDO());
        colonies.put(otherClusterId, other);
        assertSame(current, root.remove(clusterId));
        assertSame(other, colonies.get(otherClusterId));
    }
}
