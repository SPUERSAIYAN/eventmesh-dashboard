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
import org.apache.eventmesh.dashboard.common.model.base.BaseOrganizationBase;
import org.apache.eventmesh.dashboard.common.model.metadata.ClusterMetadata;
import org.apache.eventmesh.dashboard.common.model.metadata.ConfigMetadata;
import org.apache.eventmesh.dashboard.common.model.metadata.GroupMemberMetadata;
import org.apache.eventmesh.dashboard.common.model.metadata.GroupMetadata;
import org.apache.eventmesh.dashboard.common.model.metadata.RuntimeMetadata;
import org.apache.eventmesh.dashboard.common.model.metadata.TopicMetadata;
import org.apache.eventmesh.dashboard.console.domain.metadata.ClusterMetadataDomain;
import org.apache.eventmesh.dashboard.console.domain.metadata.MetadataAllDO;
import org.apache.eventmesh.dashboard.console.entity.cluster.ClusterEntity;
import org.apache.eventmesh.dashboard.console.entity.cluster.ClusterRelationshipEntity;
import org.apache.eventmesh.dashboard.console.entity.cluster.RuntimeEntity;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StatusUnitMetadataTest {
    private static final LocalDateTime TIME = LocalDateTime.of(2026, 10, 8, 0, 0);

    @Test
    public void logicalDeletionWinsForEveryMetadataFamily() {
        for (BaseOrganizationBase value : metadataValues()) {
            value.setStatus(1L);
            value.setIsDelete(1);
            value.setCreateTime(TIME);
            value.setUpdateTime(TIME);

            assertTrue(value.getClass().getSimpleName(), value.isDelete());
            assertFalse(value.getClass().getSimpleName(), value.isInsert());
            assertFalse(value.getClass().getSimpleName(), value.isUpdate());
        }
    }

    @Test
    public void stoppedClusterAndRuntimeAreNotLogicalDeletionsAndUnknownStatesDoNotActivate() {
        for (BaseOrganizationBase value : Arrays.asList(new ClusterMetadata(), new RuntimeMetadata())) {
            value.setStatus(0L);
            assertFalse(value.getClass().getSimpleName(), value.isDelete());
            value.setStatus(null);
            assertFalse(value.getClass().getSimpleName(), value.isDelete());
            value.setStatus(9L);
            assertFalse(value.getClass().getSimpleName(), value.isDelete());
        }

        for (BaseOrganizationBase value : Arrays.asList(new GroupMetadata(), new GroupMemberMetadata())) {
            value.setStatus(null);
            assertFalse(value.getClass().getSimpleName(), value.isDelete());
            value.setStatus(9L);
            assertFalse(value.getClass().getSimpleName(), value.isDelete());
        }
    }

    @Test
    public void legacyInactiveStatusMeansDeletionOnlyForTopicAndConfig() {
        for (BaseOrganizationBase value : Arrays.asList(new TopicMetadata(), new ConfigMetadata())) {
            value.setStatus(0L);
            assertTrue(value.getClass().getSimpleName(), value.isDelete());
        }

        GroupMemberMetadata member = new GroupMemberMetadata();
        member.setStatus(0L);
        assertFalse(member.isDelete());
    }

    @Test
    public void configIsActiveOnlyAtStatusOneAndOnlyIsDeleteMeansDeleted() {
        ConfigMetadata config = new ConfigMetadata();
        config.setIsDelete(0);
        for (Long status : Arrays.asList(1L, null, 9L)) {
            config.setStatus(status);
            assertFalse("active config status=" + status, config.isDelete());
        }
        config.setStatus(1L);
        config.setIsDelete(1);
        assertTrue(config.isDelete());
    }

    @Test
    public void missingTimestampsNeverClassifyAsInsert() {
        TopicMetadata metadata = new TopicMetadata();
        assertFalse(metadata.isInsert());
        assertTrue(metadata.isUpdate());

        metadata.setCreateTime(TIME);
        metadata.setUpdateTime(null);
        assertFalse(metadata.isInsert());
        assertTrue(metadata.isUpdate());
    }

    @Test
    public void unknownClusterOrRuntimeStatusDoesNotRegisterInTopology() {
        ClusterMetadataDomain domain = new ClusterMetadataDomain();
        domain.rootClusterDHO();
        ReflectionTestUtils.setField(domain, "buildConfig", false);
        ClusterEntity cluster = new ClusterEntity();
        cluster.setId(41L);
        cluster.setClusterType(ClusterType.EVENTMESH_RUNTIME);
        cluster.setStatus(9L);
        domain.handlerMetadata(MetadataAllDO.builder().clusterEntityList(List.of(cluster))
            .runtimeEntityList(List.of()).clusterRelationshipEntityList(List.of()).build());
        assertNull(domain.getColonyDO(41L));

        cluster.setStatus(1L);
        domain.handlerMetadata(MetadataAllDO.builder().clusterEntityList(List.of(cluster))
            .runtimeEntityList(List.of()).clusterRelationshipEntityList(List.of()).build());
        RuntimeEntity runtime = new RuntimeEntity();
        runtime.setId(51L);
        runtime.setClusterId(41L);
        runtime.setClusterType(ClusterType.EVENTMESH_RUNTIME);
        runtime.setStatus(null);
        domain.handlerMetadata(MetadataAllDO.builder().clusterEntityList(List.of())
            .runtimeEntityList(List.of(runtime)).clusterRelationshipEntityList(List.of()).build());
        assertFalse(domain.getColonyDO(41L).getClusterDO().getRuntimeMap().containsKey(51L));
    }

    @Test
    public void unknownRelationshipStatusDoesNotCreateTopologyLink() {
        for (Long status : Arrays.asList(null, 9L)) {
            ClusterMetadataDomain domain = new ClusterMetadataDomain();
            domain.rootClusterDHO();
            ReflectionTestUtils.setField(domain, "buildConfig", false);
            ClusterEntity parent = cluster(61L, ClusterType.EVENTMESH_CLUSTER);
            ClusterEntity child = cluster(62L, ClusterType.EVENTMESH_RUNTIME);
            ClusterRelationshipEntity relationship = new ClusterRelationshipEntity();
            relationship.setClusterId(61L);
            relationship.setRelationshipId(62L);
            relationship.setRelationshipType(ClusterType.EVENTMESH_RUNTIME);
            relationship.setStatus(status);
            domain.handlerMetadata(MetadataAllDO.builder().clusterEntityList(Arrays.asList(parent, child))
                .runtimeEntityList(List.of()).clusterRelationshipEntityList(List.of(relationship)).build());

            assertTrue(domain.getColonyDO(61L).getRuntimeColonyDOMap().isEmpty());
            assertNull(domain.getColonyDO(62L).getSuperiorId());
        }
    }

    private ClusterEntity cluster(Long id, ClusterType clusterType) {
        ClusterEntity cluster = new ClusterEntity();
        cluster.setId(id);
        cluster.setClusterType(clusterType);
        cluster.setStatus(1L);
        return cluster;
    }

    private List<BaseOrganizationBase> metadataValues() {
        return Arrays.asList(new ClusterMetadata(), new RuntimeMetadata(), new TopicMetadata(),
            new ConfigMetadata(), new GroupMetadata(), new GroupMemberMetadata());
    }
}
