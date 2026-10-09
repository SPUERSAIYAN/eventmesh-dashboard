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
import org.apache.eventmesh.dashboard.console.domain.metadata.ClusterMetadataDomain;
import org.apache.eventmesh.dashboard.console.domain.metadata.MetadataAllDO;
import org.apache.eventmesh.dashboard.console.entity.cluster.ClusterEntity;
import org.apache.eventmesh.dashboard.console.entity.cluster.ClusterRelationshipEntity;
import org.apache.eventmesh.dashboard.console.entity.cluster.RuntimeEntity;
import org.apache.eventmesh.dashboard.console.entity.message.TopicEntity;
import org.apache.eventmesh.dashboard.console.function.health.Health2Service;
import org.apache.eventmesh.dashboard.console.mapper.SyncDataHandlerMapper;
import org.apache.eventmesh.dashboard.console.service.cluster.ClusterRelationshipService;
import org.apache.eventmesh.dashboard.console.service.cluster.ClusterService;
import org.apache.eventmesh.dashboard.console.service.cluster.RuntimeService;
import org.apache.eventmesh.dashboard.console.service.metadata.AbstractDBDataMetadataHandler;
import org.apache.eventmesh.dashboard.console.spring.support.DefaultDataHandler;
import org.apache.eventmesh.dashboard.console.spring.support.FunctionManage;
import org.apache.eventmesh.dashboard.console.spring.support.FunctionConfig;
import org.apache.eventmesh.dashboard.core.cluster.ClusterDO;
import org.apache.eventmesh.dashboard.core.cluster.RuntimeDO;
import org.apache.eventmesh.dashboard.core.function.SDK.SDKManage;
import org.apache.eventmesh.dashboard.core.metadata.MetadataSyncManage;

import java.time.LocalDateTime;
import java.util.List;

import org.junit.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class BaseIdRuntimeContractTest {
    @Test
    @SuppressWarnings("unchecked")
    public void databaseQueryFailurePreservesCursorAndRetryReadsTombstone() {
        TopicEntity cursor = new TopicEntity();
        LocalDateTime old = LocalDateTime.of(2026, 1, 1, 0, 0);
        cursor.setUpdateTime(old);
        TopicEntity tombstone = new TopicEntity();
        tombstone.setIsDelete(1);
        SyncDataHandlerMapper<TopicEntity> mapper = mock(SyncDataHandlerMapper.class);
        when(mapper.syncGet(cursor)).thenThrow(new IllegalStateException("read failure")).thenAnswer(call -> {
            assertEquals(old, cursor.getUpdateTime());
            return List.of(tombstone);
        });
        TopicHandler handler = new TopicHandler();
        ReflectionTestUtils.setField(handler, "baseRuntimeIdBase", cursor);
        ReflectionTestUtils.setField(handler, "syncDataHandlerMapper", mapper);
        assertThrows(IllegalStateException.class, handler::getData);
        assertEquals(old, cursor.getUpdateTime());
        assertSame(tombstone, handler.getData().get(0));
        assertTrue(cursor.getUpdateTime().isAfter(old));
        assertEquals(0, cursor.getUpdateTime().getNano());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void lifecycleApplyFailureRetainsCursorsAndRetryFinishesActualDomainRemoval() {
        FunctionManage manage = new FunctionManage();
        ReflectionTestUtils.setField(manage, "enabled", true);
        ReflectionTestUtils.setField(manage, "functionConfig", new FunctionConfig());
        RuntimeService runtimeService = mock(RuntimeService.class);
        ClusterService clusterService = mock(ClusterService.class);
        ClusterRelationshipService relationshipService = mock(ClusterRelationshipService.class);
        ReflectionTestUtils.setField(manage, "runtimeService", runtimeService);
        ReflectionTestUtils.setField(manage, "clusterService", clusterService);
        ReflectionTestUtils.setField(manage, "clusterRelationshipService", relationshipService);
        LocalDateTime old = LocalDateTime.of(2026, 1, 1, 0, 0);
        RuntimeEntity runtimeCursor = (RuntimeEntity) ReflectionTestUtils.getField(manage, "runtimeEntity");
        ClusterEntity clusterCursor = (ClusterEntity) ReflectionTestUtils.getField(manage, "clusterEntity");
        ClusterRelationshipEntity relationshipCursor =
            (ClusterRelationshipEntity) ReflectionTestUtils.getField(manage, "clusterRelationshipEntity");
        runtimeCursor.setUpdateTime(old);
        clusterCursor.setUpdateTime(old);
        relationshipCursor.setUpdateTime(old);
        ClusterEntity cluster = new ClusterEntity();
        cluster.setId(501L);
        cluster.setClusterType(ClusterType.EVENTMESH_RUNTIME);
        cluster.setStatus(1L);
        ClusterMetadataDomain domain = manage.registerBean();
        domain.rootClusterDHO();
        domain.handlerMetadata(MetadataAllDO.builder().clusterEntityList(List.of(cluster))
            .runtimeEntityList(List.of()).clusterRelationshipEntityList(List.of()).build());
        ClusterMetadataDomain.DataHandler handler = mock(ClusterMetadataDomain.DataHandler.class);
        domain.setHandler(handler);
        doThrow(new IllegalStateException("apply failure")).doNothing().when(handler).unRegisterCluster(any(), any(), any());
        cluster.setStatus(0L);
        when(runtimeService.queryByUpdateTime(any())).thenReturn(List.of());
        when(clusterService.queryByUpdateTime(any())).thenAnswer(call -> {
            assertEquals(old, clusterCursor.getUpdateTime());
            return List.of(cluster);
        });
        when(relationshipService.queryByUpdateTime(any())).thenReturn(List.of());
        assertThrows(IllegalStateException.class, manage::sync);
        assertEquals(old, runtimeCursor.getUpdateTime());
        assertEquals(old, clusterCursor.getUpdateTime());
        assertEquals(old, relationshipCursor.getUpdateTime());
        assertNotNull(domain.getColonyDO(501L));
        manage.sync();
        assertNull(domain.getColonyDO(501L));
        assertTrue(clusterCursor.getUpdateTime().isAfter(old));
        assertEquals(clusterCursor.getUpdateTime(), runtimeCursor.getUpdateTime());
        assertEquals(clusterCursor.getUpdateTime(), relationshipCursor.getUpdateTime());
        assertEquals(0, clusterCursor.getUpdateTime().getNano());
        verify(handler, times(2)).unRegisterCluster(any(), any(), any());
    }

    @Test
    public void actualDefaultHandlerReleasesOwnClusterAndRuntimeRegistrations() {
        DefaultDataHandler handler = new DefaultDataHandler();
        Health2Service health = mock(Health2Service.class);
        MetadataSyncManage metadata = mock(MetadataSyncManage.class);
        handler.setHealthService(health);
        handler.setMetadataSyncManage(metadata);
        ClusterEntity entity = new ClusterEntity();
        entity.setId(601L);
        entity.setClusterType(ClusterType.EVENTMESH_RUNTIME);
        ClusterDO cluster = new ClusterDO();
        ClusterMetadata clusterMetadata = new ClusterMetadata();
        clusterMetadata.setId(601L);
        cluster.setClusterInfo(clusterMetadata);
        RuntimeDO runtime = new RuntimeDO();
        RuntimeMetadata runtimeMetadata = new RuntimeMetadata();
        runtimeMetadata.setId(602L);
        runtime.setRuntimeMetadata(runtimeMetadata);
        cluster.getRuntimeMap().put(602L, runtime);
        SDKManage sdk = mock(SDKManage.class);
        try (MockedStatic<SDKManage> boundary = mockStatic(SDKManage.class)) {
            boundary.when(SDKManage::getInstance).thenReturn(sdk);
            handler.unRegisterCluster(entity, cluster, null);
            verify(health).unRegisterCluster(601L);
            verify(health, never()).unRegisterCluster(999L);
            verify(health).unRegister(runtimeMetadata);
            verify(metadata).unRegister(clusterMetadata);
            verify(metadata).unRegister(runtimeMetadata);
            verify(sdk).deleteClient(null, clusterMetadata.getUnique());
            verify(sdk).deleteClient(null, runtimeMetadata.getUnique());
        }
    }

    @Test
    public void emptyIncrementalSnapshotDoesNotAdvanceAnyFunctionCursor() {
        FunctionManage manage = new FunctionManage();
        FunctionConfig config = new FunctionConfig();
        config.setEnabledSync(true);
        ReflectionTestUtils.setField(manage, "functionConfig", config);
        RuntimeService runtimeService = mock(RuntimeService.class);
        ClusterService clusterService = mock(ClusterService.class);
        ClusterRelationshipService relationshipService = mock(ClusterRelationshipService.class);
        ReflectionTestUtils.setField(manage, "runtimeService", runtimeService);
        ReflectionTestUtils.setField(manage, "clusterService", clusterService);
        ReflectionTestUtils.setField(manage, "clusterRelationshipService", relationshipService);
        LocalDateTime old = LocalDateTime.of(2026, 1, 1, 0, 0);
        RuntimeEntity runtimeCursor = (RuntimeEntity) ReflectionTestUtils.getField(manage, "runtimeEntity");
        ClusterEntity clusterCursor = (ClusterEntity) ReflectionTestUtils.getField(manage, "clusterEntity");
        ClusterRelationshipEntity relationshipCursor =
            (ClusterRelationshipEntity) ReflectionTestUtils.getField(manage, "clusterRelationshipEntity");
        runtimeCursor.setUpdateTime(old);
        clusterCursor.setUpdateTime(old);
        relationshipCursor.setUpdateTime(old);
        when(runtimeService.queryByUpdateTime(any())).thenReturn(List.of());
        when(clusterService.queryByUpdateTime(any())).thenReturn(List.of());
        when(relationshipService.queryByUpdateTime(any())).thenReturn(List.of());

        manage.sync();

        assertEquals(old, runtimeCursor.getUpdateTime());
        assertEquals(old, clusterCursor.getUpdateTime());
        assertEquals(old, relationshipCursor.getUpdateTime());
    }

    @Test
    public void apClusterUnregisterClearsOnlyItsRuntimeAndClusterRegistrationsAndReplays() {
        DefaultDataHandler handler = new DefaultDataHandler();
        Health2Service health = mock(Health2Service.class);
        MetadataSyncManage metadata = mock(MetadataSyncManage.class);
        handler.setHealthService(health);
        handler.setMetadataSyncManage(metadata);
        ClusterEntity entity = new ClusterEntity();
        entity.setId(611L);
        entity.setClusterType(ClusterType.STORAGE_ROCKETMQ_NAMESERVER);
        ClusterDO cluster = new ClusterDO();
        ClusterMetadata clusterMetadata = new ClusterMetadata();
        clusterMetadata.setId(611L);
        cluster.setClusterInfo(clusterMetadata);
        RuntimeMetadata ownRuntime = new RuntimeMetadata();
        ownRuntime.setId(612L);
        ownRuntime.setClusterId(611L);
        ownRuntime.setName("own-runtime");
        RuntimeMetadata otherRuntime = new RuntimeMetadata();
        otherRuntime.setId(613L);
        otherRuntime.setClusterId(611L);
        otherRuntime.setName("other-runtime");
        RuntimeMetadata unrelatedRuntime = new RuntimeMetadata();
        unrelatedRuntime.setId(614L);
        unrelatedRuntime.setClusterId(999L);
        unrelatedRuntime.setName("unrelated-runtime");
        RuntimeDO own = new RuntimeDO();
        own.setRuntimeMetadata(ownRuntime);
        RuntimeDO other = new RuntimeDO();
        other.setRuntimeMetadata(otherRuntime);
        cluster.getRuntimeMap().put(612L, own);
        cluster.getRuntimeMap().put(613L, other);
        SDKManage sdk = mock(SDKManage.class);
        try (MockedStatic<SDKManage> boundary = mockStatic(SDKManage.class)) {
            boundary.when(SDKManage::getInstance).thenReturn(sdk);
            handler.unRegisterCluster(entity, cluster, null);
            handler.unRegisterCluster(entity, cluster, null);
            verify(health, times(2)).unRegisterCluster(611L);
            verify(health, times(2)).unRegister(same(ownRuntime));
            verify(health, times(2)).unRegister(same(otherRuntime));
            verify(metadata, times(2)).unRegister(same(ownRuntime));
            verify(metadata, times(2)).unRegister(same(otherRuntime));
            verify(metadata, times(2)).unRegister(clusterMetadata);
            verify(sdk, times(2)).deleteClient(null, ownRuntime.getUnique());
            verify(sdk, times(2)).deleteClient(null, otherRuntime.getUnique());
            verify(sdk, times(2)).deleteClient(null, clusterMetadata.getUnique());
            verify(health, never()).unRegisterCluster(613L);
            verify(health, never()).unRegister(same(unrelatedRuntime));
            verify(metadata, never()).unRegister(same(unrelatedRuntime));
            verify(sdk, never()).deleteClient(null, unrelatedRuntime.getUnique());
        }
    }

    static class TopicHandler extends AbstractDBDataMetadataHandler<TopicEntity> {
    }
}
