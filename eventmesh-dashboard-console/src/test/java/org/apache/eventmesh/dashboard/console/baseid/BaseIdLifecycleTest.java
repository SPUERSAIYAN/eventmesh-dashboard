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
import org.apache.eventmesh.dashboard.console.domain.metadata.ClusterMetadataDomain;
import org.apache.eventmesh.dashboard.console.domain.metadata.MetadataAllDO;
import org.apache.eventmesh.dashboard.console.entity.cluster.ClusterEntity;
import org.apache.eventmesh.dashboard.console.entity.cluster.ClusterRelationshipEntity;
import org.apache.eventmesh.dashboard.console.entity.cluster.RuntimeEntity;
import org.apache.eventmesh.dashboard.console.mapper.cluster.ClusterMapper;
import org.apache.eventmesh.dashboard.console.mapper.cluster.ClusterRelationshipMapper;
import org.apache.eventmesh.dashboard.console.mapper.cluster.RuntimeMapper;
import org.apache.eventmesh.dashboard.console.service.cluster.impl.RuntimeServiceImpl;
import org.apache.eventmesh.dashboard.console.service.cluster.ClusterRelationshipService;
import org.apache.eventmesh.dashboard.console.service.cluster.ClusterService;
import org.apache.eventmesh.dashboard.console.service.cluster.RuntimeService;
import org.apache.eventmesh.dashboard.core.cluster.ClusterDO;
import org.apache.eventmesh.dashboard.core.cluster.ColonyDO;
import org.apache.eventmesh.dashboard.core.function.SDK.ConfigManage;
import org.apache.eventmesh.dashboard.core.function.SDK.config.AbstractMultiCreateSDKConfig;
import org.apache.eventmesh.dashboard.core.function.SDK.config.AbstractSimpleCreateSDKConfig;
import org.apache.eventmesh.dashboard.console.spring.support.FunctionConfig;
import org.apache.eventmesh.dashboard.console.spring.support.FunctionManage;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class BaseIdLifecycleTest extends BaseIdDatabaseTest {
    @Test
    @SuppressWarnings("unchecked")
    public void runtimeDeactivateTraversesServiceMapperDomainAndCapAddressRefresh() throws Exception {
        for (ClusterType type : List.of(ClusterType.EVENTMESH_RUNTIME, ClusterType.STORAGE_KAFKA_BROKER)) {
            sql("delete from runtime");
            sql("delete from cluster");
            seed("cluster", ClusterEntity.class, 100, Map.of("cluster_type", type.name(), "name", "cluster"));
            seed("runtime", RuntimeEntity.class, 201, Map.of("cluster_id", 100, "cluster_type", type.name(), "host", 101,
                "kubernetes_cluster_id", 201));
            seed("runtime", RuntimeEntity.class, 202, Map.of("cluster_id", 100, "cluster_type", type.name(), "host", 102,
                "kubernetes_cluster_id", 202));
            ConfigManage configs = mock(ConfigManage.class);
            when(configs.getMultiCreateSdkConfig(any(), any())).thenAnswer(call -> new AbstractMultiCreateSDKConfig());
            when(configs.getSimpleCreateSdkConfig(any(), any())).thenAnswer(call -> new AbstractSimpleCreateSDKConfig());
            try (MockedStatic<ConfigManage> configBoundary = mockStatic(ConfigManage.class)) {
                configBoundary.when(ConfigManage::getInstance).thenReturn(configs);
                ClusterMetadataDomain domain = new ClusterMetadataDomain();
                domain.rootClusterDHO();
                ClusterMetadataDomain.DataHandler handler = mock(ClusterMetadataDomain.DataHandler.class);
                domain.setHandler(handler);
                load(domain, true);
                ColonyDO<ClusterDO> colony = domain.getColonyDO(100L);
                assertEquals(2, colony.getClusterDO().getRuntimeMap().size());
                RuntimeEntity runtime = new RuntimeEntity();
                runtime.setId(201L);
                RuntimeServiceImpl service = new RuntimeServiceImpl();
                ReflectionTestUtils.setField(service, "runtimeMapper", session.getMapper(RuntimeMapper.class));
                service.deactivate(runtime);
                load(domain, false);
                assertFalse(colony.getClusterDO().getRuntimeMap().containsKey(201L));
                assertTrue(colony.getClusterDO().getRuntimeMap().containsKey(202L));
                if (type == ClusterType.STORAGE_KAFKA_BROKER) {
                    assertArrayEquals(new String[]{"102:0"}, colony.getClusterDO().getMultiCreateSDKConfig().getNetAddresses());
                    verify(handler, atLeastOnce()).registerCluster(argThat(value -> value != null && value.getId() == 100L),
                        any(), same(colony));
                } else {
                    verify(handler).unRegisterRuntime(argThat(value -> value.getId() == 201L && value.getClusterId() == 100L),
                        isNull(), same(colony));
                }
                sql("update runtime set is_delete=1 where id=202");
                load(domain, false);
                assertTrue(colony.getClusterDO().getRuntimeMap().isEmpty());
                if (type == ClusterType.STORAGE_KAFKA_BROKER) {
                    assertArrayEquals(new String[0], colony.getClusterDO().getMultiCreateSDKConfig().getNetAddresses());
                    verify(handler).unRegisterCluster(argThat(value -> value != null && value.getId() == 100L), any(), same(colony));
                }
                load(domain, false);
                assertTrue(colony.getClusterDO().getRuntimeMap().isEmpty());
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void relationshipThreeZeroAndDeletionDetachWithoutRemovingOtherEndpoint() throws Exception {
        seed("cluster", ClusterEntity.class, 101, Map.of("name", "main"));
        seed("cluster", ClusterEntity.class, 102, Map.of("name", "child"));
        seed("cluster_relationship", ClusterRelationshipEntity.class, 301,
            Map.of("cluster_id", 101, "relationship_id", 102, "relationship_type", ClusterType.EVENTMESH_RUNTIME.name()));
        ClusterMetadataDomain domain = new ClusterMetadataDomain();
        domain.rootClusterDHO();
        load(domain, true);
        ColonyDO<ClusterDO> main = domain.getColonyDO(101L);
        ColonyDO<ClusterDO> child = domain.getColonyDO(102L);
        assertSame(child, main.getRuntimeColonyDOMap().get(102L));
        assertEquals(Long.valueOf(101), child.getSuperiorId());
        ClusterRelationshipEntity relationship = new ClusterRelationshipEntity();
        relationship.setId(301L);
        assertEquals(Integer.valueOf(1), session.getMapper(ClusterRelationshipMapper.class).relieveRelationship(relationship));
        load(domain, false);
        assertNull(child.getSuperiorId());
        assertTrue(main.getRuntimeColonyDOMap().isEmpty());
        assertSame(child, domain.getColonyDO(102L));
        load(domain, false);
        for (String change : List.of("status=0", "status=1,is_delete=1")) {
            sql("update cluster_relationship set status=1,is_delete=0 where id=301");
            load(domain, false);
            assertEquals(Long.valueOf(101), child.getSuperiorId());
            sql("update cluster_relationship set " + change + " where id=301");
            load(domain, false);
            assertNull(child.getSuperiorId());
            assertTrue(main.getRuntimeColonyDOMap().isEmpty());
        }
        sql("update cluster set status=0 where id=101");
        load(domain, true);
        assertNull(domain.getColonyDO(101L));
        assertSame(child, domain.getColonyDO(102L));
        load(domain, false);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void clusterDisabledOrDeletedIsRemovedUsingOwnIdAndRepeatedChangesAreSafe() throws Exception {
        for (String removal : List.of("status=0", "is_delete=1")) {
            sql("delete from cluster");
            seed("cluster", ClusterEntity.class, 103, Map.of("name", "removed"));
            ClusterMetadataDomain domain = new ClusterMetadataDomain();
            domain.rootClusterDHO();
            ClusterMetadataDomain.DataHandler handler = mock(ClusterMetadataDomain.DataHandler.class);
            domain.setHandler(handler);
            load(domain, true);
            sql("update cluster set " + removal + " where id=103");
            load(domain, true);
            assertNull(domain.getColonyDO(103L));
            verify(handler).unRegisterCluster(argThat(value -> value.getId() == 103L), any(), any());
            load(domain, true);
            verify(handler, times(1)).unRegisterCluster(any(), any(), any());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void reenabledClusterReloadsUnchangedRuntimeAndRelationshipAndRetriesFailedSnapshot() throws Exception {
        seed("cluster", ClusterEntity.class, 111, Map.of("cluster_type", ClusterType.EVENTMESH_CLUSTER.name(), "name", "parent"));
        seed("cluster", ClusterEntity.class, 112, Map.of("cluster_type", ClusterType.EVENTMESH_RUNTIME.name(), "name", "child"));
        seed("runtime", RuntimeEntity.class, 211, Map.of("cluster_id", 112, "cluster_type", ClusterType.EVENTMESH_RUNTIME.name(),
            "host", 2130706433, "port", 10911));
        seed("cluster_relationship", ClusterRelationshipEntity.class, 311,
            Map.of("cluster_id", 111, "relationship_id", 112, "relationship_type", ClusterType.EVENTMESH_RUNTIME.name()));

        ClusterMetadataDomain domain;
        FunctionManage functionManage = new FunctionManage();
        ReflectionTestUtils.setField(functionManage, "functionConfig", new FunctionConfig());
        RuntimeMapper runtimeMapper = session.getMapper(RuntimeMapper.class);
        ClusterMapper clusterMapper = session.getMapper(ClusterMapper.class);
        ClusterRelationshipMapper relationshipMapper = session.getMapper(ClusterRelationshipMapper.class);
        AtomicBoolean failSnapshot = new AtomicBoolean();
        RuntimeService runtimeService = mock(RuntimeService.class);
        when(runtimeService.queryByUpdateTime(any())).thenAnswer(call -> {
            RuntimeEntity query = call.getArgument(0);
            if (failSnapshot.get() && query.getUpdateTime().equals(LocalDateTime.of(2000, 1, 1, 0, 0))) {
                throw new IllegalStateException("runtime snapshot failed");
            }
            return runtimeMapper.queryByUpdateTime(query);
        });
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.queryByUpdateTime(any())).thenAnswer(call -> clusterMapper.queryClusterByUpdate(call.getArgument(0)));
        ClusterRelationshipService relationshipService = mock(ClusterRelationshipService.class);
        when(relationshipService.queryByUpdateTime(any())).thenAnswer(call ->
            relationshipMapper.queryNewlyIncreased(call.getArgument(0)));
        ReflectionTestUtils.setField(functionManage, "runtimeService", runtimeService);
        ReflectionTestUtils.setField(functionManage, "clusterService", clusterService);
        ReflectionTestUtils.setField(functionManage, "clusterRelationshipService", relationshipService);
        domain = functionManage.registerBean();
        ReflectionTestUtils.setField(domain, "buildConfig", false);
        domain.rootClusterDHO();
        ClusterMetadataDomain.DataHandler handler = mock(ClusterMetadataDomain.DataHandler.class);
        domain.setHandler(handler);

        ConfigManage configs = mock(ConfigManage.class);
        when(configs.getSimpleCreateSdkConfig(any(), any())).thenAnswer(call -> new AbstractSimpleCreateSDKConfig());
        try (MockedStatic<ConfigManage> configBoundary = mockStatic(ConfigManage.class)) {
            configBoundary.when(ConfigManage::getInstance).thenReturn(configs);
            functionManage.sync();
            ColonyDO<ClusterDO> parent = domain.getColonyDO(111L);
            ColonyDO<ClusterDO> child = domain.getColonyDO(112L);
            assertNotNull(parent);
            assertSame(child, parent.getRuntimeColonyDOMap().get(112L));
            assertTrue(child.getClusterDO().getRuntimeMap().containsKey(211L));
            assertEquals(Long.valueOf(111L), child.getSuperiorId());

            LocalDateTime stoppedAt = LocalDateTime.now().plusMinutes(2).withNano(0);
            sql("update cluster set status=0, update_time='" + formatSqlTime(stoppedAt) + "' where id=111");
            functionManage.sync();
            assertNull(domain.getColonyDO(111L));
            assertNotNull(domain.getColonyDO(112L));
            assertNull(domain.getColonyDO(112L).getSuperiorId());

            LocalDateTime resumedAt = stoppedAt.plusSeconds(2);
            sql("update cluster set status=1, update_time='" + formatSqlTime(resumedAt) + "' where id=111");
            LocalDateTime runtimeCursor = ((RuntimeEntity) ReflectionTestUtils.getField(functionManage, "runtimeEntity")).getUpdateTime();
            LocalDateTime clusterCursor = ((ClusterEntity) ReflectionTestUtils.getField(functionManage, "clusterEntity")).getUpdateTime();
            LocalDateTime relationshipCursor = ((ClusterRelationshipEntity) ReflectionTestUtils.getField(functionManage,
                "clusterRelationshipEntity")).getUpdateTime();
            failSnapshot.set(true);
            try {
                functionManage.sync();
                fail("Snapshot read failure must propagate");
            } catch (IllegalStateException expected) {
                assertEquals("runtime snapshot failed", expected.getMessage());
            }
            assertEquals(runtimeCursor, ((RuntimeEntity) ReflectionTestUtils.getField(functionManage, "runtimeEntity")).getUpdateTime());
            assertEquals(clusterCursor, ((ClusterEntity) ReflectionTestUtils.getField(functionManage, "clusterEntity")).getUpdateTime());
            assertEquals(relationshipCursor, ((ClusterRelationshipEntity) ReflectionTestUtils.getField(functionManage,
                "clusterRelationshipEntity")).getUpdateTime());
            assertNull(domain.getColonyDO(111L));

            failSnapshot.set(false);
            functionManage.sync();
            parent = domain.getColonyDO(111L);
            child = domain.getColonyDO(112L);
            assertNotNull(parent);
            assertSame(child, parent.getRuntimeColonyDOMap().get(112L));
            assertEquals(Long.valueOf(111L), child.getSuperiorId());
            assertTrue(child.getClusterDO().getRuntimeMap().containsKey(211L));
            verify(handler, times(2)).registerRuntime(argThat(value -> value.getId() == 211L), any(), any());
        }
    }

    private String formatSqlTime(LocalDateTime value) {
        return value.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    @Test
    public void incrementalQueriesIncludeExactBoundaryAndDoNotExposeDeletedInOrdinaryReads() throws Exception {
        seed("cluster", ClusterEntity.class, 104, Map.of("name", "deleted", "is_delete", 1));
        seed("runtime", RuntimeEntity.class, 204, Map.of("is_delete", 1));
        seed("cluster_relationship", ClusterRelationshipEntity.class, 304,
            Map.of("is_delete", 1, "relationship_type", ClusterType.EVENTMESH_RUNTIME.name()));
        ClusterEntity cluster = new ClusterEntity();
        cluster.setUpdateTime(BOUNDARY);
        RuntimeEntity runtime = new RuntimeEntity();
        runtime.setUpdateTime(BOUNDARY);
        ClusterRelationshipEntity relationship = new ClusterRelationshipEntity();
        relationship.setUpdateTime(BOUNDARY);
        assertEquals(1, session.getMapper(ClusterMapper.class).queryClusterByUpdate(cluster).size());
        assertEquals(1, session.getMapper(RuntimeMapper.class).queryByUpdateTime(runtime).size());
        assertEquals(1, session.getMapper(ClusterRelationshipMapper.class).queryNewlyIncreased(relationship).size());
        assertTrue(session.getMapper(ClusterMapper.class).queryAllCluster().isEmpty());
        assertTrue(session.getMapper(RuntimeMapper.class).queryAll().isEmpty());
        assertTrue(session.getMapper(ClusterRelationshipMapper.class).queryAll(relationship).isEmpty());
    }

    @Test
    public void incrementalQueriesIncludeStoppedAndUnlinkedRows() throws Exception {
        seed("cluster", ClusterEntity.class, 105, Map.of("status", 0));
        seed("runtime", RuntimeEntity.class, 205, Map.of("status", 0));
        seed("cluster_relationship", ClusterRelationshipEntity.class, 305,
            Map.of("status", 3, "relationship_type", ClusterType.EVENTMESH_RUNTIME.name()));
        ClusterEntity cluster = new ClusterEntity();
        cluster.setUpdateTime(BOUNDARY);
        RuntimeEntity runtime = new RuntimeEntity();
        runtime.setUpdateTime(BOUNDARY);
        ClusterRelationshipEntity relationship = new ClusterRelationshipEntity();
        relationship.setUpdateTime(BOUNDARY);

        assertEquals(1, session.getMapper(ClusterMapper.class).queryClusterByUpdate(cluster).size());
        assertEquals(1, session.getMapper(RuntimeMapper.class).queryByUpdateTime(runtime).size());
        assertEquals(1, session.getMapper(ClusterRelationshipMapper.class).queryNewlyIncreased(relationship).size());
    }

    void load(ClusterMetadataDomain domain, boolean includeClusters) {
        ClusterEntity cluster = new ClusterEntity();
        cluster.setUpdateTime(BOUNDARY);
        RuntimeEntity runtime = new RuntimeEntity();
        runtime.setUpdateTime(BOUNDARY);
        ClusterRelationshipEntity relationship = new ClusterRelationshipEntity();
        relationship.setUpdateTime(BOUNDARY);
        domain.handlerMetadata(MetadataAllDO.builder()
            .clusterEntityList(includeClusters ? session.getMapper(ClusterMapper.class).queryClusterByUpdate(cluster) : List.of())
            .runtimeEntityList(session.getMapper(RuntimeMapper.class).queryByUpdateTime(runtime))
            .clusterRelationshipEntityList(session.getMapper(ClusterRelationshipMapper.class).queryNewlyIncreased(relationship)).build());
    }
}
