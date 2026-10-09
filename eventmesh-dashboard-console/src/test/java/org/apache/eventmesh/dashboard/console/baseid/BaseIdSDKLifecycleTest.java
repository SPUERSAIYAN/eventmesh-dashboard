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
import org.apache.eventmesh.dashboard.core.function.SDK.*;
import org.apache.eventmesh.dashboard.core.function.SDK.config.CreateKakfaConfig;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class BaseIdSDKLifecycleTest {
    @Test
    public void realHealthRegistrationRemovesOnlyItsCluster() {
        var health = new org.apache.eventmesh.dashboard.console.function.health.Health2Service();
        for (long id : List.of(9401L, 9402L)) {
            ClusterMetadata cluster = new ClusterMetadata();
            cluster.setId(id);
            cluster.setClusterType(ClusterType.STORAGE_ROCKETMQ_NAMESERVER);
            health.register(cluster);
        }
        Map<?, ?> registrations = (Map<?, ?>) ReflectionTestUtils.getField(health, "checkServiceMap");
        assertEquals(2, registrations.size());
        health.unRegisterCluster(9401L);
        assertEquals(1, registrations.size());
        health.unRegisterCluster(9401L);
        assertEquals(1, registrations.size());
        health.unRegisterCluster(9402L);
        assertTrue(registrations.isEmpty());
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void sharedClientClosesOnceAndFailureRemainsRetryable() throws Exception {
        SDKManage manager = SDKManage.getInstance();
        Map registry = (Map) ReflectionTestUtils.getField(SDKManage.class, "CLUSTER_TYPE_MAP_CONCURRENT_HASH_MAP");
        Map operations = (Map) registry.get(ClusterType.STORAGE_KAFKA_BROKER);
        Object adminWrapper = operations.get(SDKTypeEnum.ADMIN);
        Object pingWrapper = operations.get(SDKTypeEnum.PING);
        Object oldAdmin = ReflectionTestUtils.getField(adminWrapper, "abstractSDKOperation");
        Object oldPing = ReflectionTestUtils.getField(pingWrapper, "abstractSDKOperation");
        AbstractSDKOperation operation = mock(AbstractSDKOperation.class);
        Object client = new Object();
        when(operation.createClient(any())).thenReturn(client);
        ClusterMetadata cluster = new ClusterMetadata();
        cluster.setId(9301L);
        cluster.setClusterType(ClusterType.STORAGE_KAFKA_BROKER);
        try {
            ReflectionTestUtils.setField(adminWrapper, "abstractSDKOperation", operation);
            ReflectionTestUtils.setField(pingWrapper, "abstractSDKOperation", operation);
            manager.createClient(SDKTypeEnum.ADMIN, cluster, new CreateKakfaConfig(), cluster.getClusterType());
            doThrow(new IllegalStateException("close unavailable")).when(operation).close(client);
            try {
                manager.deleteClient(null, cluster.getUnique());
                fail("Close failure must propagate");
            } catch (RuntimeException expected) {
                assertEquals("close unavailable", expected.getCause().getMessage());
            }
            verify(operation, times(1)).close(client);
            assertNotNull(manager.getClientWrapper(cluster.getUnique()));
            clearInvocations(operation);
            doNothing().when(operation).close(client);
            manager.deleteClient(null, cluster.getUnique());
            verify(operation, times(1)).close(client);
            assertNull(manager.getClientWrapper(cluster.getUnique()));
            manager.deleteClient(null, cluster.getUnique());
            verify(operation, times(1)).close(client);
        } finally {
            doNothing().when(operation).close(client);
            manager.deleteClient(null, cluster.getUnique());
            ReflectionTestUtils.setField(adminWrapper, "abstractSDKOperation", oldAdmin);
            ReflectionTestUtils.setField(pingWrapper, "abstractSDKOperation", oldPing);
        }
    }
}
