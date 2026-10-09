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
import org.apache.eventmesh.dashboard.core.function.SDK.AbstractSDKOperation;
import org.apache.eventmesh.dashboard.core.function.SDK.SDKManage;
import org.apache.eventmesh.dashboard.core.function.SDK.SDKTypeEnum;
import org.apache.eventmesh.dashboard.core.function.SDK.config.CreateKakfaConfig;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StatusBehaviorSdkTest {

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void replacementClosesOldDistinctClientsAndKeepsNewRolesAndConfiguration() throws Exception {
        SDKManage manager = SDKManage.getInstance();
        ClusterType clusterType = ClusterType.STORAGE_KAFKA_BROKER;
        Map registry = (Map) ReflectionTestUtils.getField(SDKManage.class, "CLUSTER_TYPE_MAP_CONCURRENT_HASH_MAP");
        Map operations = (Map) registry.get(clusterType);
        Object adminWrapper = operations.get(SDKTypeEnum.ADMIN);
        Object pingWrapper = operations.get(SDKTypeEnum.PING);
        Object oldAdminOperation = ReflectionTestUtils.getField(adminWrapper, "abstractSDKOperation");
        Object oldPingOperation = ReflectionTestUtils.getField(pingWrapper, "abstractSDKOperation");
        AbstractSDKOperation operation = mock(AbstractSDKOperation.class);
        Object previousAdmin = new Object();
        Object previousPing = new Object();
        Object nextAdmin = new Object();
        Object nextPing = new Object();
        when(operation.createClient(any())).thenReturn(previousAdmin, previousPing, nextAdmin, nextPing);
        ClusterMetadata cluster = cluster(9611L);
        CreateKakfaConfig firstConfig = new CreateKakfaConfig();
        firstConfig.setKey("first-config");
        CreateKakfaConfig replacementConfig = new CreateKakfaConfig();
        replacementConfig.setKey("replacement-config");

        try {
            ReflectionTestUtils.setField(adminWrapper, "abstractSDKOperation", operation);
            ReflectionTestUtils.setField(pingWrapper, "abstractSDKOperation", operation);

            Object firstReturn = manager.createClient(SDKTypeEnum.ADMIN, cluster, firstConfig, clusterType);
            assertSame(previousPing, firstReturn);
            assertSame(previousAdmin, manager.getClient(SDKTypeEnum.ADMIN, cluster.getUnique()));
            assertSame(previousPing, manager.getClient(SDKTypeEnum.PING, cluster.getUnique()));

            Object replacementReturn = manager.createClient(SDKTypeEnum.ADMIN, cluster, replacementConfig, clusterType);

            assertSame(nextPing, replacementReturn);
            assertSame(nextAdmin, manager.getClient(SDKTypeEnum.ADMIN, cluster.getUnique()));
            assertSame(nextPing, manager.getClient(SDKTypeEnum.PING, cluster.getUnique()));
            assertSame(replacementConfig, manager.getClientWrapper(cluster.getUnique()).getConfig());
            verify(operation).close(previousAdmin);
            verify(operation).close(previousPing);

            manager.deleteClient(SDKTypeEnum.ADMIN, cluster.getUnique());
            assertNull(manager.getClientWrapper(cluster.getUnique()).getClientMap().get(SDKTypeEnum.ADMIN));
            assertSame(nextPing, manager.getClient(SDKTypeEnum.PING, cluster.getUnique()));
            verify(operation).close(nextAdmin);
            verify(operation, never()).close(nextPing);
        } finally {
            doNothing().when(operation).close(any());
            manager.deleteClient(null, cluster.getUnique());
            ReflectionTestUtils.setField(adminWrapper, "abstractSDKOperation", oldAdminOperation);
            ReflectionTestUtils.setField(pingWrapper, "abstractSDKOperation", oldPingOperation);
        }
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void deleteAggregatesDistinctCloseFailuresAndRetainsClientsForRetry() throws Exception {
        SDKManage manager = SDKManage.getInstance();
        ClusterType clusterType = ClusterType.STORAGE_KAFKA_BROKER;
        Map registry = (Map) ReflectionTestUtils.getField(SDKManage.class, "CLUSTER_TYPE_MAP_CONCURRENT_HASH_MAP");
        Map operations = (Map) registry.get(clusterType);
        Object adminWrapper = operations.get(SDKTypeEnum.ADMIN);
        Object pingWrapper = operations.get(SDKTypeEnum.PING);
        Object oldAdminOperation = ReflectionTestUtils.getField(adminWrapper, "abstractSDKOperation");
        Object oldPingOperation = ReflectionTestUtils.getField(pingWrapper, "abstractSDKOperation");
        AbstractSDKOperation operation = mock(AbstractSDKOperation.class);
        Object adminClient = new Object();
        Object pingClient = new Object();
        when(operation.createClient(any())).thenReturn(adminClient, pingClient);
        doThrow(new IllegalStateException("client close failed")).when(operation).close(any());
        ClusterMetadata cluster = cluster(9612L);

        try {
            ReflectionTestUtils.setField(adminWrapper, "abstractSDKOperation", operation);
            ReflectionTestUtils.setField(pingWrapper, "abstractSDKOperation", operation);
            manager.createClient(SDKTypeEnum.ADMIN, cluster, new CreateKakfaConfig(), clusterType);

            try {
                manager.deleteClient(null, cluster.getUnique());
                fail("Both failed closes must be reported");
            } catch (RuntimeException expected) {
                assertEquals("close client error", expected.getMessage());
                assertTrue(expected.getCause() instanceof IllegalStateException);
                assertEquals(1, expected.getSuppressed().length);
                assertTrue(expected.getSuppressed()[0] instanceof IllegalStateException);
            }
            verify(operation).close(adminClient);
            verify(operation).close(pingClient);
            assertSame(adminClient, manager.getClient(SDKTypeEnum.ADMIN, cluster.getUnique()));
            assertSame(pingClient, manager.getClient(SDKTypeEnum.PING, cluster.getUnique()));

            doNothing().when(operation).close(any());
            manager.deleteClient(null, cluster.getUnique());
            verify(operation, times(2)).close(adminClient);
            verify(operation, times(2)).close(pingClient);
            assertNull(manager.getClientWrapper(cluster.getUnique()));
        } finally {
            doNothing().when(operation).close(any());
            manager.deleteClient(null, cluster.getUnique());
            ReflectionTestUtils.setField(adminWrapper, "abstractSDKOperation", oldAdminOperation);
            ReflectionTestUtils.setField(pingWrapper, "abstractSDKOperation", oldPingOperation);
        }
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void failedReplacementClosesCreatedClientAndKeepsOldWrapper() throws Exception {
        SDKManage manager = SDKManage.getInstance();
        ClusterType clusterType = ClusterType.STORAGE_KAFKA_BROKER;
        Map registry = (Map) ReflectionTestUtils.getField(SDKManage.class, "CLUSTER_TYPE_MAP_CONCURRENT_HASH_MAP");
        Map operations = (Map) registry.get(clusterType);
        Object adminWrapper = operations.get(SDKTypeEnum.ADMIN);
        Object pingWrapper = operations.get(SDKTypeEnum.PING);
        Object oldAdminOperation = ReflectionTestUtils.getField(adminWrapper, "abstractSDKOperation");
        Object oldPingOperation = ReflectionTestUtils.getField(pingWrapper, "abstractSDKOperation");
        AbstractSDKOperation operation = mock(AbstractSDKOperation.class);
        Object oldAdmin = new Object();
        Object oldPing = new Object();
        Object partiallyCreated = new Object();
        when(operation.createClient(any())).thenReturn(oldAdmin, oldPing);
        ClusterMetadata cluster = cluster(9613L);
        CreateKakfaConfig oldConfig = new CreateKakfaConfig();
        CreateKakfaConfig attemptedConfig = new CreateKakfaConfig();

        try {
            ReflectionTestUtils.setField(adminWrapper, "abstractSDKOperation", operation);
            ReflectionTestUtils.setField(pingWrapper, "abstractSDKOperation", operation);
            manager.createClient(SDKTypeEnum.ADMIN, cluster, oldConfig, clusterType);
            Object originalWrapper = manager.getClientWrapper(cluster.getUnique());

            reset(operation);
            when(operation.createClient(any())).thenReturn(partiallyCreated)
                .thenThrow(new IllegalStateException("second role creation failed"));
            try {
                manager.createClient(SDKTypeEnum.ADMIN, cluster, attemptedConfig, clusterType);
                fail("Failed replacement must propagate the creation failure");
            } catch (RuntimeException expected) {
                assertEquals("create client error", expected.getMessage());
                assertEquals("second role creation failed", expected.getCause().getMessage());
            }

            verify(operation).close(partiallyCreated);
            verify(operation, never()).close(oldAdmin);
            verify(operation, never()).close(oldPing);
            assertSame(originalWrapper, manager.getClientWrapper(cluster.getUnique()));
            assertSame(oldConfig, manager.getClientWrapper(cluster.getUnique()).getConfig());
            assertSame(oldAdmin, manager.getClient(SDKTypeEnum.ADMIN, cluster.getUnique()));
            assertSame(oldPing, manager.getClient(SDKTypeEnum.PING, cluster.getUnique()));
        } finally {
            doNothing().when(operation).close(any());
            manager.deleteClient(null, cluster.getUnique());
            ReflectionTestUtils.setField(adminWrapper, "abstractSDKOperation", oldAdminOperation);
            ReflectionTestUtils.setField(pingWrapper, "abstractSDKOperation", oldPingOperation);
        }
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void cleanupFailureIsSuppressedOnOriginalCreationFailure() throws Exception {
        SDKManage manager = SDKManage.getInstance();
        ClusterType clusterType = ClusterType.STORAGE_KAFKA_BROKER;
        Map registry = (Map) ReflectionTestUtils.getField(SDKManage.class, "CLUSTER_TYPE_MAP_CONCURRENT_HASH_MAP");
        Map operations = (Map) registry.get(clusterType);
        Object adminWrapper = operations.get(SDKTypeEnum.ADMIN);
        Object pingWrapper = operations.get(SDKTypeEnum.PING);
        Object oldAdminOperation = ReflectionTestUtils.getField(adminWrapper, "abstractSDKOperation");
        Object oldPingOperation = ReflectionTestUtils.getField(pingWrapper, "abstractSDKOperation");
        AbstractSDKOperation operation = mock(AbstractSDKOperation.class);
        Object oldAdmin = new Object();
        Object oldPing = new Object();
        Object partiallyCreated = new Object();
        when(operation.createClient(any())).thenReturn(oldAdmin, oldPing);
        ClusterMetadata cluster = cluster(9614L);

        try {
            ReflectionTestUtils.setField(adminWrapper, "abstractSDKOperation", operation);
            ReflectionTestUtils.setField(pingWrapper, "abstractSDKOperation", operation);
            manager.createClient(SDKTypeEnum.ADMIN, cluster, new CreateKakfaConfig(), clusterType);

            reset(operation);
            IllegalStateException creationFailure = new IllegalStateException("creation remains primary");
            IllegalArgumentException closeFailure = new IllegalArgumentException("partial client close failed");
            when(operation.createClient(any())).thenReturn(partiallyCreated).thenThrow(creationFailure);
            doThrow(closeFailure).when(operation).close(partiallyCreated);
            try {
                manager.createClient(SDKTypeEnum.ADMIN, cluster, new CreateKakfaConfig(), clusterType);
                fail("Creation failure must propagate");
            } catch (RuntimeException expected) {
                assertEquals("create client error", expected.getMessage());
                assertSame(creationFailure, expected.getCause());
                assertEquals(1, expected.getCause().getSuppressed().length);
                Throwable suppressedCleanupFailure = expected.getCause().getSuppressed()[0];
                assertEquals("close client error", suppressedCleanupFailure.getMessage());
                assertNotNull(suppressedCleanupFailure.getCause());
                assertSame(closeFailure, suppressedCleanupFailure.getCause());
            }
            verify(operation).close(partiallyCreated);
            assertSame(oldAdmin, manager.getClient(SDKTypeEnum.ADMIN, cluster.getUnique()));
            assertSame(oldPing, manager.getClient(SDKTypeEnum.PING, cluster.getUnique()));
        } finally {
            doNothing().when(operation).close(any());
            manager.deleteClient(null, cluster.getUnique());
            ReflectionTestUtils.setField(adminWrapper, "abstractSDKOperation", oldAdminOperation);
            ReflectionTestUtils.setField(pingWrapper, "abstractSDKOperation", oldPingOperation);
        }
    }

    private ClusterMetadata cluster(long id) {
        ClusterMetadata cluster = new ClusterMetadata();
        cluster.setId(id);
        cluster.setClusterType(ClusterType.STORAGE_KAFKA_BROKER);
        return cluster;
    }
}
