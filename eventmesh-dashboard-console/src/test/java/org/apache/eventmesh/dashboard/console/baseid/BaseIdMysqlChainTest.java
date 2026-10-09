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
import org.apache.eventmesh.dashboard.common.enums.ClusterTrusteeshipType;
import org.apache.eventmesh.dashboard.console.entity.cluster.*;
import org.apache.eventmesh.dashboard.console.mapper.cluster.*;
import org.apache.eventmesh.dashboard.console.service.cluster.impl.*;
import org.apache.eventmesh.dashboard.console.spring.support.*;
import org.apache.eventmesh.dashboard.console.spring.support.metadata.DefaultMetadataSyncResultHandler;
import org.apache.eventmesh.dashboard.console.domain.metadata.ClusterMetadataDomain;
import org.apache.eventmesh.dashboard.console.function.health.Health2Service;
import org.apache.eventmesh.dashboard.core.metadata.MetadataSyncManage;
import org.apache.eventmesh.dashboard.core.function.SDK.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.*;

public class BaseIdMysqlChainTest extends BaseIdDatabaseTest {
    @Test
    public void realKafkaClientsFollowMysqlFunctionDomainAndFinalCapRemoval() throws Exception {
        org.junit.Assume.assumeTrue(Boolean.getBoolean("baseid.mysql"));
        seed("cluster", ClusterEntity.class, 9001, Map.of("cluster_type", ClusterType.STORAGE_KAFKA_BROKER.name(),
            "name", "baseid_mysql_kafka", "trusteeship_type", ClusterTrusteeshipType.NO_TRUSTEESHIP.name()));
        seed("runtime", RuntimeEntity.class, 9002, Map.of("cluster_id", 9001, "cluster_type", ClusterType.STORAGE_KAFKA_BROKER.name(),
            "host", 2130706433, "port", 19092, "kubernetes_cluster_id", 9002));
        RuntimeServiceImpl runtimes = new RuntimeServiceImpl();
        ReflectionTestUtils.setField(runtimes, "runtimeMapper", session.getMapper(RuntimeMapper.class));
        ClusterServiceImpl clusters = new ClusterServiceImpl();
        ReflectionTestUtils.setField(clusters, "clusterMapper", session.getMapper(ClusterMapper.class));
        ClusterRelationshipServiceImpl relationships = new ClusterRelationshipServiceImpl();
        ReflectionTestUtils.setField(relationships, "clusterRelationshipMapper", session.getMapper(ClusterRelationshipMapper.class));
        FunctionManage function = new FunctionManage();
        ReflectionTestUtils.setField(function, "enabled", true);
        ReflectionTestUtils.setField(function, "functionConfig", new FunctionConfig());
        ReflectionTestUtils.setField(function, "runtimeService", runtimes);
        ReflectionTestUtils.setField(function, "clusterService", clusters);
        ReflectionTestUtils.setField(function, "clusterRelationshipService", relationships);
        ReflectionTestUtils.invokeMethod(function, "initQueueData");
        ClusterMetadataDomain domain = function.registerBean();
        domain.rootClusterDHO();
        MetadataSyncManage metadata = new MetadataSyncManage();
        metadata.setMetadataSyncResultHandler(new DefaultMetadataSyncResultHandler());
        Health2Service health = new Health2Service();
        DefaultDataHandler handler = new DefaultDataHandler();
        handler.setHealthService(health);
        handler.setMetadataSyncManage(metadata);
        domain.setHandler(handler);
        List<AdminClient> clients = new ArrayList<>();
        String unique = null;
        try {
            function.sync();
            var colony = domain.getColonyDO(9001L);
            assertNotNull(colony);
            var cluster = colony.getClusterDO();
            // This checkout has no Kafka HealthCheckService adapter; registration is unsupported.
            assertFalse(((Map<?, ?>) ReflectionTestUtils.getField(Health2Service.class,
                "HEALTH_PING_CHECK_CLASS_CACHE")).containsKey(ClusterType.STORAGE_KAFKA_BROKER));
            unique = cluster.getClusterInfo().getUnique();
            assertArrayEquals(new String[]{"2130706433:19092"}, cluster.getMultiCreateSDKConfig().getNetAddresses());
            AdminClient admin = SDKManage.getInstance().getClient(SDKTypeEnum.ADMIN, unique);
            AdminClient ping = SDKManage.getInstance().getClient(SDKTypeEnum.PING, unique);
            clients.add(admin);
            clients.add(ping);
            assertFalse(admin.describeCluster().nodes().get(10, TimeUnit.SECONDS).isEmpty());
            assertFalse(ping.describeCluster().nodes().get(10, TimeUnit.SECONDS).isEmpty());
            RuntimeEntity replayCursor = (RuntimeEntity) ReflectionTestUtils.getField(function, "runtimeEntity");
            sql("update cluster set update_time='" + replayCursor.getUpdateTime() + "' where id=9001");
            sql("update runtime set update_time='" + replayCursor.getUpdateTime() + "' where id=9002");
            function.sync();
            AdminClient replacement = SDKManage.getInstance().getClient(SDKTypeEnum.ADMIN, unique);
            assertNotSame(admin, replacement);
            for (AdminClient old : clients) {
                try {
                    old.describeCluster().nodes().get(5, TimeUnit.SECONDS);
                    fail("Replaced Kafka client must be closed");
                } catch (java.util.concurrent.ExecutionException expected) {
                    assertNotNull(expected.getCause());
                }
            }
            clients.add(replacement);
            clients.add(SDKManage.getInstance().getClient(SDKTypeEnum.PING, unique));
            assertFalse(replacement.describeCluster().nodes().get(10, TimeUnit.SECONDS).isEmpty());
            RuntimeEntity cursor = (RuntimeEntity) ReflectionTestUtils.getField(function, "runtimeEntity");
            assertTrue(cursor.getUpdateTime().isAfter(BOUNDARY));
            RuntimeEntity runtime = new RuntimeEntity();
            runtime.setId(9002L);
            runtimes.deactivate(runtime);
            function.sync();
            assertTrue(cluster.getRuntimeMap().isEmpty());
            assertArrayEquals(new String[0], cluster.getMultiCreateSDKConfig().getNetAddresses());
            assertNull(SDKManage.getInstance().getClientWrapper(unique));
            assertTrue(((Map<?, ?>) ReflectionTestUtils.getField(health, "checkServiceMap")).isEmpty());
            for (AdminClient client : clients) {
                try {
                    client.describeCluster().nodes().get(5, TimeUnit.SECONDS);
                    fail("Unregistered Kafka client must be closed");
                } catch (java.util.concurrent.ExecutionException expected) {
                    assertNotNull(expected.getCause());
                }
            }
            function.sync();
            assertTrue(cluster.getRuntimeMap().isEmpty());
        } finally {
            if (unique != null) {
                SDKManage.getInstance().deleteClient(null, unique);
            }
            clients.forEach(client -> client.close(java.time.Duration.ofSeconds(2)));
        }
    }
}
