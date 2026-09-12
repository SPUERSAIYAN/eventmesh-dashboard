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


package org.apache.eventmesh.dashboard.core.function.SDK;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.eventmesh.dashboard.common.enums.ClusterType;
import org.apache.eventmesh.dashboard.common.enums.RemotingType;
import org.apache.eventmesh.dashboard.common.model.metadata.RuntimeMetadata;
import org.apache.eventmesh.dashboard.core.function.SDK.config.CreateRemotingConfig;
import org.apache.eventmesh.dashboard.core.function.SDK.operation.rocketmq.RocketMQAdminOperation;
import org.apache.eventmesh.dashboard.core.function.SDK.operation.rocketmq.RocketMQRemotingSDKOperation;
import org.apache.eventmesh.dashboard.core.remoting.AbstractRemotingService;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Verifies client ownership, rollback, replacement, and distinct ADMIN/PING registration. */
class SDKManageLifecycleTest {

    private static final List<TestClient> CREATED = new ArrayList<>();
    private static boolean failPing;
    private final SDKManage manage = SDKManage.getInstance();
    private final RuntimeMetadata runtime = new RuntimeMetadata();

    @BeforeEach
    void setUp() {
        runtime.setId(93001L);
        runtime.setClusterType(ClusterType.STORAGE_ROCKETMQ_BROKER_MAIN_SLAVE);
        CREATED.clear();
        failPing = false;
        SDKManage.createSDKMetadataWrapper(AdminOperation.class);
        SDKManage.createSDKMetadataWrapper(PingOperation.class);
    }

    @AfterEach
    void tearDown() {
        manage.deleteClient(null, runtime.getUnique());
        SDKManage.createSDKMetadataWrapper(RocketMQAdminOperation.class);
        SDKManage.createSDKMetadataWrapper(RocketMQRemotingSDKOperation.class);
    }

    @Test
    void createsDistinctTypesAndClosesBothExactlyOnce() {
        TestClient admin = create();
        TestClient ping = manage.getClient(SDKTypeEnum.PING, runtime.getUnique());
        assertSame(admin, manage.getClient(SDKTypeEnum.ADMIN, runtime.getUnique()));
        assertNotSame(admin, ping);
        assertEquals(2, CREATED.size());
        manage.deleteClient(null, runtime.getUnique());
        manage.deleteClient(null, runtime.getUnique());
        assertEquals(1, admin.closeCount);
        assertEquals(1, ping.closeCount);
    }

    @Test
    void sharedClientClosesOnlyAfterLastAliasIsRemoved() {
        SDKManage.createSDKMetadataWrapper(SharedOperation.class);
        TestClient admin = create();
        assertSame(admin, manage.getClient(SDKTypeEnum.PING, runtime.getUnique()));
        assertEquals(1, CREATED.size());
        manage.deleteClient(SDKTypeEnum.ADMIN, runtime.getUnique());
        assertEquals(0, admin.closeCount);
        manage.deleteClient(SDKTypeEnum.PING, runtime.getUnique());
        assertEquals(1, admin.closeCount);
    }

    @Test
    void replacementClosesOldClientsAndInvalidatesCachedService() {
        TestClient first = create();
        TestService oldService = manage.createAbstractClientInfo(TestService.class, runtime);
        TestClient second = create();
        TestService newService = manage.createAbstractClientInfo(TestService.class, runtime);
        assertEquals(1, first.closeCount);
        assertNotSame(oldService, newService);
        assertSame(second, newService.getClient());
    }

    @Test
    void failedPingCreationRollsBackNewAdminAndKeepsPreviousClient() {
        TestClient first = create();
        failPing = true;
        assertThrows(IllegalStateException.class, this::create);
        assertSame(first, manage.getClient(SDKTypeEnum.ADMIN, runtime.getUnique()));
        assertEquals(0, first.closeCount);
        assertEquals(1, CREATED.get(2).closeCount);
    }

    @Test
    void replacingPingDoesNotCloseOrReplaceAdmin() {
        TestClient admin = create();
        TestClient oldPing = manage.getClient(SDKTypeEnum.PING, runtime.getUnique());
        TestClient ping = manage.createClient(SDKTypeEnum.PING, runtime,
            new CreateRemotingConfig(), runtime.getClusterType());
        assertSame(admin, manage.getClient(SDKTypeEnum.ADMIN, runtime.getUnique()));
        assertSame(ping, manage.getClient(SDKTypeEnum.PING, runtime.getUnique()));
        assertEquals(0, admin.closeCount);
        assertEquals(1, oldPing.closeCount);
    }

    private TestClient create() {
        return manage.createClient(SDKTypeEnum.ADMIN, runtime, new CreateRemotingConfig(), runtime.getClusterType());
    }

    /** Instrumented client used to count resource release. */
    public static class TestClient {
        private int closeCount;
    }

    /** Cached service whose injected client must change on replacement. */
    public static class TestService extends AbstractRemotingService<TestClient> {
    }

    /** Test-only ADMIN provider. */
    @SDKMetadata(clusterType = {ClusterType.STORAGE_ROCKETMQ_BROKER_MAIN_SLAVE},
        remotingType = RemotingType.ROCKETMQ, sdkTypeEnum = {SDKTypeEnum.ADMIN})
    public static class AdminOperation extends AbstractSDKOperation<TestClient, CreateRemotingConfig> {
        @Override
        public TestClient createClient(CreateRemotingConfig config) {
            TestClient client = new TestClient();
            CREATED.add(client);
            return client;
        }

        @Override
        public void close(TestClient client) {
            client.closeCount++;
        }
    }

    /** Test-only PING provider with an injectable startup failure. */
    @SDKMetadata(clusterType = {ClusterType.STORAGE_ROCKETMQ_BROKER_MAIN_SLAVE},
        remotingType = RemotingType.ROCKETMQ, sdkTypeEnum = {SDKTypeEnum.PING})
    public static class PingOperation extends AbstractSDKOperation<TestClient, CreateRemotingConfig> {
        @Override
        public TestClient createClient(CreateRemotingConfig config) {
            if (failPing) {
                throw new IllegalStateException("Simulated PING startup failure");
            }
            TestClient client = new TestClient();
            CREATED.add(client);
            return client;
        }

        @Override
        public void close(TestClient client) {
            client.closeCount++;
        }
    }

    /** Test-only provider exposing a shared ADMIN/PING client. */
    @SDKMetadata(clusterType = {ClusterType.STORAGE_ROCKETMQ_BROKER_MAIN_SLAVE},
        remotingType = RemotingType.ROCKETMQ, sdkTypeEnum = {SDKTypeEnum.ADMIN, SDKTypeEnum.PING})
    public static class SharedOperation extends AbstractSDKOperation<TestClient, CreateRemotingConfig> {
        @Override
        public TestClient createClient(CreateRemotingConfig config) {
            TestClient client = new TestClient();
            CREATED.add(client);
            return client;
        }

        @Override
        public void close(TestClient client) {
            client.closeCount++;
        }
    }
}
