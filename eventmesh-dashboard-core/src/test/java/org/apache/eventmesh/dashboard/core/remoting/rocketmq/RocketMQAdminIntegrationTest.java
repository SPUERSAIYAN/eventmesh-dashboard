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


package org.apache.eventmesh.dashboard.core.remoting.rocketmq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.eventmesh.dashboard.common.enums.ClusterType;
import org.apache.eventmesh.dashboard.common.model.base.BaseClusterIdBase;
import org.apache.eventmesh.dashboard.common.model.metadata.RuntimeMetadata;
import org.apache.eventmesh.dashboard.common.model.metadata.TopicMetadata;
import org.apache.eventmesh.dashboard.common.model.remoting.topic.CreateTopic2Request;
import org.apache.eventmesh.dashboard.common.model.remoting.topic.DeleteTopicRequest;
import org.apache.eventmesh.dashboard.common.model.remoting.topic.GetTopics2Request;
import org.apache.eventmesh.dashboard.core.function.SDK.ConfigManage;
import org.apache.eventmesh.dashboard.core.function.SDK.SDKManage;
import org.apache.eventmesh.dashboard.core.function.SDK.SDKTypeEnum;
import org.apache.eventmesh.dashboard.core.function.SDK.config.CreateRocketmqAdminSDKConfig;
import org.apache.eventmesh.dashboard.core.function.SDK.config.NetAddress;
import org.apache.eventmesh.dashboard.core.function.SDK.operation.rocketmq.RocketMQRemotingSDKOperation.DefaultRemotingClient;
import org.apache.eventmesh.dashboard.core.metadata.DataMetadataHandler;
import org.apache.eventmesh.dashboard.core.remoting.Remoting2Manage;
import org.apache.eventmesh.dashboard.service.remoting.TopicRemotingService;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.net.ServerSocket;
import java.util.Collections;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/** Real Broker tests: all clients and services are constructed through the production managers. */
@EnabledIfSystemProperty(named = "rocketmq.integration", matches = "true")
class RocketMQAdminIntegrationTest {

    private final SDKManage sdkManage = SDKManage.getInstance();

    @Test
    void topicLifecycleThroughManagedClientAndRemotingService() throws Exception {
        RuntimeMetadata runtime = runtime(92001L, 20911);
        CreateRocketmqAdminSDKConfig config = config(runtime);
        String topicName = "eventmesh_admin_it_" + UUID.randomUUID().toString().replace("-", "");
        try {
            DefaultMQAdminExt admin = sdkManage.createClient(SDKTypeEnum.ADMIN, runtime, config, runtime.getClusterType());
            assertSame(admin, sdkManage.getClient(SDKTypeEnum.ADMIN, runtime.getUnique()));
            TopicRemotingService service = Remoting2Manage.getInstance()
                .createRemotingService(TopicRemotingService.class, runtime);
            assertSame(admin, ((RocketMQTopicRemotingService) service).getAdminClient());
            DefaultRemotingClient ping = sdkManage.getClient(SDKTypeEnum.PING, runtime.getUnique());
            AbstractRocketMQRemotingService remoting = (AbstractRocketMQRemotingService) service;
            assertSame(ping, remoting.getClient());
            assertEquals(ResponseCode.SUCCESS, remoting.invokeSync(RemotingCommand.createRequestCommand(
                RequestCode.GET_BROKER_RUNTIME_INFO, null)).getCode());
            assertEquals(ResponseCode.SUCCESS, ping.invokeSync(RemotingCommand.createRequestCommand(
                RequestCode.GET_BROKER_RUNTIME_INFO, null), 3000).getCode());
            assertTrue(admin.examineBrokerClusterInfo().getClusterAddrTable().containsKey("EventMeshAdminTest"));

            TopicMetadata topic = new TopicMetadata();
            topic.setId(92003L);
            topic.setClusterId(runtime.getClusterId());
            topic.setClusterType(runtime.getClusterType());
            topic.setRuntimeId(runtime.getId());
            topic.setTopicName(topicName);
            topic.setReadQueueNum(2);
            topic.setWriteQueueNum(2);
            topic.setOrder(0);
            topic.setTopicFilterType("SINGLE_TAG");
            topic.setTopicConfig("{\"message.type\":\"NORMAL\",\"queue.type\":\"SimpleCQ\"}");
            CreateTopic2Request create = new CreateTopic2Request();
            create.setMetaData(topic);
            DeleteTopicRequest delete = new DeleteTopicRequest();
            delete.setMetaData(topic);
            try {
                assertEquals(200, service.createTopic(create).getCode());
                assertTopic(service, topicName, 2);
                topic.setTopicConfig("{\"+eventmesh.unsupported.attribute\":\"value\"}");
                var rejected = service.createTopic(create);
                assertNotEquals(200, rejected.getCode());
                assertNotNull(rejected.getMessage());
                TopicMetadata stored = service.getAllTopics(new GetTopics2Request()).getData().getTopicMetadataList()
                    .stream().filter(value -> topicName.equals(value.getTopicName())).findFirst().orElseThrow();
                assertTrue(stored.getTopicConfig().contains("SimpleCQ"));
                topic.setTopicConfig(stored.getTopicConfig());
                topic.setReadQueueNum(4);
                topic.setWriteQueueNum(4);
                assertEquals(200, service.createTopic(create).getCode());
                assertTopic(service, topicName, 4);

                DataMetadataHandler<BaseClusterIdBase> sync = Remoting2Manage.getInstance()
                    .createDataMetadataHandler(TopicRemotingService.class, runtime);
                assertTrue(sync.getData().stream().anyMatch(value ->
                    topicName.equals(((TopicMetadata) value).getTopicName())));
                topic.setReadQueueNum(3);
                topic.setWriteQueueNum(3);
                sync.handleAll(Collections.emptyList(), Collections.emptyList(),
                    Collections.singletonList(topic), Collections.emptyList());
                assertTopic(service, topicName, 3);

                assertEquals(200, service.deleteTopic(delete).getCode());
                assertFalse(service.getAllTopics(new GetTopics2Request()).getData().getTopicMetadataList()
                    .stream().anyMatch(value -> topicName.equals(value.getTopicName())));
            } finally {
                service.deleteTopic(delete);
            }
        } finally {
            sdkManage.deleteClient(null, runtime.getUnique());
        }
        assertNull(sdkManage.getClientWrapper(runtime.getUnique()));
        assertThrows(IllegalStateException.class, () -> sdkManage.getClient(SDKTypeEnum.ADMIN, runtime.getUnique()));
    }

    @Test
    void unavailableBrokerDoesNotReturnAnEmptySuccessfulList() throws Exception {
        // A listening socket that never speaks RocketMQ produces a deterministic RPC timeout.
        try (ServerSocket socket = new ServerSocket(0)) {
            RuntimeMetadata runtime = runtime(92002L, socket.getLocalPort());
            CreateRocketmqAdminSDKConfig config = config(runtime);
            config.setTimeoutMillis(300L);
            try {
                sdkManage.createClient(SDKTypeEnum.ADMIN, runtime, config, runtime.getClusterType());
                TopicRemotingService service = Remoting2Manage.getInstance()
                    .createRemotingService(TopicRemotingService.class, runtime);
                assertThrows(Exception.class, () -> service.getAllTopics(new GetTopics2Request()));
                DataMetadataHandler<BaseClusterIdBase> sync = Remoting2Manage.getInstance()
                    .createDataMetadataHandler(TopicRemotingService.class, runtime);
                assertThrows(IllegalStateException.class, sync::getData);
            } finally {
                sdkManage.deleteClient(null, runtime.getUnique());
            }
        }
    }

    @Test
    void closingOneManagedAdminDoesNotAffectAnother() throws Exception {
        RuntimeMetadata first = runtime(92004L, 20911);
        RuntimeMetadata second = runtime(92005L, 20911);
        try {
            DefaultMQAdminExt firstAdmin = sdkManage.createClient(SDKTypeEnum.ADMIN,
                first, config(first), first.getClusterType());
            DefaultMQAdminExt secondAdmin = sdkManage.createClient(SDKTypeEnum.ADMIN,
                second, config(second), second.getClusterType());
            assertNotEquals(firstAdmin.buildMQClientId(), secondAdmin.buildMQClientId());
            sdkManage.deleteClient(null, first.getUnique());
            TopicRemotingService service = Remoting2Manage.getInstance()
                .createRemotingService(TopicRemotingService.class, second);
            assertSame(secondAdmin, ((RocketMQTopicRemotingService) service).getAdminClient());
            assertEquals(200, service.getAllTopics(new GetTopics2Request()).getCode());
        } finally {
            sdkManage.deleteClient(null, first.getUnique());
            sdkManage.deleteClient(null, second.getUnique());
        }
    }

    private void assertTopic(TopicRemotingService service, String name, int queues) throws Exception {
        var result = service.getAllTopics(new GetTopics2Request());
        assertEquals(200, result.getCode());
        assertNotNull(result.getData());
        TopicMetadata topic = result.getData().getTopicMetadataList().stream()
            .filter(value -> name.equals(value.getTopicName())).findFirst().orElseThrow();
        assertEquals(queues, topic.getReadQueueNum());
        assertEquals(queues, topic.getWriteQueueNum());
        assertEquals("SINGLE_TAG", topic.getTopicFilterType());
    }

    private RuntimeMetadata runtime(long id, int port) {
        RuntimeMetadata runtime = new RuntimeMetadata();
        runtime.setId(id);
        runtime.setClusterId(92000L);
        runtime.setHost("127.0.0.1");
        runtime.setPort(port);
        runtime.setClusterType(ClusterType.STORAGE_ROCKETMQ_BROKER_MAIN_SLAVE);
        return runtime;
    }

    private CreateRocketmqAdminSDKConfig config(RuntimeMetadata runtime) {
        CreateRocketmqAdminSDKConfig config = (CreateRocketmqAdminSDKConfig) ConfigManage.getInstance()
            .getSimpleCreateSdkConfig(runtime.getClusterType(), SDKTypeEnum.ADMIN);
        config.setKey(runtime.getId().toString());
        config.setNetAddress(NetAddress.create(runtime.getHost(), runtime.getPort()));
        config.setNamesrvAddr("127.0.0.1:19876");
        return config;
    }
}
