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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.eventmesh.dashboard.common.enums.ClusterType;
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
import org.apache.eventmesh.dashboard.core.remoting.Remoting2Manage;
import org.apache.eventmesh.dashboard.service.remoting.TopicRemotingService;

import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import lombok.extern.slf4j.Slf4j;

/**
 * Manual Docker CRUD examples for IDEA. Run createTopic first, then queryTopic/updateTopic, then deleteTopic.
 * Topic data survives individual runs; only deleteTopic removes it. Running the class executes all four steps.
 * Explicit opt-in keeps these stateful examples out of ordinary automated regression runs.
 */
@Slf4j
@TestMethodOrder(OrderAnnotation.class)
@EnabledIfSystemProperty(named = "rocketmq.topic.crud", matches = "true")
class RocketMQAdminTopicCrudTest {

    private static final String TOPIC_NAME = "eventmesh_admin_manual_topic";

    private final SDKManage sdkManage = SDKManage.getInstance();
    private RuntimeMetadata runtime;
    private TopicRemotingService service;

    @BeforeEach
    void createManagedClient() throws Exception {
        runtime = new RuntimeMetadata();
        runtime.setId(93001L);
        runtime.setClusterId(93000L);
        runtime.setHost("127.0.0.1");
        runtime.setPort(20911);
        runtime.setClusterType(ClusterType.STORAGE_ROCKETMQ_BROKER_MAIN_SLAVE);
        CreateRocketmqAdminSDKConfig config = (CreateRocketmqAdminSDKConfig) ConfigManage.getInstance()
            .getSimpleCreateSdkConfig(runtime.getClusterType(), SDKTypeEnum.ADMIN);
        config.setKey(runtime.getId().toString());
        config.setNetAddress(NetAddress.create(runtime.getHost(), runtime.getPort()));
        config.setNamesrvAddr("127.0.0.1:19876");
        DefaultMQAdminExt admin = sdkManage.createClient(SDKTypeEnum.ADMIN, runtime, config, runtime.getClusterType());
        service = Remoting2Manage.getInstance().createRemotingService(TopicRemotingService.class, runtime);
        assertSame(admin, sdkManage.getClient(SDKTypeEnum.ADMIN, runtime.getUnique()));
        assertSame(admin, ((RocketMQTopicRemotingService) service).getAdminClient());
    }

    @AfterEach
    void closeManagedClient() {
        // Close ADMIN and PING connections, but retain the Topic for the next manual operation.
        if (runtime != null) {
            sdkManage.deleteClient(null, runtime.getUnique());
        }
    }

    @Test
    @Order(1)
    void createTopic() throws Exception {
        assertFalse(topics().stream().anyMatch(topic -> TOPIC_NAME.equals(topic.getTopicName())),
            "Topic already exists; use queryTopic/updateTopic, or run deleteTopic before recreating it");
        TopicMetadata topic = new TopicMetadata();
        topic.setId(93003L);
        topic.setClusterId(runtime.getClusterId());
        topic.setClusterType(runtime.getClusterType());
        topic.setRuntimeId(runtime.getId());
        topic.setTopicName(TOPIC_NAME);
        topic.setReadQueueNum(2);
        topic.setWriteQueueNum(2);
        topic.setOrder(0);
        topic.setTopicFilterType("SINGLE_TAG");
        topic.setTopicConfig("{\"message.type\":\"NORMAL\",\"queue.type\":\"SimpleCQ\"}");
        CreateTopic2Request request = new CreateTopic2Request();
        request.setMetaData(topic);

        var result = service.createTopic(request);

        assertEquals(200, result.getCode(), result.getMessage());
        assertQueues(currentTopic(), 2);
        log.info("Created Topic {}; retained for queryTopic/updateTopic/deleteTopic", TOPIC_NAME);
    }

    @Test
    @Order(2)
    void queryTopic() throws Exception {
        TopicMetadata topic = currentTopic();

        assertEquals(TOPIC_NAME, topic.getTopicName());
        assertNotNull(topic.getReadQueueNum());
        assertNotNull(topic.getWriteQueueNum());
        assertEquals("SINGLE_TAG", topic.getTopicFilterType());
        log.info("Topic {}: readQueues={}, writeQueues={}, config={}",
            topic.getTopicName(), topic.getReadQueueNum(), topic.getWriteQueueNum(), topic.getTopicConfig());
    }

    @Test
    @Order(3)
    void updateTopic() throws Exception {
        TopicMetadata topic = currentTopic();
        topic.setReadQueueNum(4);
        topic.setWriteQueueNum(4);
        CreateTopic2Request request = new CreateTopic2Request();
        request.setMetaData(topic);

        // The existing service uses createTopic for both creation and configuration updates.
        var result = service.createTopic(request);

        assertEquals(200, result.getCode(), result.getMessage());
        assertQueues(currentTopic(), 4);
        log.info("Updated Topic {}: readQueues=4, writeQueues=4; Topic retained", TOPIC_NAME);
    }

    @Test
    @Order(4)
    void deleteTopic() throws Exception {
        DeleteTopicRequest request = new DeleteTopicRequest();
        request.setMetaData(currentTopic());

        var result = service.deleteTopic(request);

        assertEquals(200, result.getCode(), result.getMessage());
        assertFalse(topics().stream().anyMatch(topic -> TOPIC_NAME.equals(topic.getTopicName())));
        log.info("Deleted Topic {} from Broker 127.0.0.1:20911", TOPIC_NAME);
    }

    private List<TopicMetadata> topics() throws Exception {
        var result = service.getAllTopics(new GetTopics2Request());
        assertEquals(200, result.getCode(), result.getMessage());
        assertNotNull(result.getData());
        assertNotNull(result.getData().getTopicMetadataList());
        return result.getData().getTopicMetadataList();
    }

    private TopicMetadata currentTopic() throws Exception {
        return topics().stream().filter(topic -> TOPIC_NAME.equals(topic.getTopicName())).findFirst()
            .orElseThrow(() -> new AssertionError("Topic " + TOPIC_NAME + " is absent; run createTopic first"));
    }

    private void assertQueues(TopicMetadata topic, int queues) {
        assertEquals(queues, topic.getReadQueueNum());
        assertEquals(queues, topic.getWriteQueueNum());
    }
}
