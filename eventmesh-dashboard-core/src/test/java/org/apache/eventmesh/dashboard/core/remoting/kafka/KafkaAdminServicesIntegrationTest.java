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


package org.apache.eventmesh.dashboard.core.remoting.kafka;

import org.apache.eventmesh.dashboard.common.enums.ClusterType;
import org.apache.eventmesh.dashboard.common.enums.message.ResetOffsetMode;
import org.apache.eventmesh.dashboard.common.model.metadata.ClusterMetadata;
import org.apache.eventmesh.dashboard.common.model.metadata.GroupMetadata;
import org.apache.eventmesh.dashboard.common.model.remoting.group.DeleteGroupRequest;
import org.apache.eventmesh.dashboard.common.model.remoting.kafka.config.ConfigRequest;
import org.apache.eventmesh.dashboard.common.model.remoting.offset.GetOffsetRequest;
import org.apache.eventmesh.dashboard.common.model.remoting.offset.ResetOffsetRequest;
import org.apache.eventmesh.dashboard.core.function.SDK.ConfigManage;
import org.apache.eventmesh.dashboard.core.function.SDK.SDKManage;
import org.apache.eventmesh.dashboard.core.function.SDK.SDKTypeEnum;
import org.apache.eventmesh.dashboard.core.function.SDK.config.AbstractMultiCreateSDKConfig;
import org.apache.eventmesh.dashboard.core.function.SDK.config.NetAddress;
import org.apache.eventmesh.dashboard.core.remoting.Remoting2Manage;
import org.apache.eventmesh.dashboard.service.remoting.kafka.ConfigRemotingService;
import org.apache.eventmesh.dashboard.service.remoting.kafka.GroupRemotingService;
import org.apache.eventmesh.dashboard.service.remoting.kafka.OffsetRemotingService;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@EnabledIfSystemProperty(named = "kafka.integration", matches = "true")
class KafkaAdminServicesIntegrationTest {

    private ClusterMetadata cluster;
    private AdminClient client;
    private AdminClient ping;
    private GroupRemotingService groups;
    private OffsetRemotingService offsets;
    private ConfigRemotingService configs;
    private String topic;
    private String group;
    private String address;
    private long timestamp;

    @BeforeEach
    void setUp() throws Exception {
        cluster = new ClusterMetadata();
        cluster.setId(System.nanoTime());
        cluster.setClusterType(ClusterType.STORAGE_KAFKA_BROKER);
        AbstractMultiCreateSDKConfig config = ConfigManage.getInstance().getMultiCreateSdkConfig(cluster.getClusterType(), SDKTypeEnum.ADMIN);
        config.setKey(cluster.getId().toString());
        String host = System.getProperty("kafka.broker.host", "127.0.0.1");
        int port = Integer.getInteger("kafka.broker.port", 19092);
        address = host + ":" + port;
        config.addNetAddress(NetAddress.create(host, port));
        SDKManage manager = SDKManage.getInstance();
        manager.createClient(SDKTypeEnum.ADMIN, cluster, config, cluster.getClusterType());
        client = manager.getClient(SDKTypeEnum.ADMIN, cluster.getUnique());
        ping = manager.getClient(SDKTypeEnum.PING, cluster.getUnique());
        var registry = Remoting2Manage.getInstance();
        groups = registry.createRemotingService(GroupRemotingService.class, cluster);
        offsets = registry.createRemotingService(OffsetRemotingService.class, cluster);
        configs = registry.createRemotingService(ConfigRemotingService.class, cluster);
        topic = "dashboard-kafka-admin-" + UUID.randomUUID();
        group = "dashboard-kafka-group-" + UUID.randomUUID();
        client.createTopics(List.of(new NewTopic(topic, 2, (short) 1).configs(Map.of("retention.ms", "600000", "segment.ms", "300000"))))
            .all().get(10, TimeUnit.SECONDS);
        timestamp = System.currentTimeMillis() - 10000;
        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", address);
        producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig)) {
            for (int partition = 0; partition < 2; partition++) {
                for (int index = 0; index < 5; index++) {
                    producer.send(new ProducerRecord<>(topic, partition, timestamp + index * 1000, "key", "value"))
                        .get(10, TimeUnit.SECONDS);
                }
            }
        }
        try (KafkaConsumer<String, String> consumer = consumer()) {
            consumer.assign(List.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1)));
            consumer.commitSync(Map.of(new TopicPartition(topic, 0), new OffsetAndMetadata(2),
                new TopicPartition(topic, 1), new OffsetAndMetadata(3)), Duration.ofSeconds(10));
        }
        awaitEmpty();
    }

    @Test
    void registeredGroupQueryAndReflectiveDelete() throws Exception {
        log.info("【消费组真实验证】查询组={}、成员数，再经反射 DELETE 删除", group);
        var found = groups.getAllGroups(null).getData().stream().filter(row -> group.equals(row.getName())).findFirst().orElseThrow();
        Assertions.assertEquals(0, found.getMemberCount());
        var handler = Remoting2Manage.getInstance().createDataMetadataHandler(GroupRemotingService.class, cluster);
        Assertions.assertTrue(handler.getData().stream().map(GroupMetadata.class::cast).anyMatch(row -> group.equals(row.getName())));
        GroupMetadata metadata = new GroupMetadata();
        metadata.setId(1L);
        metadata.setName(group);
        handler.handleAll(List.of(), List.of(), List.of(), List.of(metadata));
        Assertions.assertFalse(client.listConsumerGroups().all().get(10, TimeUnit.SECONDS).stream().anyMatch(row -> group.equals(row.groupId())));
    }

    @Test
    void offsetsQueryAndAllResetModesPreserveOtherPartition() throws Exception {
        log.info("【Offset 真实验证】组={}，主题={}，分区0=2，分区1=3，日志末尾=5", group, topic);
        GetOffsetRequest query = new GetOffsetRequest();
        query.setGroupName(group);
        query.setTopic(topic);
        var rows = offsets.getOffsets(query).getData();
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals(2L, rows.get(0).getOffset());
        Assertions.assertEquals(5L, rows.get(0).getBrokerOffset());
        ResetOffsetRequest request = reset();
        for (ResetOffsetMode mode : List.of(ResetOffsetMode.CONSUME_FROM_FIRST_OFFSET, ResetOffsetMode.CONSUME_FROM_LAST_OFFSET,
            ResetOffsetMode.CONSUME_FROM_TIMESTAMP, ResetOffsetMode.CONSUME_FROM_DESIGNATED_OFFSET)) {
            request.setResetOffsetMode(mode);
            request.setTimestamp(mode == ResetOffsetMode.CONSUME_FROM_TIMESTAMP ? timestamp + 2000 : null);
            request.setOffset(mode == ResetOffsetMode.CONSUME_FROM_DESIGNATED_OFFSET ? 1L : null);
            Assertions.assertEquals(200, offsets.resetOffsets(request).getCode());
            long expected = mode == ResetOffsetMode.CONSUME_FROM_FIRST_OFFSET ? 0 : mode == ResetOffsetMode.CONSUME_FROM_LAST_OFFSET ? 5
                : mode == ResetOffsetMode.CONSUME_FROM_TIMESTAMP ? 2 : 1;
            assertOffsets(expected, 3);
            log.info("【回读通过】模式={}，分区0={}，分区1保持3", mode, expected);
        }
        request.setPartitionId(null);
        request.setOffset(4L);
        Assertions.assertEquals(200, offsets.resetOffsets(request).getCode());
        assertOffsets(4, 4);
        request.setResetOffsetMode(ResetOffsetMode.CONSUME_FROM_TIMESTAMP);
        request.setOffset(null);
        request.setTimestamp(System.currentTimeMillis() + 60000);
        Assertions.assertThrows(IllegalArgumentException.class, () -> offsets.resetOffsets(request));
        assertOffsets(4, 4);
    }

    @Test
    void activeGroupRejectsResetAndDeletion() throws Exception {
        log.info("【活跃组真实验证】保持 consumer 在线，验证重置和删除被拒绝");
        try (KafkaConsumer<String, String> consumer = consumer()) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (consumer.assignment().isEmpty() && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(200));
            }
            Assertions.assertFalse(consumer.assignment().isEmpty());
            Assertions.assertThrows(IllegalStateException.class, () -> offsets.resetOffsets(reset()));
            DeleteGroupRequest delete = new DeleteGroupRequest();
            GroupMetadata metadata = new GroupMetadata();
            metadata.setName(group);
            delete.setMetaData(metadata);
            Assertions.assertInstanceOf(GroupNotEmptyException.class,
                Assertions.assertThrows(ExecutionException.class, () -> groups.deleteGroup(delete)).getCause());
            assertOffsets(2, 3);
        }
        awaitEmpty();
    }

    @Test
    void configScopesQueryAndIncrementalUpdates() throws Exception {
        ConfigRequest request = new ConfigRequest();
        request.setScope(ConfigRequest.Scope.TOPIC);
        request.setResourceName(topic);
        request.setConfigs(Map.of("retention.ms", "900000"));
        Assertions.assertEquals(200, configs.updateConfigs(request).getCode());
        var rows = configs.getConfigs(request).getData();
        Assertions.assertEquals("900000", rows.stream().filter(row -> row.getName().equals("retention.ms")).findFirst().orElseThrow().getValue());
        Assertions.assertEquals("300000", rows.stream().filter(row -> row.getName().equals("segment.ms")).findFirst().orElseThrow().getValue());
        request.setConfigs(Map.of("retention.ms", "invalid"));
        Assertions.assertThrows(ExecutionException.class, () -> configs.updateConfigs(request));
        int brokerId = client.describeCluster().nodes().get(10, TimeUnit.SECONDS).iterator().next().id();
        for (ConfigRequest.Scope scope : List.of(ConfigRequest.Scope.BROKER, ConfigRequest.Scope.DEFAULT_BROKER)) {
            request.setScope(scope);
            request.setResourceName(scope == ConfigRequest.Scope.BROKER ? Integer.toString(brokerId) : null);
            var resource = new ConfigResource(ConfigResource.Type.BROKER, scope == ConfigRequest.Scope.BROKER ? Integer.toString(brokerId) : "");
            ConfigEntry original = client.describeConfigs(List.of(resource)).all().get(10, TimeUnit.SECONDS).get(resource).get("log.retention.ms");
            ConfigEntry.ConfigSource ownSource = scope == ConfigRequest.Scope.BROKER ? ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG
                : ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG;
            try {
                request.setConfigs(Map.of("log.retention.ms", "604800123"));
                Assertions.assertEquals(200, configs.updateConfigs(request).getCode());
                boolean visible = false;
                for (int attempt = 0; attempt < 50; attempt++) {
                    var current = configs.getConfigs(request).getData().stream().filter(row -> row.getName().equals("log.retention.ms"))
                        .findFirst().orElse(null);
                    if (current != null && "604800123".equals(current.getValue()) && ownSource.name().equals(current.getSource())) {
                        visible = true;
                        break;
                    }
                    Thread.sleep(100);
                }
                Assertions.assertTrue(visible, "Dynamic configuration did not become visible");
                log.info("【配置真实验证】scope={}，目标={}，增量修改并回读成功", scope, request.getResourceName());
            } finally {
                boolean hadOverride = original != null && original.source() == ownSource;
                client.incrementalAlterConfigs(Map.of(resource, List.of(new AlterConfigOp(
                    new ConfigEntry("log.retention.ms", hadOverride ? original.value() : null),
                    hadOverride ? AlterConfigOp.OpType.SET : AlterConfigOp.OpType.DELETE)))).all().get(10, TimeUnit.SECONDS);
            }
        }
    }

    private KafkaConsumer<String, String> consumer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", address);
        properties.put("group.id", group);
        properties.put("enable.auto.commit", "false");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<>(properties);
    }

    private ResetOffsetRequest reset() {
        ResetOffsetRequest request = new ResetOffsetRequest();
        request.setGroupName(group);
        request.setTopic(topic);
        request.setPartitionId(0);
        request.setResetOffsetMode(ResetOffsetMode.CONSUME_FROM_DESIGNATED_OFFSET);
        request.setOffset(1L);
        return request;
    }

    private void assertOffsets(long first, long second) throws Exception {
        var result = client.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
        Assertions.assertEquals(first, result.get(new TopicPartition(topic, 0)).offset());
        Assertions.assertEquals(second, result.get(new TopicPartition(topic, 1)).offset());
    }

    private void awaitEmpty() throws Exception {
        for (int attempt = 0; attempt < 30; attempt++) {
            if (client.describeConsumerGroups(List.of(group)).all().get(10, TimeUnit.SECONDS).get(group).state() == ConsumerGroupState.EMPTY) {
                return;
            }
            Thread.sleep(100);
        }
        Assertions.fail("Consumer group did not become EMPTY");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (client == null) {
            return;
        }
        List<Exception> errors = new ArrayList<>();
        try {
            if (group != null) {
                try {
                    client.deleteConsumerGroups(List.of(group)).all().get(10, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    if (!(e.getCause() instanceof GroupIdNotFoundException)) {
                        errors.add(e);
                    }
                }
            }
            if (topic != null) {
                client.deleteTopics(List.of(topic)).all().get(10, TimeUnit.SECONDS);
            }
        } finally {
            client.close(Duration.ofSeconds(5));
            ping.close(Duration.ofSeconds(5));
            SDKManage.getInstance().deleteClient(null, cluster.getUnique());
        }
        if (!errors.isEmpty()) {
            throw errors.get(0);
        }
    }
}
