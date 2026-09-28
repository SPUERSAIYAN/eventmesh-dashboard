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


package org.apache.eventmesh.dashboard.core.remoting.kafka.mock;

import org.apache.eventmesh.dashboard.common.enums.message.ResetOffsetMode;
import org.apache.eventmesh.dashboard.common.model.remoting.kafka.config.ConfigRequest;
import org.apache.eventmesh.dashboard.common.model.remoting.kafka.offset.ResetOffsetsResult.Status;
import org.apache.eventmesh.dashboard.common.model.remoting.offset.GetOffsetRequest;
import org.apache.eventmesh.dashboard.common.model.remoting.offset.ResetOffsetRequest;
import org.apache.eventmesh.dashboard.core.function.SDK.ClientWrapper;
import org.apache.eventmesh.dashboard.core.function.SDK.SDKTypeEnum;
import org.apache.eventmesh.dashboard.core.remoting.kafka.KafkaConfigRemotingService;
import org.apache.eventmesh.dashboard.core.remoting.kafka.KafkaGroupRemotingService;
import org.apache.eventmesh.dashboard.core.remoting.kafka.KafkaOffsetRemotingService;
import org.apache.eventmesh.dashboard.core.remoting.kafka.KafkaTestLog;


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@ExtendWith(KafkaTestLog.class)
class KafkaAdminServicesTest {

    private AdminClient client;
    private KafkaGroupRemotingService groups;
    private KafkaOffsetRemotingService offsets;
    private KafkaConfigRemotingService configs;
    private final TopicPartition partition = new TopicPartition("topic-a", 0);

    @BeforeEach
    void setUp() {
        client = Mockito.mock(AdminClient.class, Mockito.RETURNS_DEEP_STUBS);
        ClientWrapper wrapper = new ClientWrapper();
        wrapper.getClientMap().put(SDKTypeEnum.ADMIN, client);
        groups = new KafkaGroupRemotingService();
        groups.setClientWrapper(wrapper);
        offsets = new KafkaOffsetRemotingService();
        offsets.setClientWrapper(wrapper);
        configs = new KafkaConfigRemotingService();
        configs.setClientWrapper(wrapper);
    }

    /** 消费组完整描述转换为名称和成员数。 */
    @Test
    @DisplayName("模拟响应：消费组完整描述转换为名称和成员数")
    void groupQueryConvertsCompleteDescriptions() throws Exception {
        Mockito.when(client.listConsumerGroups(Mockito.any()).all()).thenReturn(KafkaFuture.completedFuture(List.of(
            new ConsumerGroupListing("group-a", false))));
        Mockito.when(client.describeConsumerGroups(Mockito.anyCollection(), Mockito.any()).all())
            .thenReturn(KafkaFuture.completedFuture(Map.of("group-a", group(ConsumerGroupState.EMPTY))));
        var result = groups.getAllGroups(null);
        Assertions.assertEquals(200, result.getCode());
        Assertions.assertEquals("group-a", result.getData().get(0).getName());
        Assertions.assertEquals(0, result.getData().get(0).getMemberCount());
        Assertions.assertNull(result.getData().get(0).getRetryQueueNums());
    }

    /** 消费组查询失败不能返回部分成功。 */
    @Test
    @DisplayName("模拟响应：消费组查询失败不能返回部分成功")
    void groupQueryDoesNotReturnPartialListOnFailure() {
        Mockito.when(client.listConsumerGroups(Mockito.any()).all()).thenReturn(failed(new GroupAuthorizationException("group-a")));
        Assertions.assertThrows(ExecutionException.class, () -> groups.getAllGroups(null));
    }

    /** 消费组描述缺失时明确失败。 */
    @Test
    @DisplayName("模拟响应：消费组描述缺失时明确失败")
    void groupQueryRejectsMissingDescription() {
        Mockito.when(client.listConsumerGroups(Mockito.any()).all()).thenReturn(KafkaFuture.completedFuture(List.of(
            new ConsumerGroupListing("group-a", false))));
        Mockito.when(client.describeConsumerGroups(Mockito.anyCollection(), Mockito.any()).all()).thenReturn(KafkaFuture.completedFuture(Map.of()));
        Assertions.assertThrows(IllegalStateException.class, () -> groups.getAllGroups(null));
    }

    /** 删除消费组必须指定名称。 */
    @Test
    @DisplayName("模拟响应：删除消费组必须指定名称")
    void groupDeletionRequiresExplicitName() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> groups.deleteGroup(null));
        Mockito.verifyNoInteractions(client);
    }

    /** 未提交位点保持空且不伪造消息时间戳。 */
    @Test
    @DisplayName("模拟响应：未提交位点保持空且不伪造消息时间戳")
    void offsetQueryPreservesUncommittedAndDoesNotInventTimestamp() throws Exception {
        topic();
        Mockito.when(client.listConsumerGroupOffsets(Mockito.anyString(), Mockito.any()).partitionsToOffsetAndMetadata())
            .thenReturn(KafkaFuture.completedFuture(Map.of()));
        logOffsets();
        GetOffsetRequest request = new GetOffsetRequest();
        request.setGroupName("group-a");
        request.setTopic("topic-a");
        var row = offsets.getOffsets(request).getData().get(0);
        Assertions.assertNull(row.getOffset());
        Assertions.assertEquals(10L, row.getBrokerOffset());
        Assertions.assertNull(row.getLastTimestamp());
        Assertions.assertNull(row.getBrokerName());
    }

    /** 没有提交位点时不扩大为全 Topic 查询。 */
    @Test
    @DisplayName("模拟响应：没有提交位点时不扩大为全 Topic 查询")
    void emptyCommittedOffsetsDoNotQueryAllTopics() throws Exception {
        Mockito.when(client.listConsumerGroupOffsets(Mockito.anyString(), Mockito.any()).partitionsToOffsetAndMetadata())
            .thenReturn(KafkaFuture.completedFuture(Map.of()));
        GetOffsetRequest request = new GetOffsetRequest();
        request.setGroupName("group-a");
        Assertions.assertTrue(offsets.getOffsets(request).getData().isEmpty());
        Mockito.verify(client, Mockito.never()).describeTopics(Mockito.anyCollection(), Mockito.any());
        Mockito.verify(client, Mockito.never()).listOffsets(Mockito.anyMap(), Mockito.any());
    }

    /** 位点查询保留原始失败。 */
    @Test
    @DisplayName("模拟响应：位点查询保留原始失败")
    void queryDoesNotHideOffsetFailure() {
        Mockito.when(client.listConsumerGroupOffsets(Mockito.anyString(), Mockito.any()).partitionsToOffsetAndMetadata())
            .thenReturn(failed(new GroupAuthorizationException("group-a")));
        GetOffsetRequest request = new GetOffsetRequest();
        request.setGroupName("group-a");
        Assertions.assertThrows(ExecutionException.class, () -> offsets.getOffsets(request));
    }

    /** 操作参数不能覆盖托管客户端地址。 */
    @Test
    @DisplayName("模拟响应：操作参数不能覆盖托管客户端地址")
    void operationCannotOverrideManagedAddress() {
        GetOffsetRequest request = new GetOffsetRequest();
        request.setGroupName("group-a");
        request.setBootstrapServers("different-host:9092");
        Assertions.assertThrows(IllegalArgumentException.class, () -> offsets.getOffsets(request));
        Mockito.verifyNoInteractions(client);
    }

    /** 活跃消费组在重置前被拒绝。 */
    @Test
    @DisplayName("模拟响应：活跃消费组在重置前被拒绝")
    void activeGroupIsRejectedBeforeAlteration() {
        prepareReset(ConsumerGroupState.STABLE);
        Assertions.assertThrows(IllegalStateException.class, () -> offsets.resetOffsets(reset()));
        Mockito.verify(client, Mockito.never()).alterConsumerGroupOffsets(Mockito.anyString(), Mockito.anyMap(), Mockito.any());
    }

    /** 非法分区和越界位点不产生修改。 */
    @Test
    @DisplayName("模拟响应：非法分区和越界位点不产生修改")
    void invalidPartitionAndOutOfRangeOffsetNeverAlter() {
        prepareReset(ConsumerGroupState.EMPTY);
        ResetOffsetRequest request = reset();
        request.setPartitionId(8);
        Assertions.assertThrows(IllegalArgumentException.class, () -> offsets.resetOffsets(request));
        request.setPartitionId(0);
        request.setOffset(11L);
        Assertions.assertThrows(IllegalArgumentException.class, () -> offsets.resetOffsets(request));
        Mockito.verify(client, Mockito.never()).alterConsumerGroupOffsets(Mockito.anyString(), Mockito.anyMap(), Mockito.any());
    }

    /** 没有匹配时间戳时不能自动重置到末尾。 */
    @Test
    @DisplayName("模拟响应：没有匹配时间戳时不能自动重置到末尾")
    void unmatchedTimestampDoesNotSilentlyResetToEnd() {
        prepareReset(ConsumerGroupState.EMPTY);
        ResetOffsetRequest request = reset();
        request.setOffset(null);
        request.setTimestamp(100L);
        request.setResetOffsetMode(ResetOffsetMode.CONSUME_FROM_TIMESTAMP);
        Assertions.assertThrows(IllegalArgumentException.class, () -> offsets.resetOffsets(request));
        Mockito.verify(client, Mockito.never()).alterConsumerGroupOffsets(Mockito.anyString(), Mockito.anyMap(), Mockito.any());
    }

    /** 分区部分成功时返回各分区状态和原始错误。 */
    @Test
    @DisplayName("模拟响应：分区部分成功时返回各分区状态和原始错误")
    @SuppressWarnings("unchecked")
    void resetReportsPartialSuccessAndPreservesCause() throws Exception {
        prepareReset(ConsumerGroupState.EMPTY);
        AlterConsumerGroupOffsetsResult changed = Mockito.mock(AlterConsumerGroupOffsetsResult.class);
        Mockito.when(client.alterConsumerGroupOffsets(Mockito.anyString(), Mockito.anyMap(), Mockito.any())).thenReturn(changed);
        Mockito.when(changed.partitionResult(partition)).thenReturn(KafkaFuture.completedFuture(null));
        GroupAuthorizationException denied = new GroupAuthorizationException("group-a");
        Mockito.when(changed.partitionResult(new TopicPartition("topic-a", 1))).thenReturn(failed(denied));
        ResetOffsetRequest request = reset();
        request.setPartitionId(null);
        var result = offsets.resetOffsets(request);
        Assertions.assertEquals(207, result.getCode());
        Assertions.assertEquals(Status.SUCCESS, result.getData().get(0).getStatus());
        Assertions.assertEquals(Status.FAILED, result.getData().get(1).getStatus());
        Assertions.assertSame(denied, result.getData().get(1).getThrowable());
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> targets = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(client).alterConsumerGroupOffsets(Mockito.eq("group-a"), targets.capture(), Mockito.any());
        Assertions.assertEquals(2, targets.getValue().size());
        Assertions.assertEquals(4, targets.getValue().get(partition).offset());
    }

    /** 重置超时返回结果未知。 */
    @Test
    @DisplayName("模拟响应：重置超时返回结果未知")
    @SuppressWarnings("unchecked")
    void timeoutIsUnknownNotFailed() throws Exception {
        prepareReset(ConsumerGroupState.EMPTY);
        KafkaFuture<Void> future = Mockito.mock(KafkaFuture.class);
        Mockito.when(future.get(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS))).thenThrow(new TimeoutException("timeout"));
        Mockito.when(client.alterConsumerGroupOffsets(Mockito.anyString(), Mockito.anyMap(), Mockito.any()).partitionResult(partition))
            .thenReturn(future);
        var result = offsets.resetOffsets(reset());
        Assertions.assertEquals(207, result.getCode());
        Assertions.assertEquals(Status.UNKNOWN, result.getData().get(0).getStatus());
    }

    /** 重置中断保留中断标记并返回结果未知。 */
    @Test
    @DisplayName("模拟响应：重置中断保留中断标记并返回结果未知")
    @SuppressWarnings("unchecked")
    void interruptedResetPreservesFlagAndUnknownOutcome() throws Exception {
        prepareReset(ConsumerGroupState.EMPTY);
        KafkaFuture<Void> future = Mockito.mock(KafkaFuture.class);
        Mockito.when(future.get(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS))).thenThrow(new InterruptedException());
        Mockito.when(client.alterConsumerGroupOffsets(Mockito.anyString(), Mockito.anyMap(), Mockito.any()).partitionResult(partition))
            .thenReturn(future);
        try {
            Assertions.assertEquals(Status.UNKNOWN, offsets.resetOffsets(reset()).getData().get(0).getStatus());
            Assertions.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    /** 配置请求必须明确指定目标范围。 */
    @Test
    @DisplayName("模拟响应：配置请求必须明确指定目标范围")
    void configRequiresExplicitScopeAndUnambiguousResource() {
        ConfigRequest request = new ConfigRequest();
        Assertions.assertThrows(IllegalArgumentException.class, () -> configs.getConfigs(request));
        request.setScope(ConfigRequest.Scope.DEFAULT_BROKER);
        request.setResourceName("1");
        Assertions.assertThrows(IllegalArgumentException.class, () -> configs.getConfigs(request));
        request.setScope(ConfigRequest.Scope.BROKER);
        request.setResourceName("-1");
        Assertions.assertThrows(IllegalArgumentException.class, () -> configs.getConfigs(request));
        Mockito.verifyNoInteractions(client);
    }

    /** 配置查询不返回敏感值。 */
    @Test
    @DisplayName("模拟响应：配置查询不返回敏感值")
    void configQuerySuppressesSensitiveValues() throws Exception {
        ConfigEntry secret = Mockito.mock(ConfigEntry.class);
        Mockito.when(secret.name()).thenReturn("password");
        Mockito.when(secret.value()).thenReturn("must-not-leak");
        Mockito.when(secret.isSensitive()).thenReturn(true);
        Mockito.when(secret.source()).thenReturn(ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG);
        Config config = new Config(List.of(secret));
        Mockito.when(client.describeConfigs(Mockito.anyCollection(), Mockito.any()).all()).thenReturn(KafkaFuture.completedFuture(
            Map.of(new ConfigResource(ConfigResource.Type.BROKER, ""), config)));
        ConfigRequest request = new ConfigRequest();
        request.setScope(ConfigRequest.Scope.DEFAULT_BROKER);
        var row = configs.getConfigs(request).getData().get(0);
        Assertions.assertTrue(row.isSensitive());
        Assertions.assertNull(row.getValue());
        Assertions.assertEquals("DYNAMIC_BROKER_CONFIG", row.getSource());
    }

    /** 配置修改只设置显式传入的键和目标。 */
    @Test
    @DisplayName("模拟响应：配置修改只设置显式传入的键和目标")
    @SuppressWarnings("unchecked")
    void configUpdateOnlySetsExplicitKeysAndTarget() throws Exception {
        Mockito.when(client.incrementalAlterConfigs(Mockito.anyMap(), Mockito.any()).all()).thenReturn(KafkaFuture.completedFuture(null));
        ConfigRequest request = new ConfigRequest();
        request.setScope(ConfigRequest.Scope.BROKER);
        request.setResourceName("1");
        request.setConfigs(Map.of("log.retention.ms", "60000"));
        Mockito.clearInvocations(client);
        Assertions.assertEquals(200, configs.updateConfigs(request).getCode());
        ArgumentCaptor<Map<ConfigResource, Collection<AlterConfigOp>>> changes = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(client).incrementalAlterConfigs(changes.capture(), Mockito.any());
        Assertions.assertEquals(1, changes.getValue().size());
        var ops = changes.getValue().get(new ConfigResource(ConfigResource.Type.BROKER, "1"));
        Assertions.assertEquals(1, ops.size());
        Assertions.assertEquals(AlterConfigOp.OpType.SET, ops.iterator().next().opType());
        Assertions.assertEquals("60000", ops.iterator().next().configEntry().value());
    }

    private ResetOffsetRequest reset() {
        ResetOffsetRequest request = new ResetOffsetRequest();
        request.setGroupName("group-a");
        request.setTopic("topic-a");
        request.setPartitionId(0);
        request.setResetOffsetMode(ResetOffsetMode.CONSUME_FROM_DESIGNATED_OFFSET);
        request.setOffset(4L);
        return request;
    }

    private void prepareReset(ConsumerGroupState state) {
        topic();
        logOffsets();
        Mockito.when(client.describeConsumerGroups(Mockito.anyCollection(), Mockito.any()).all())
            .thenReturn(KafkaFuture.completedFuture(Map.of("group-a", group(state))));
    }

    private void topic() {
        Node node = new Node(1, "localhost", 9092);
        TopicDescription description = new TopicDescription("topic-a", false, List.of(
            new TopicPartitionInfo(0, node, List.of(node), List.of(node)), new TopicPartitionInfo(1, node, List.of(node), List.of(node))));
        Mockito.when(client.describeTopics(Mockito.anyCollection(), Mockito.any()).allTopicNames())
            .thenReturn(KafkaFuture.completedFuture(Map.of("topic-a", description)));
    }

    private void logOffsets() {
        Mockito.when(client.listOffsets(Mockito.anyMap(), Mockito.any())).thenAnswer(call -> {
            Map<TopicPartition, OffsetSpec> query = call.getArgument(0);
            Map<TopicPartition, ListOffsetsResultInfo> result = new java.util.LinkedHashMap<>();
            query.forEach((key, spec) -> result.put(key, new ListOffsetsResultInfo(
                spec instanceof OffsetSpec.EarliestSpec ? 0 : spec instanceof OffsetSpec.LatestSpec ? 10 : -1, -1, Optional.empty())));
            var response = Mockito.mock(org.apache.kafka.clients.admin.ListOffsetsResult.class);
            Mockito.when(response.all()).thenReturn(KafkaFuture.completedFuture(result));
            return response;
        });
    }

    private ConsumerGroupDescription group(ConsumerGroupState state) {
        return new ConsumerGroupDescription("group-a", false, List.of(), "range", state, new Node(1, "localhost", 9092));
    }

    private <T> KafkaFuture<T> failed(Throwable cause) {
        KafkaFutureImpl<T> future = new KafkaFutureImpl<>();
        future.completeExceptionally(cause);
        return future;
    }
}
