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

import org.apache.eventmesh.dashboard.common.model.metadata.TopicMetadata;
import org.apache.eventmesh.dashboard.common.model.remoting.GlobalResult;
import org.apache.eventmesh.dashboard.common.model.remoting.topic.CreateTopic2Request;
import org.apache.eventmesh.dashboard.common.model.remoting.topic.CreateTopicResult;
import org.apache.eventmesh.dashboard.common.model.remoting.topic.DeleteTopicRequest;
import org.apache.eventmesh.dashboard.common.model.remoting.topic.DeleteTopicResult;
import org.apache.eventmesh.dashboard.common.model.remoting.topic.GetTopics2Request;
import org.apache.eventmesh.dashboard.common.model.remoting.topic.GetTopicsResponse;
import org.apache.eventmesh.dashboard.common.model.remoting.topic.GetTopicsResult;
import org.apache.eventmesh.dashboard.core.function.SDK.SDKTypeEnum;
import org.apache.eventmesh.dashboard.core.function.SDK.config.CreateRocketmqAdminSDKConfig;
import org.apache.eventmesh.dashboard.core.function.SDK.operation.rocketmq.RocketMQAdminOperation;
import org.apache.eventmesh.dashboard.service.remoting.TopicRemotingService;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

/** Manages Topic configuration on exactly the registered Broker, preserving node-level synchronization. */
public class RocketMQTopicRemotingService extends AbstractRocketMQRemotingService implements TopicRemotingService {

    private static final int SUCCESS_CODE = 200;
    private static final int SDK_ERROR_BASE = 10000;

    private static final TypeReference<Map<String, String>> ATTRIBUTES_TYPE_REFERENCE = new TypeReference<>() { };

    @Override
    public CreateTopicResult createTopic(CreateTopic2Request request) throws Exception {
        TopicMetadata metadata = requireTopic(request == null ? null : request.getMetaData());
        if (metadata.getReadQueueNum() == null || metadata.getReadQueueNum() <= 0
            || metadata.getWriteQueueNum() == null || metadata.getWriteQueueNum() <= 0) {
            throw new IllegalArgumentException("Topic read and write queue counts must be positive");
        }
        TopicConfig config = new TopicConfig(metadata.getTopicName());
        config.setReadQueueNums(metadata.getReadQueueNum());
        config.setWriteQueueNums(metadata.getWriteQueueNum());
        config.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
        config.setTopicFilterType(parseFilterType(metadata.getTopicFilterType()));
        config.setOrder(Integer.valueOf(1).equals(metadata.getOrder()));
        if (StringUtils.isNotBlank(metadata.getTopicConfig())) {
            Map<String, String> attributes = JSON.parseObject(metadata.getTopicConfig(), ATTRIBUTES_TYPE_REFERENCE);
            if (attributes == null) {
                throw new IllegalArgumentException("Topic attributes must be a JSON object");
            }
            config.setAttributes(attributes);
        }
        return invokeAdmin(new CreateTopicResult(), () -> {
            if (!config.getAttributes().isEmpty()) {
                TopicConfigSerializeWrapper existing =
                    getAdminClient().getAllTopicConfig(getBrokerAddress(), getTimeoutMillis());
                if (existing == null || existing.getTopicConfigTable() == null) {
                    throw new IllegalStateException("RocketMQ returned no Topic configuration");
                }
                config.setAttributes(attributeChanges(config.getAttributes(),
                    existing.getTopicConfigTable().get(config.getTopicName())));
            }
            getAdminClient().createAndUpdateTopicConfig(getBrokerAddress(), config);
            return null;
        });
    }

    @Override
    public DeleteTopicResult deleteTopic(DeleteTopicRequest request) throws Exception {
        TopicMetadata metadata = requireTopic(request == null ? null : request.getMetaData());
        return invokeAdmin(new DeleteTopicResult(), () -> {
            // Do not delete a cluster-wide NameServer route for a node-scoped synchronization operation.
            getAdminClient().deleteTopicInBroker(Collections.singleton(getBrokerAddress()), metadata.getTopicName());
            return null;
        });
    }

    @Override
    public GetTopicsResult getAllTopics(GetTopics2Request request) throws Exception {
        return invokeAdmin(new GetTopicsResult(), () -> {
            TopicConfigSerializeWrapper wrapper = getAdminClient().getAllTopicConfig(getBrokerAddress(), getTimeoutMillis());
            if (wrapper == null || wrapper.getTopicConfigTable() == null) {
                throw new IllegalStateException("RocketMQ returned no Topic configuration");
            }
            List<TopicMetadata> topics = new ArrayList<>();
            for (TopicConfig config : wrapper.getTopicConfigTable().values()) {
                TopicMetadata metadata = new TopicMetadata();
                metadata.setTopicName(config.getTopicName());
                metadata.setReadQueueNum(config.getReadQueueNums());
                metadata.setWriteQueueNum(config.getWriteQueueNums());
                metadata.setTopicFilterType(config.getTopicFilterType().name());
                metadata.setOrder(config.isOrder() ? 1 : 0);
                metadata.setTopicConfig(JSON.toJSONString(config.getAttributes()));
                topics.add(metadata);
            }
            topics.sort(Comparator.comparing(TopicMetadata::getTopicName));
            return new GetTopicsResponse(topics);
        });
    }

    /** Converts stored attribute snapshots into SDK changes, omitting unchanged immutable attributes. */
    private Map<String, String> attributeChanges(Map<String, String> requested, TopicConfig current) {
        Map<String, String> existing = current == null ? Collections.emptyMap() : current.getAttributes();
        Map<String, String> changes = new HashMap<>();
        for (Map.Entry<String, String> attribute : requested.entrySet()) {
            String key = attribute.getKey();
            if (StringUtils.isBlank(key) || attribute.getValue() == null) {
                throw new IllegalArgumentException("Topic attribute names and values must not be null");
            }
            if (key.startsWith("-")) {
                changes.put(key, attribute.getValue());
                continue;
            }
            String name = key.startsWith("+") ? key.substring(1) : key;
            if (!Objects.equals(existing.get(name), attribute.getValue())) {
                changes.put("+" + name, attribute.getValue());
            }
        }
        return changes;
    }

    private TopicMetadata requireTopic(TopicMetadata metadata) {
        if (metadata == null || StringUtils.isBlank(metadata.getTopicName())) {
            throw new IllegalArgumentException("Topic name is required");
        }
        return metadata;
    }

    private TopicFilterType parseFilterType(String filterType) {
        if (StringUtils.isBlank(filterType)) {
            return TopicFilterType.SINGLE_TAG;
        }
        try {
            return TopicFilterType.valueOf(filterType);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported Topic filter type: " + filterType, e);
        }
    }

    /** Returns the Admin SDK from the wrapper supplied by SDKManage, without replacing getClient(). */
    protected DefaultMQAdminExt getAdminClient() {
        return getClient(SDKTypeEnum.ADMIN);
    }

    private String getBrokerAddress() {
        return adminConfig().getNetAddress().doUniqueKey();
    }

    private long getTimeoutMillis() {
        Long timeout = adminConfig().getTimeoutMillis();
        return timeout == null ? RocketMQAdminOperation.DEFAULT_TIMEOUT_MILLIS : timeout;
    }

    private CreateRocketmqAdminSDKConfig adminConfig() {
        return (CreateRocketmqAdminSDKConfig) getCreateSdkConfig();
    }

    /** Preserves the existing result codes and lets transport failures propagate to the caller. */
    private <T, R extends GlobalResult<T>> R invokeAdmin(R result, AdminCall<T> call) throws Exception {
        try {
            result.setData(call.execute());
            result.setCode(SUCCESS_CODE);
        } catch (MQBrokerException e) {
            result.setCode(SDK_ERROR_BASE + e.getResponseCode());
            result.setMessage(e.getErrorMessage());
            result.setThrowable(e);
        } catch (MQClientException e) {
            if (e.getResponseCode() < 0) {
                throw e;
            }
            result.setCode(SDK_ERROR_BASE + e.getResponseCode());
            result.setMessage(e.getErrorMessage());
            result.setThrowable(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
        return result;
    }

    /** One Admin SDK invocation. */
    @FunctionalInterface
    private interface AdminCall<T> {

        /** Executes one SDK operation, returning its payload or null for an operation without a payload. */
        T execute() throws Exception;
    }
}
