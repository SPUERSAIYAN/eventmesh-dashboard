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

import org.apache.eventmesh.dashboard.common.model.metadata.ConfigMetadata;
import org.apache.eventmesh.dashboard.common.model.remoting.BaseGlobalResult;
import org.apache.eventmesh.dashboard.common.model.remoting.config.ConfigType;
import org.apache.eventmesh.dashboard.common.model.remoting.config.GetConfigRequest;
import org.apache.eventmesh.dashboard.common.model.remoting.config.GetConfigResult;
import org.apache.eventmesh.dashboard.common.model.remoting.config.UpdateConfigRequest;
import org.apache.eventmesh.dashboard.service.remoting.kafka.ConfigRemotingService;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.common.config.ConfigResource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class KafkaConfigRemotingService extends AbstractKafkaRemotingService implements ConfigRemotingService {

    @Override
    public GetConfigResult getConfigs(GetConfigRequest request) throws Exception {
        if (request == null) {
            throw new IllegalArgumentException("Config request is required");
        }
        final ConfigResource resource = this.resource(request.getConfigType(), request.getNode(), request.getConfigObjectName());
        Map<ConfigResource, Config> configs = this.awaitResult(this.getClient()
            .describeConfigs(List.of(resource), new DescribeConfigsOptions().timeoutMs(ADMIN_TIMEOUT_MS)).all());
        Config config = configs == null ? null : configs.get(resource);
        if (config == null) {
            throw new IllegalStateException("Kafka configuration response is incomplete");
        }
        List<ConfigMetadata> entries = new ArrayList<>();
        for (ConfigEntry entry : config.entries()) {
            ConfigMetadata converted = new ConfigMetadata();
            converted.setConfigName(entry.name());
            converted.setConfigValue(entry.isSensitive() ? null : entry.value());
            converted.setSensitive(entry.isSensitive());
            converted.setReadOnly(entry.isReadOnly());
            converted.setSource(entry.source().name());
            entries.add(converted);
        }
        entries.sort(Comparator.comparing(ConfigMetadata::getConfigName));
        GetConfigResult result = new GetConfigResult();
        result.setCode(200);
        result.setData(entries);
        return result;
    }

    @Override
    public BaseGlobalResult updateConfigs(UpdateConfigRequest request) throws Exception {
        if (request == null) {
            throw new IllegalArgumentException("Config request is required");
        }
        final ConfigResource resource = this.resource(request.getConfigType(), request.getNode(), request.getConfigObjectName());
        if (request.getFullConfig() != null) {
            throw new IllegalArgumentException("Full configuration replacement is not supported");
        }
        List<Object> entries = request.getIncrementConfig();
        if (request.getMetaData() != null) {
            if (entries != null) {
                throw new IllegalArgumentException("Specify metaData or incrementConfig, not both");
            }
            entries = List.of(request.getMetaData());
        }
        if (entries == null || entries.isEmpty()) {
            throw new IllegalArgumentException("At least one ConfigMetadata is required");
        }
        Collection<AlterConfigOp> operations = new ArrayList<>();
        for (Object entry : entries) {
            if (!(entry instanceof ConfigMetadata)) {
                throw new IllegalArgumentException("incrementConfig must contain ConfigMetadata");
            }
            ConfigMetadata config = (ConfigMetadata) entry;
            String key = config.getConfigName();
            if (key == null || key.isBlank() || !key.equals(key.trim()) || config.getConfigValue() == null) {
                throw new IllegalArgumentException("Config keys must be nonblank and values must be non-null");
            }
            operations.add(new AlterConfigOp(new ConfigEntry(key, config.getConfigValue()), AlterConfigOp.OpType.SET));
        }
        this.awaitResult(this.getClient().incrementalAlterConfigs(Map.of(resource, operations),
            new AlterConfigsOptions().timeoutMs(ADMIN_TIMEOUT_MS)).all());
        return this.successfulResult();
    }

    private ConfigResource resource(ConfigType type, String node, String objectName) {
        if (type == ConfigType.TOPIC) {
            if (node != null) {
                throw new IllegalArgumentException("Topic configuration must not specify node");
            }
            return new ConfigResource(ConfigResource.Type.TOPIC, this.requireName(objectName, "configObjectName"));
        }
        if (type != ConfigType.NODE || node == null || objectName != null) {
            throw new IllegalArgumentException("NODE configuration requires node; an explicit empty node selects broker defaults");
        }
        if (!node.isEmpty()) {
            if (!node.matches("0|[1-9][0-9]*")) {
                throw new IllegalArgumentException("node must be a nonnegative broker ID");
            }
            Integer.parseInt(node);
        }
        return new ConfigResource(ConfigResource.Type.BROKER, node);
    }
}
