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

import org.apache.eventmesh.dashboard.common.model.remoting.BaseGlobalResult;
import org.apache.eventmesh.dashboard.common.model.remoting.kafka.config.ConfigRequest;
import org.apache.eventmesh.dashboard.common.model.remoting.kafka.config.GetConfigsResult;
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
    public GetConfigsResult getConfigs(ConfigRequest request) throws Exception {
        ConfigResource resource = this.resource(request);
        Map<ConfigResource, Config> configs = this.awaitResult(this.getClient()
            .describeConfigs(List.of(resource), new DescribeConfigsOptions().timeoutMs(ADMIN_TIMEOUT_MS)).all());
        Config config = configs == null ? null : configs.get(resource);
        if (config == null) {
            throw new IllegalStateException("Kafka configuration response is incomplete");
        }
        List<GetConfigsResult.Entry> entries = new ArrayList<>();
        for (ConfigEntry entry : config.entries()) {
            GetConfigsResult.Entry converted = new GetConfigsResult.Entry();
            converted.setName(entry.name());
            converted.setValue(entry.isSensitive() ? null : entry.value());
            converted.setSensitive(entry.isSensitive());
            converted.setReadOnly(entry.isReadOnly());
            converted.setSource(entry.source().name());
            entries.add(converted);
        }
        entries.sort(Comparator.comparing(GetConfigsResult.Entry::getName));
        GetConfigsResult result = new GetConfigsResult();
        result.setCode(200);
        result.setData(entries);
        return result;
    }

    @Override
    public BaseGlobalResult updateConfigs(ConfigRequest request) throws Exception {
        ConfigResource resource = this.resource(request);
        if (request.getConfigs() == null || request.getConfigs().isEmpty()) {
            throw new IllegalArgumentException("At least one config is required");
        }
        Collection<AlterConfigOp> operations = new ArrayList<>();
        request.getConfigs().forEach((key, value) -> {
            if (key == null || key.isBlank() || !key.equals(key.trim()) || value == null) {
                throw new IllegalArgumentException("Config keys must be nonblank and values must be non-null");
            }
            operations.add(new AlterConfigOp(new ConfigEntry(key, value), AlterConfigOp.OpType.SET));
        });
        this.awaitResult(this.getClient().incrementalAlterConfigs(Map.of(resource, operations),
            new AlterConfigsOptions().timeoutMs(ADMIN_TIMEOUT_MS)).all());
        return this.successfulResult();
    }

    private ConfigResource resource(ConfigRequest request) {
        if (request == null || request.getScope() == null) {
            throw new IllegalArgumentException("Config scope is required");
        }
        switch (request.getScope()) {
            case TOPIC:
                return new ConfigResource(ConfigResource.Type.TOPIC, this.requireName(request.getResourceName(), "topic"));
            case BROKER:
                String broker = this.requireName(request.getResourceName(), "brokerId");
                if (!broker.matches("0|[1-9][0-9]*") || Long.parseLong(broker) > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("brokerId must be a nonnegative integer");
                }
                return new ConfigResource(ConfigResource.Type.BROKER, broker);
            case DEFAULT_BROKER:
                if (request.getResourceName() != null) {
                    throw new IllegalArgumentException("DEFAULT_BROKER must not specify resourceName");
                }
                return new ConfigResource(ConfigResource.Type.BROKER, "");
            default:
                throw new IllegalArgumentException("Unsupported config scope");
        }
    }
}
