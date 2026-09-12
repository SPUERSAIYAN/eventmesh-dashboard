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


package org.apache.eventmesh.dashboard.core.function.SDK.operation.rocketmq;

import org.apache.eventmesh.dashboard.common.enums.ClusterType;
import org.apache.eventmesh.dashboard.common.enums.RemotingType;
import org.apache.eventmesh.dashboard.core.function.SDK.AbstractSDKOperation;
import org.apache.eventmesh.dashboard.core.function.SDK.SDKMetadata;
import org.apache.eventmesh.dashboard.core.function.SDK.SDKTypeEnum;
import org.apache.eventmesh.dashboard.core.function.SDK.config.CreateRocketmqAdminSDKConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.util.UUID;

/** Creates isolated Admin clients, owned and closed by SDKManage. */
@SDKMetadata(clusterType = {ClusterType.STORAGE_ROCKETMQ_BROKER_MAIN_SLAVE,
    ClusterType.STORAGE_ROCKETMQ_NAMESERVER, ClusterType.STORAGE_ROCKETMQ_BROKER_RAFT},
    remotingType = RemotingType.ROCKETMQ, sdkTypeEnum = {SDKTypeEnum.ADMIN})
public class RocketMQAdminOperation extends AbstractSDKOperation<DefaultMQAdminExt, CreateRocketmqAdminSDKConfig> {

    public static final long DEFAULT_TIMEOUT_MILLIS = 3000L;

    @Override
    public DefaultMQAdminExt createClient(CreateRocketmqAdminSDKConfig config) throws Exception {
        if (config == null || config.getNetAddress() == null
            || StringUtils.isBlank(config.getNetAddress().getAddress())
            || config.getNetAddress().getPort() == null || config.getNetAddress().getPort() <= 0
            || config.getNetAddress().getPort() > 65535) {
            throw new IllegalArgumentException("A valid RocketMQ node address is required");
        }
        long timeout = config.getTimeoutMillis() == null ? DEFAULT_TIMEOUT_MILLIS : config.getTimeoutMillis();
        if (timeout <= 0) {
            throw new IllegalArgumentException("RocketMQ timeoutMillis must be positive");
        }
        boolean hasAccessKey = StringUtils.isNotBlank(config.getAccessKey());
        boolean hasSecretKey = StringUtils.isNotBlank(config.getSecretKey());
        if (hasAccessKey != hasSecretKey) {
            throw new IllegalArgumentException("RocketMQ accessKey and secretKey must be supplied together");
        }
        RPCHook hook = hasAccessKey
            ? new AclClientRPCHook(new SessionCredentials(config.getAccessKey(), config.getSecretKey())) : null;
        DefaultMQAdminExt admin = new DefaultMQAdminExt(hook, timeout);
        // Each registered node owns its MQClientInstance, preventing cross-cluster reuse and shutdown interference.
        String clientId = "eventmesh-admin-" + UUID.randomUUID();
        admin.setInstanceName(clientId);
        admin.setAdminExtGroup(clientId);
        admin.setNamesrvAddr(StringUtils.trimToNull(config.getNamesrvAddr()));
        admin.setUseTLS(Boolean.TRUE.equals(config.getUseTls()));
        admin.setVipChannelEnabled(false);
        try {
            admin.start();
            return admin;
        } catch (Exception e) {
            admin.shutdown();
            throw e;
        }
    }

    @Override
    public void close(DefaultMQAdminExt client) {
        client.shutdown();
    }
}
