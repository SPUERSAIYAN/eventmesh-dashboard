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

package org.apache.eventmesh.dashboard.console.spring.support;

import org.apache.eventmesh.dashboard.common.model.base.BaseRuntimeIdBase;
import org.apache.eventmesh.dashboard.common.model.metadata.ConfigMetadata;
import org.apache.eventmesh.dashboard.common.model.metadata.ConsumeOffsetMetadata;
import org.apache.eventmesh.dashboard.common.model.metadata.GroupMemberMetadata;
import org.apache.eventmesh.dashboard.common.model.metadata.GroupMetadata;
import org.apache.eventmesh.dashboard.common.model.metadata.NetConnectionMetadata;
import org.apache.eventmesh.dashboard.common.model.metadata.RuntimeMetadata;
import org.apache.eventmesh.dashboard.common.model.metadata.TopicMetadata;
import org.apache.eventmesh.dashboard.common.model.metadata.TopicOffsetMetadata;
import org.apache.eventmesh.dashboard.common.model.remoting.GlobalResult;
import org.apache.eventmesh.dashboard.common.model.remoting.RemotingActionType;
import org.apache.eventmesh.dashboard.console.entity.base.BaseClusterIdEntity;
import org.apache.eventmesh.dashboard.console.entity.cluster.NetConnectionEntity;
import org.apache.eventmesh.dashboard.console.entity.cluster.RuntimeEntity;
import org.apache.eventmesh.dashboard.console.entity.function.ConfigEntity;
import org.apache.eventmesh.dashboard.console.entity.message.ConsumeOffsetEntity;
import org.apache.eventmesh.dashboard.console.entity.message.GroupEntity;
import org.apache.eventmesh.dashboard.console.entity.message.GroupMemberEntity;
import org.apache.eventmesh.dashboard.console.entity.message.TopicEntity;
import org.apache.eventmesh.dashboard.console.entity.message.TopicOffsetEntity;
import org.apache.eventmesh.dashboard.core.remoting.RemotingResultHook;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;



@Component
public class DBRemotingResultHook implements RemotingResultHook {


    private final Map<Class<?>, SQLMetadataWrapper> sqlMetadataWrapperHashMap = new HashMap<>();

    private final Map<Class<?>, Map<Long, SimpleLock>> simpleLockHashMap = new HashMap<>();

    @Autowired
    private DataSource dataSource;

    {
        // TODO
        this.buildSqlMetadataWrapper(RuntimeEntity.class, RuntimeMetadata.class, "runtime");
        this.buildSqlMetadataWrapper(TopicEntity.class, TopicMetadata.class, "topic");
        this.buildSqlMetadataWrapper(TopicOffsetEntity.class, TopicOffsetMetadata.class, "topic_offset");
        this.buildSqlMetadataWrapper(ConsumeOffsetEntity.class, ConsumeOffsetMetadata.class, "consume_offset");
        this.buildSqlMetadataWrapper(GroupEntity.class, GroupMetadata.class, "group");
        this.buildSqlMetadataWrapper(GroupMemberEntity.class, GroupMemberMetadata.class, "group_member");
        this.buildSqlMetadataWrapper(ConfigEntity.class, ConfigMetadata.class, "config");
        this.buildSqlMetadataWrapper(NetConnectionEntity.class, NetConnectionMetadata.class, "net_connection");


    }

    public void monitor(List<? extends BaseClusterIdEntity> baseClusterIdEntityList) {
        SimpleLock simpleLock = new SimpleLock(baseClusterIdEntityList.size());
        Map<Long, SimpleLock> simpleLockMap = simpleLockHashMap.get(baseClusterIdEntityList.getClass());
        for (BaseClusterIdEntity baseClusterIdEntity : baseClusterIdEntityList) {
            simpleLockMap.put(baseClusterIdEntity.getId(), simpleLock);
        }

    }

    @Override
    public void success(RemotingActionType remotingActionType, BaseRuntimeIdBase baseRuntimeIdBase, GlobalResult<Object> result) {
        SQLMetadataWrapper sqlMetadataWrapper = sqlMetadataWrapperHashMap.get(baseRuntimeIdBase.getClass());
        Object[] args = new Object[1];
        args[0] = baseRuntimeIdBase.getId();
        this.execute(sqlMetadataWrapper.success, args);
    }

    @Override
    public void success(RemotingActionType remotingActionType, List<BaseRuntimeIdBase> baseRuntimeIdBase) {
        if (baseRuntimeIdBase.isEmpty()) {
            return;
        }
        SQLMetadataWrapper sqlMetadataWrapper = sqlMetadataWrapperHashMap.get(baseRuntimeIdBase.get(0).getClass());
        StringBuilder builder = new StringBuilder();
        Object[] args = new Object[baseRuntimeIdBase.size()];
        for (int i = 0; i < baseRuntimeIdBase.size(); i++) {
            builder.append(sqlMetadataWrapper.success);
            args[i] = baseRuntimeIdBase.get(i).getId();
            builder.append(";");
        }
        this.execute(builder.toString(), args);
    }

    @Override
    public void fail(RemotingActionType remotingActionType, BaseRuntimeIdBase baseRuntimeIdBase, GlobalResult<Object> result, Throwable e) {
        SQLMetadataWrapper sqlMetadataWrapper = sqlMetadataWrapperHashMap.get(baseRuntimeIdBase.getClass());
        Object[] args = new Object[1];
        args[0] = baseRuntimeIdBase.getId();
        this.execute(sqlMetadataWrapper.fail, args);
    }

    private void buildSqlMetadataWrapper(Class<?> entity, Class<?> clazz, String tableName) {

        SQLMetadataWrapper metadataWrapper = new SQLMetadataWrapper();
        metadataWrapper.success = "update " + tableName + " set sync_status = 'SUCCESS'  where id = ?";
        metadataWrapper.fail = "update " + tableName + " set sync_status = 'FAIL'     where id = ?";
        sqlMetadataWrapperHashMap.put(clazz, metadataWrapper);

        simpleLockHashMap.put(entity, new ConcurrentHashMap<>());
    }

    private void execute(String sql, Object[] args) {
        try (Connection connection = this.dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            connection.setAutoCommit(true);
            for (int i = 0; i < args.length; i++) {
                preparedStatement.setObject(i + 1, args[i]);
            }
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    static class SQLMetadataWrapper {

        private String success;

        private String fail;
    }

    static class SimpleLock {

        private final CountDownLatch countDownLatch;

        public SimpleLock(int count) {
            countDownLatch = new CountDownLatch(count);
        }

        public void countDown() {
            this.countDownLatch.countDown();
        }

        public void await() throws InterruptedException {
            this.await(5000, TimeUnit.MILLISECONDS);
        }

        public void await(long timeout, TimeUnit unit) throws InterruptedException {
            boolean isTimeout = this.countDownLatch.await(timeout, unit);
            if (isTimeout) {
                throw new InterruptedException("操作超时");
            }
        }

    }

}
