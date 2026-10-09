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


package org.apache.eventmesh.dashboard.console.baseid;

import org.apache.eventmesh.dashboard.common.enums.*;
import org.apache.eventmesh.dashboard.common.enums.ClusterTrusteeshipType.FirstToWhom;
import org.apache.eventmesh.dashboard.common.model.DatabaseAndMetadataMapper;
import org.apache.eventmesh.dashboard.common.model.base.BaseClusterIdBase;
import org.apache.eventmesh.dashboard.common.model.base.BaseSyncBase;
import org.apache.eventmesh.dashboard.common.model.metadata.RuntimeMetadata;
import org.apache.eventmesh.dashboard.common.model.metadata.TopicMetadata;
import org.apache.eventmesh.dashboard.common.model.remoting.topic.*;
import org.apache.eventmesh.dashboard.console.entity.message.TopicEntity;
import org.apache.eventmesh.dashboard.console.mapper.message.TopicMapper;
import org.apache.eventmesh.dashboard.console.service.metadata.TopicDataMetadataHandler;
import org.apache.eventmesh.dashboard.console.spring.support.metadata.convert.TopicConvertMetaData;
import org.apache.eventmesh.dashboard.core.function.SDK.*;
import org.apache.eventmesh.dashboard.core.function.SDK.config.*;
import org.apache.eventmesh.dashboard.core.function.SDK.operation.rocketmq.RocketMQRemotingSDKOperation.DefaultRemotingClient;
import org.apache.eventmesh.dashboard.core.metadata.*;
import org.apache.eventmesh.dashboard.core.metadata.result.*;
import org.apache.eventmesh.dashboard.core.remoting.Remoting2Manage;
import org.apache.eventmesh.dashboard.service.remoting.TopicRemotingService;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.DeleteTopicRequestHeader;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

/** Real MySQL -> metadata manager -> real RocketMQ, scoped to one disposable topic. */
public class BaseIdMysqlRemoteSyncTest {
    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void mysqlTombstoneAndRestoreReachRealBrokerThroughSyncManager() throws Exception {
        assumeTrue("Explicit real MySQL opt-in required", Boolean.getBoolean("baseid.mysql"));
        BaseIdDatabaseTest database = new BaseIdDatabaseTest();
        String topic = "baseid_mysql_" + UUID.randomUUID().toString().replace("-", "");
        RuntimeMetadata runtime = new RuntimeMetadata();
        runtime.setId(93001L);
        runtime.setClusterId(93002L);
        runtime.setHost("127.0.0.1");
        runtime.setPort(20911);
        runtime.setClusterType(ClusterType.STORAGE_ROCKETMQ_BROKER_RAFT);
        runtime.setTrusteeshipType(ClusterTrusteeshipType.SELF);
        runtime.setFirstToWhom(FirstToWhom.DASHBOARD);
        runtime.setFirstSyncState(FirstToWhom.WAIT_START);
        MetadataSyncManage manager = new MetadataSyncManage();
        BlockingQueue<FirstToWhom> completions = new LinkedBlockingQueue<>();
        DefaultRemotingClient client = null;
        try {
            database.database();
            TopicMapper mapper = database.session.getMapper(TopicMapper.class);
            database.seed("topic", TopicEntity.class, 93003L, Map.of(
                "topic_name", topic, "cluster_id", runtime.getClusterId(), "runtime_id", runtime.getId(),
                "cluster_type", runtime.getClusterType().name(), "read_queue_num", 2, "write_queue_num", 2));
            TopicEntity query = new TopicEntity();
            query.setId(93003L);
            TopicEntity original = mapper.queryTopicById(query);
            assertNotNull(original);

            AbstractSimpleCreateSDKConfig sdkConfig = ConfigManage.getInstance()
                .getSimpleCreateSdkConfig(runtime.getClusterType(), SDKTypeEnum.ADMIN);
            sdkConfig.setKey(topic);
            NetAddress address = new NetAddress();
            address.setAddress(runtime.getHost());
            address.setPort(runtime.getPort());
            sdkConfig.setNetAddress(address);
            SDKManage.getInstance().createClient(SDKTypeEnum.ADMIN, runtime, sdkConfig, runtime.getClusterType());
            client = SDKManage.getInstance().getClient(SDKTypeEnum.ADMIN, runtime.getUnique());
            assertNotNull(client);
            TopicRemotingService remote = Remoting2Manage.getInstance().createRemotingService(TopicRemotingService.class, runtime);
            assertNull(findTopic(client, topic));

            TopicDataMetadataHandler databaseHandler = new TopicDataMetadataHandler();
            TopicEntity cursor = new TopicEntity();
            cursor.setUpdateTime(BaseIdDatabaseTest.BOUNDARY);
            ReflectionTestUtils.setField(databaseHandler, "baseRuntimeIdBase", cursor);
            ReflectionTestUtils.setField(databaseHandler, "syncDataHandlerMapper", mapper);
            manager.setDataMetadataHandlerList((List) List.of(databaseHandler));
            MetadataSyncResultHandler resultHandler = new MetadataSyncResultHandler() {
                public void register(List<MetadataSyncResult> values) { }
                public void unregister(BaseSyncBase value) { }
                public void persistence() { }
                public void handleMetadataSyncResult(MetadataSyncResult result) {
                    completions.add(result.getFirstToWhom());
                }
            };
            manager.setMetadataSyncResultHandler(resultHandler);
            manager.init(Integer.MAX_VALUE, Integer.MAX_VALUE, List.of(DatabaseAndMetadataMapper.builder()
                .metaType(MetadataType.TOPIC).databaseHandlerClass(TopicDataMetadataHandler.class)
                .metadataHandlerClass(TopicRemotingService.class).convertMetaData(TopicConvertMetaData.INSTANCE).build()));
            MetadataSyncResult result = manager.createMetadataSyncResult(new ArrayList<>(), MetadataType.TOPIC, runtime);
            MetadataSyncManage.MetadataSyncConfig config = ReflectionTestUtils.invokeMethod(manager,
                "createMetadataSyncConfig", MetadataType.TOPIC, result, runtime);
            DataMetadataHandler<BaseClusterIdBase> realRemote = config.getClusterService();
            // Production writes stay real. This guard restricts all reads/writes to the disposable topic,
            // preventing a whole-broker reconciliation from touching any pre-existing topic.
            config.setClusterService(new DataMetadataHandler<>() {
                public List<BaseClusterIdBase> getData() {
                    List<BaseClusterIdBase> data = realRemote.getData();
                    assertNotNull("Real Broker read must succeed", data);
                    return data.stream().filter(value -> topic.equals(((TopicMetadata) value).getName())).toList();
                }
                public void handleAll(Collection<BaseClusterIdBase> all, List<BaseClusterIdBase> add,
                    List<BaseClusterIdBase> update, List<BaseClusterIdBase> delete) {
                    for (List<BaseClusterIdBase> values : List.of(add, update, delete)) {
                        values.forEach(value -> assertEquals("Only the disposable topic may be changed", topic,
                            ((TopicMetadata) value).getName()));
                    }
                    realRemote.handleAll(all, add, update, delete);
                }
            });
            MetadataSyncWrapper wrapper = new MetadataSyncWrapper();
            wrapper.setMetadataSyncConfig(config);
            wrapper.setMetadataSyncResultHandler(resultHandler);
            wrapper.createDifference();
            // Focus this test on first and incremental synchronization, avoiding periodic reconciliation.
            wrapper.setCheck(false);
            Map<String, List<MetadataSyncWrapper>> registrations = (Map) ReflectionTestUtils.getField(manager, "metadataSyncConfigMap");
            registrations.put(runtime.getUnique(), List.of(wrapper));

            sync(manager, completions);
            TopicMetadata created = findTopic(client, topic);
            assertNotNull("MySQL row must create a real Broker topic", created);
            assertEquals(Integer.valueOf(2), created.getReadQueueNum());
            assertEquals(Integer.valueOf(2), created.getWriteQueueNum());

            database.sql("update topic set read_queue_num=3, write_queue_num=4, update_time=CURRENT_TIMESTAMP where id=93003");
            sync(manager, completions);
            TopicMetadata updated = findTopic(client, topic);
            assertNotNull(updated);
            assertEquals(Integer.valueOf(3), updated.getReadQueueNum());
            assertEquals(Integer.valueOf(4), updated.getWriteQueueNum());

            database.sql("update topic set is_delete=1, update_time=CURRENT_TIMESTAMP where id=93003");
            assertNull(mapper.queryTopicById(query));
            sync(manager, completions);
            assertNull("MySQL tombstone must remove the real Broker topic", findTopic(client, topic));
            // Verify typed success as well as Broker effects; the manager logs/swallow remote failures.
            DeleteTopicRequest deleteRequest = new DeleteTopicRequest();
            TopicMetadata deleted = new TopicMetadata();
            deleted.setName(topic);
            deleteRequest.setMetaData(deleted);
            assertEquals(Integer.valueOf(200), remote.deleteTopic(deleteRequest).getCode());

            database.sql("update topic set status=1, is_delete=0, update_time=CURRENT_TIMESTAMP where id=93003");
            assertEquals(Integer.valueOf(0), mapper.queryTopicById(query).getIsDelete());
            sync(manager, completions);
            assertNotNull("Restoring the same MySQL business key must recreate the Broker topic", findTopic(client, topic));
            database.sql("update topic set is_delete=1, update_time=CURRENT_TIMESTAMP where id=93003");
            sync(manager, completions);
            assertNull(findTopic(client, topic));
            database.sql("update topic set is_delete=1, update_time=CURRENT_TIMESTAMP where id=93003");
            sync(manager, completions);
            assertNull("Repeated tombstones must remain safely deleted", findTopic(client, topic));
        } finally {
            try {
                for (String name : List.of("scheduledExecutorService", "dbThreadPoolExecutor", "threadPoolExecutor")) {
                    ExecutorService executor = (ExecutorService) ReflectionTestUtils.getField(manager, name);
                    executor.shutdownNow();
                    assertTrue("Sync executor must terminate", executor.awaitTermination(10, TimeUnit.SECONDS));
                }
            } finally {
                try {
                    if (client != null) {
                        DeleteTopicRequestHeader header = new DeleteTopicRequestHeader();
                        header.setTopic(topic);
                        // Raw protocol cleanup remains usable even if the adapter's typed delete result is broken.
                        assertEquals(ResponseCode.SUCCESS, client.invokeSync(RemotingCommand.createRequestCommand(
                            RequestCode.DELETE_TOPIC_IN_BROKER, header), 3000).getCode());
                    }
                } finally {
                    try {
                        SDKManage.getInstance().deleteClient(null, runtime.getUnique());
                    } finally {
                        database.close();
                    }
                }
            }
        }
    }

    private static void sync(MetadataSyncManage manager, BlockingQueue<FirstToWhom> completions) throws Exception {
        manager.run();
        FirstToWhom result = completions.poll(15, TimeUnit.SECONDS);
        assertEquals("Real sync worker must complete successfully", FirstToWhom.COMPLETE, result);
    }

    private static TopicMetadata findTopic(DefaultRemotingClient client, String name) throws Exception {
        RemotingCommand response = client.invokeSync(RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_CONFIG, null), 3000);
        assertEquals("Independent Broker protocol read must succeed", ResponseCode.SUCCESS, response.getCode());
        org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper configs =
            org.apache.rocketmq.remoting.protocol.RemotingSerializable.decode(response.getBody(),
                org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper.class);
        org.apache.rocketmq.common.TopicConfig config = configs.getTopicConfigTable().get(name);
        if (config == null) {
            return null;
        }
        TopicMetadata result = new TopicMetadata();
        result.setName(config.getTopicName());
        result.setReadQueueNum(config.getReadQueueNums());
        result.setWriteQueueNum(config.getWriteQueueNums());
        return result;
    }
}
