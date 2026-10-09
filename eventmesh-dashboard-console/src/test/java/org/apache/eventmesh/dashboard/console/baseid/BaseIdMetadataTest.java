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

import org.apache.eventmesh.dashboard.common.model.ConvertMetaData;
import org.apache.eventmesh.dashboard.common.model.base.BaseOrganizationBase;
import org.apache.eventmesh.dashboard.common.model.base.BaseClusterIdBase;
import org.apache.eventmesh.dashboard.common.model.metadata.*;
import org.apache.eventmesh.dashboard.console.entity.base.BaseIdEntity;
import org.apache.eventmesh.dashboard.console.entity.cluster.*;
import org.apache.eventmesh.dashboard.console.entity.message.*;
import org.apache.eventmesh.dashboard.console.entity.function.ConfigEntity;
import org.apache.eventmesh.dashboard.console.spring.support.metadata.convert.*;
import org.apache.eventmesh.dashboard.core.metadata.DataMetadataHandler;
import org.apache.eventmesh.dashboard.core.metadata.difference.BodyDataDifference;

import java.time.LocalDateTime;
import java.util.*;

import org.junit.Test;
import static org.junit.Assert.*;

public class BaseIdMetadataTest {
    @Test
    public void deletionHasPriorityInActualDifference() {
        for (BaseOrganizationBase value : List.of(new ClusterMetadata(), new RuntimeMetadata())) {
            value.setStatus(1L);
            value.setIsDelete(1);
            value.setCreateTime(null);
            value.setUpdateTime(null);
            assertTrue(value.isDelete());
            assertFalse(value.isInsert());
            assertFalse(value.isUpdate());
        }
        for (BaseClusterIdBase value : List.of(new TopicMetadata(), new ConfigMetadata(), new GroupMetadata(),
            new GroupMemberMetadata())) {
            value.setStatus(1L);
            value.setIsDelete(1);
            for (LocalDateTime time : Arrays.asList(null, LocalDateTime.of(2026, 1, 1, 0, 0))) {
                value.setCreateTime(time);
                value.setUpdateTime(time);
                assertTrue(value.isDelete());
                assertFalse(value.isInsert());
                assertFalse(value.isUpdate());
                assertActions(value, 0, 0, 1);
            }
        }
    }

    @Test
    public void classificationIsNullSafeAndLegacyCompatibilityIsTypeLimited() {
        for (BaseClusterIdBase value : List.of(new GroupMetadata(), new GroupMemberMetadata())) {
            value.setStatus(0L);
            assertFalse(value.isDelete());
            assertTrue(value.isUpdate());
            assertActions(value, 0, 1, 0);
        }
        for (BaseClusterIdBase value : List.of(new TopicMetadata(), new ConfigMetadata())) {
            value.setStatus(0L);
            value.setIsDelete(0);
            assertActions(value, 0, 0, 1);
        }
        TopicMetadata value = new TopicMetadata();
        assertActions(value, 0, 1, 0);
        value.setIsDelete(7);
        value.setStatus(7L);
        LocalDateTime time = LocalDateTime.of(2026, 1, 1, 0, 0);
        value.setCreateTime(time);
        value.setUpdateTime(time);
        assertActions(value, 1, 0, 0);
        value.setUpdateTime(time.plusSeconds(1));
        assertActions(value, 0, 1, 0);
        value.setUpdateTime(null);
        assertActions(value, 0, 1, 0);
        assertNull(new BaseIdEntity().getStatus());
        assertNull(new BaseIdEntity().getIsDelete());
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void generatedConvertersPreserveBusinessStateAndTimestampsBothWays() {
        List<BaseIdEntity> entities = List.of(new ClusterEntity(), new RuntimeEntity(), new TopicEntity(),
            new ConfigEntity(), new GroupEntity(), new GroupMemberEntity());
        List<ConvertMetaData> converters = List.of(ClusterConvertMetaData.INSTANCE, RuntimeConvertMetaData.INSTANCE,
            TopicConvertMetaData.INSTANCE, ConfigConvertMetaData.INSTANCE, GroupConvertMetaData.INSTANCE,
            GroupMemberConvertMetaData.INSTANCE);
        for (int i = 0; i < entities.size(); i++) {
            for (Long status : Arrays.asList(null, 0L, 1L, 3L)) {
                for (Integer deleted : Arrays.asList(null, 0, 1)) {
                    BaseIdEntity entity = entities.get(i);
                    entity.setStatus(status);
                    entity.setIsDelete(deleted);
                    entity.setCreateTime(LocalDateTime.of(2026, 1, 1, 0, 0));
                    entity.setUpdateTime(entity.getCreateTime());
                    BaseOrganizationBase metadata = (BaseOrganizationBase) converters.get(i).toMetaData(entity);
                    assertEquals(status, metadata.getStatus());
                    assertEquals(deleted, metadata.getIsDelete());
                    BaseIdEntity restored = (BaseIdEntity) converters.get(i).toEntity(metadata);
                    assertEquals(status, restored.getStatus());
                    assertEquals(deleted, restored.getIsDelete());
                    assertEquals(entity.getCreateTime(), restored.getCreateTime());
                    assertEquals(entity.getUpdateTime(), restored.getUpdateTime());
                }
            }
        }
    }

    static void assertActions(BaseClusterIdBase value, int inserts, int updates, int deletes) {
        value.setClusterId(10L);
        BodyDataDifference difference = new BodyDataDifference();
        Map<String, BaseClusterIdBase> all = new HashMap<>();
        all.put(value.nodeUnique(), value);
        difference.setAllData(all);
        CapturingHandler source = new CapturingHandler();
        source.values = List.of(value);
        CapturingHandler target = new CapturingHandler();
        difference.setSourceHandler(source);
        difference.setTargetHandler(target);
        difference.difference();
        assertEquals(1, target.calls);
        assertEquals(inserts, target.add.size());
        assertEquals(updates, target.update.size());
        assertEquals(deletes, target.delete.size());
        assertEquals(deletes == 0, all.containsKey(value.nodeUnique()));
    }

    static class CapturingHandler implements DataMetadataHandler<BaseClusterIdBase> {
        List<BaseClusterIdBase> values;
        List<BaseClusterIdBase> add;
        List<BaseClusterIdBase> update;
        List<BaseClusterIdBase> delete;
        int calls;

        @Override
        public List<BaseClusterIdBase> getData() {
            return values;
        }

        @Override
        public void handleAll(Collection<BaseClusterIdBase> all, List<BaseClusterIdBase> adds,
            List<BaseClusterIdBase> updates, List<BaseClusterIdBase> deletes) {
            calls++;
            add = new ArrayList<>(adds);
            update = new ArrayList<>(updates);
            delete = new ArrayList<>(deletes);
        }
    }
}
