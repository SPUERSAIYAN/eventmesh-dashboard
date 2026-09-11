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


package org.apache.eventmesh.dashboard.core.remoting.jvm;

import org.apache.eventmesh.dashboard.common.enums.MetadataType;
import org.apache.eventmesh.dashboard.common.model.base.BaseClusterIdBase;
import org.apache.eventmesh.dashboard.common.model.remoting.GlobalResult;
import org.apache.eventmesh.dashboard.core.metadata.MetadataHandler;
import org.apache.eventmesh.dashboard.core.metadata.SyncMetadataCreateFactory;
import org.apache.eventmesh.dashboard.core.remoting.AbstractRemotingService;

import org.apache.commons.lang3.reflect.FieldUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;


public abstract class AbstractJvmService<T> extends AbstractRemotingService<T> {

    private SyncMetadataCreateFactory syncMetadataCreateFactory;

    private MetadataHandler<BaseClusterIdBase> baseMetadataHandler;

    protected abstract MetadataType metadataType();

    public void setSyncMetadataCreateFactoryMap(Map<MetadataType, SyncMetadataCreateFactory> syncMetadataCreateFactoryMap) {
        this.syncMetadataCreateFactory = syncMetadataCreateFactoryMap.get(this.metadataType());
        this.baseMetadataHandler = this.syncMetadataCreateFactory.createDataMetadataHandler(this.baseSyncBase);
    }

    @SuppressWarnings("unchecked")
    public <T> T tt(Object t) {
        return (T) t;
    }

    @SuppressWarnings("unchecked")
    protected List<T> getMockDatabaseData() {
        List<Object> list = (List<Object>) ((Object) baseMetadataHandler.getData(null));
        return (List<T>) list.stream().map(syncMetadataCreateFactory.getConvertMetaData()::toMetaData).toList();
    }

    protected <T> T randomUpdateMockDatabaseData(List<String> updateKey, GlobalResult<Object> globalResult) {
        return this.randomUpdateMockDatabaseData(10, updateKey, globalResult);
    }

    @SuppressWarnings("unchecked")
    protected <T> T randomUpdateMockDatabaseData(int percentage, List<String> updateKey, GlobalResult<Object> globalResult) {
        List<T> list = (List<T>) this.getMockDatabaseData();
        int updateNum = list.size() * 100 / percentage == 0 ? 1 : list.size() * 100 / percentage + 1;
        List<T> newHandleData = new ArrayList<>(list.size() + updateNum);
        for (int i = 0; i < list.size(); i++) {
            T data = list.get(i);
            if (updateNum > i) {
                newHandleData.add(data);
            }
            updateKey.forEach(k -> {
                try {
                    Object value = FieldUtils.readField(data, k, true);
                    if (Objects.isNull(value)) {
                        return;
                    }
                    Object newValue = null;
                    if (value instanceof String) {
                        newValue = value + "1";
                    } else if (value instanceof Integer) {
                        newValue = value.toString() + 1;
                    }
                    FieldUtils.writeField(data, k, newValue, true);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        globalResult.setCode(200);
        globalResult.setData(newHandleData);
        return (T) globalResult;
    }

}
