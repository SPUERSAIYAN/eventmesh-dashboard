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

package org.apache.eventmesh.dashboard.console.function.report.collect.padding;

import org.apache.eventmesh.dashboard.common.model.base.BaseOrganizationBase;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import lombok.Setter;


public abstract class AbstractPadding<T, D extends BaseOrganizationBase> implements Padding<T> {

    private final Map<Long, Map<String, D>> paddingData = new ConcurrentHashMap<>();


    @Setter
    private Function<D, Long> idFunction;

    @Setter
    private Function<D, String> keyFunction;


    public void put(List<D> dataList) {
        dataList.forEach(data -> {
            if (data.getIsDelete() == 0) {
                this.remove(data);
            } else {
                this.put(data);
            }
        });
    }

    public void put(D data) {
        paddingData.computeIfAbsent(this.getId(data), key -> new ConcurrentHashMap<>()).put(this.getKey(data), data);
    }

    public D getData(Long id, String key) {
        Map<String, D> dataMap = paddingData.get(id);
        if (Objects.isNull(dataMap)) {
            return null;
        }
        return dataMap.get(key);
    }

    public void remove(D data) {
        Map<String, D> map = paddingData.get(this.getId(data));
        if (Objects.isNull(map)) {
            return;
        }
        map.remove(this.getKey(data));
    }

    private String getKey(D d) {
        return keyFunction.apply(d);
    }

    private Long getId(D d) {
        return idFunction.apply(d);
    }

}
