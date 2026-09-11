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

package org.apache.eventmesh.dashboard.console.utils.data.adaptation.type;

import org.apache.eventmesh.dashboard.common.enums.MetadataType;
import org.apache.eventmesh.dashboard.common.enums.message.TopicType;
import org.apache.eventmesh.dashboard.console.entity.message.TopicEntity;
import org.apache.eventmesh.dashboard.console.utils.data.adaptation.Adaptation;

import java.util.Objects;

/**
 *
 */
public interface TopicAdaptation<V> extends Adaptation<TopicEntity, V> {

    @Override
    default MetadataType metadataType() {
        return MetadataType.TOPIC;
    }

    @Override
    default void fill(TopicEntity t) {
        if (Objects.isNull(t.getWriteQueueNum())) {
            t.setWriteQueueNum(0);
        }
        if (Objects.isNull(t.getTopicType())) {
            t.setTopicType(TopicType.NORMAL.name());
        }
        if (Objects.isNull(t.getOrder())) {
            t.setOrder(0);
        }
        if (Objects.isNull(t.getReplicationFactor())) {
            t.setReplicationFactor(1);
        }
        t.setCreateProgress(1);
        if (Objects.isNull(t.getRetentionMs())) {
            t.setRetentionMs(3600 * 24 * 3L);
        }
    }
}
