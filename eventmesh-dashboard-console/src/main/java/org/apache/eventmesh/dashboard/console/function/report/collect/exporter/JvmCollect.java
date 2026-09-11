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

package org.apache.eventmesh.dashboard.console.function.report.collect.exporter;

import org.apache.eventmesh.dashboard.common.annotation.ClusterTypeMark;
import org.apache.eventmesh.dashboard.common.enums.ClusterType;
import org.apache.eventmesh.dashboard.common.util.ClasspathScanner;
import org.apache.eventmesh.dashboard.console.function.report.ReportHandlerManage;
import org.apache.eventmesh.dashboard.console.function.report.collect.AbstractCollect;
import org.apache.eventmesh.dashboard.console.function.report.model.base.ClusterId.ClusterFloatValue;
import org.apache.eventmesh.dashboard.console.function.report.model.base.ClusterId.ClusterLongValue;
import org.apache.eventmesh.dashboard.console.function.report.model.base.OrganizationId;
import org.apache.eventmesh.dashboard.console.function.report.model.base.RuntimeId.RuntimeFloatValue;
import org.apache.eventmesh.dashboard.console.function.report.model.base.RuntimeId.RuntimeLongValue;
import org.apache.eventmesh.dashboard.console.function.report.model.base.not.GroupId.GroupFloatValue;
import org.apache.eventmesh.dashboard.console.function.report.model.base.not.GroupId.GroupLongValue;
import org.apache.eventmesh.dashboard.console.function.report.model.base.not.SubscribeId;
import org.apache.eventmesh.dashboard.console.function.report.model.base.not.SubscribeId.SubscribeFloatValue;
import org.apache.eventmesh.dashboard.console.function.report.model.base.not.SubscribeId.SubscribeLongValue;
import org.apache.eventmesh.dashboard.console.function.report.model.base.not.TopicId.TopicFloatValue;
import org.apache.eventmesh.dashboard.console.function.report.model.base.not.TopicId.TopicLongValue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


@ClusterTypeMark(clusterType = {ClusterType.STORAGE_JVM_BROKER, ClusterType.STORAGE_JVM_CAP_BROKER})
public class JvmCollect extends AbstractCollect {

    private static final List<Class<?>> CLASS_LIST = new ArrayList<>();

    static {
        ClasspathScanner classpathScanner =
            ClasspathScanner.builder().base(ReportHandlerManage.class).subPath("model/rocketmq").build();
        try {
            List<Class<?>> classList = classpathScanner.getClazz();
            classList.forEach(value -> {
                Field[] fields = value.getDeclaredFields();
                if (fields.length == 0) {
                    CLASS_LIST.add(value);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private final Random rand = new Random();

    @Override
    protected void doCollect() {
        CLASS_LIST.forEach(this::padding);
    }

    @SuppressWarnings({"AliDeprecation", "deprecation"})
    private void padding(Class<?> clazz) {
        OrganizationId object;
        try {
            object = (OrganizationId) clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        this.runtimePadding(object);
        this.clusterPadding(object);
        this.topicPadding(object);
        this.groupPadding(object);
        this.subscriptionPadding(object);

        this.setData(object);
    }

    private void runtimePadding(Object object) {
        if (object instanceof RuntimeLongValue runtimeLongValue) {
            runtimeLongValue.setValue(rand.nextLong());
        } else if (object instanceof RuntimeFloatValue runtimeFloatValue) {
            runtimeFloatValue.setValue(rand.nextFloat());
        }
    }

    private void clusterPadding(Object object) {
        if (object instanceof ClusterLongValue clusterLongValue) {
            clusterLongValue.setValue(rand.nextLong());
        } else if (object instanceof ClusterFloatValue clusterFloatValue) {
            clusterFloatValue.setValue(rand.nextFloat());
        }
    }

    private void topicPadding(Object object) {
        if (object instanceof TopicLongValue topicLongValue) {
            topicLongValue.setValue(rand.nextLong());
            topicLongValue.setTopicName("1");
        } else if (object instanceof TopicFloatValue topicFloatValue) {
            topicFloatValue.setValue(rand.nextFloat());
            topicFloatValue.setTopicName("1");
        }
    }

    private void groupPadding(Object object) {
        if (object instanceof GroupLongValue groupLongValue) {
            groupLongValue.setValue(rand.nextLong());
            groupLongValue.setGroupName("1");
        } else if (object instanceof GroupFloatValue groupFloatValue) {
            groupFloatValue.setValue(rand.nextFloat());
            groupFloatValue.setGroupName("1");
        }
    }

    private void subscriptionPadding(Object object) {
        if (object instanceof SubscribeId subscribeId) {
            subscribeId.setGroupName("1");
            subscribeId.setTopicName("1");
            if (object instanceof SubscribeLongValue subscribeLongValue) {
                subscribeLongValue.setValue(rand.nextLong());
            } else if (object instanceof SubscribeFloatValue subscribeFloatValue) {
                subscribeFloatValue.setValue(rand.nextFloat());
            }
        }
    }
}
