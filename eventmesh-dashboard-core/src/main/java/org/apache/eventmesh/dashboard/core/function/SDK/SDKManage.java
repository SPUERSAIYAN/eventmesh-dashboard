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


package org.apache.eventmesh.dashboard.core.function.SDK;

import org.apache.eventmesh.dashboard.common.enums.ClusterSyncMetadataEnum;
import org.apache.eventmesh.dashboard.common.enums.ClusterType;
import org.apache.eventmesh.dashboard.common.model.base.BaseSyncBase;
import org.apache.eventmesh.dashboard.common.model.metadata.RuntimeMetadata;
import org.apache.eventmesh.dashboard.common.util.ClasspathScanner;
import org.apache.eventmesh.dashboard.core.function.SDK.config.AbstractCreateSDKConfig;
import org.apache.eventmesh.dashboard.core.function.SDK.config.CreateSDKConfig;

import org.apache.commons.lang3.ArrayUtils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


import lombok.extern.slf4j.Slf4j;

/**
 * SDK manager is a singleton to manage all SDK clients, it is a facade to create, delete and get a client.
 */
@Slf4j
public class SDKManage {

    /**
     * Initialise the SDKOperation object instance according to SDKTypeEnum.
     * <p>
     * key: SDKTypeEnum value: SDKOperation
     *
     * @see SDKTypeEnum
     * @see SDKOperation
     */
    private static final Map<ClusterType, Map<SDKTypeEnum, SDKMetadataWrapper>> CLUSTER_TYPE_MAP_CONCURRENT_HASH_MAP =
        new ConcurrentHashMap<>();
    private static final SDKManage INSTANCE = new SDKManage();

    // register all client create operation
    static {
        Set<Class<?>> interfaceSet = new HashSet<>();
        interfaceSet.add(SDKOperation.class);
        ClasspathScanner classpathScanner =
            ClasspathScanner.builder().base(SDKManage.class).subPath("/operation/**").interfaceSet(interfaceSet).build();
        try {
            List<Class<?>> classList = classpathScanner.getClazz();
            classList.forEach(SDKManage::createSDKMetadataWrapper);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * inner key is the unique key of a client, such as (ip + port) they are defined in CreateClientConfig
     * <p>
     * key: SDKTypeEnum value: A map collection is used with key being (ip+port) and value being client.
     *
     * @see CreateSDKConfig#getUniqueKey()
     */
    private final Map<String, ClientWrapper> clientMap = new ConcurrentHashMap<>();
    private final Map<String, Map<Class<?>, AbstractClientInfo<Object>>> stringMapConcurrentHashMap = new ConcurrentHashMap<>();

    private SDKManage() {
    }

    @SuppressWarnings("unchecked")
    static void createSDKMetadataWrapper(Class<?> clazz) {
        SDKMetadata[] sdkMetadataArray = clazz.getAnnotationsByType(SDKMetadata.class);
        if (ArrayUtils.isEmpty(sdkMetadataArray)) {
            return;
        }

        try {
            SDKMetadata sdkMetadata = sdkMetadataArray[0];
            Class<?> multi = getCreateSDKConfigClass(clazz);
            for (ClusterType clusterType : sdkMetadata.clusterType()) {
                Map<SDKTypeEnum, SDKMetadataWrapper> map =
                    CLUSTER_TYPE_MAP_CONCURRENT_HASH_MAP.computeIfAbsent(clusterType, k -> new ConcurrentHashMap<>());
                SDKTypeEnum[] sdkTypeEnums = sdkMetadata.sdkTypeEnum();
                if (Objects.equals(sdkTypeEnums[0], SDKTypeEnum.ALL)) {
                    sdkTypeEnums = new SDKTypeEnum[] {SDKTypeEnum.ADMIN, SDKTypeEnum.PING, SDKTypeEnum.PRODUCER, SDKTypeEnum.CONSUMER};
                }
                for (SDKTypeEnum sdkTypeEnum : sdkTypeEnums) {
                    SDKMetadataWrapper sdkMetadataWrapper = new SDKMetadataWrapper();
                    sdkMetadataWrapper.sdkMetadata = sdkMetadata;
                    sdkMetadataWrapper.createSDKConfigClass = multi;
                    sdkMetadataWrapper.abstractSDKOperation = (AbstractSDKOperation<Object, CreateSDKConfig>) clazz.newInstance();
                    map.put(sdkTypeEnum, sdkMetadataWrapper);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Class<?> getCreateSDKConfigClass(Class<?> genericClass) {
        Type supercType = genericClass.getGenericSuperclass();
        if (supercType instanceof ParameterizedType type) {
            Type[] typeArguments = type.getActualTypeArguments();
            for (Type typeArgument : typeArguments) {
                Class<?> argument;
                if (typeArgument instanceof ParameterizedType parameterizedType) {
                    argument = (Class<?>) parameterizedType.getRawType();
                } else {
                    argument = (Class<?>) typeArgument;
                }
                for (; ; ) {
                    Class<?> superclass = argument.getSuperclass();
                    if (Objects.isNull(superclass)) {
                        break;
                    }
                    if (Objects.equals(superclass, AbstractCreateSDKConfig.class)) {
                        if (typeArgument instanceof Class<?>) {
                            return (Class<?>) typeArgument;
                        }
                    }
                    argument = superclass;
                }
            }
        }
        return null;
    }

    public static synchronized SDKManage getInstance() {
        return INSTANCE;
    }

    /**
     * Creates registered ADMIN/PING clients independently when they use different SDK implementations.
     * Replacements are published only after all clients have started successfully.
     */
    @SuppressWarnings("unchecked")
    public synchronized <T> T createClient(SDKTypeEnum sdkType, BaseSyncBase base, CreateSDKConfig config,
        ClusterType clusterType) {
        SDKMetadataWrapper metadata = requireMetadata(clusterType, sdkType);
        if (sdkType == SDKTypeEnum.PRODUCER || sdkType == SDKTypeEnum.CONSUMER) {
            try {
                return (T) metadata.abstractSDKOperation.createClient(config);
            } catch (Exception e) {
                throw new IllegalStateException("Cannot create SDK client for " + clusterType, e);
            }
        }
        ClientWrapper wrapper = new ClientWrapper();
        wrapper.setConfig(config);
        wrapper.setBaseSyncBase(base);
        try {
            addClient(wrapper, sdkType, metadata, config);
            if (sdkType == SDKTypeEnum.ADMIN) {
                SDKMetadataWrapper ping = CLUSTER_TYPE_MAP_CONCURRENT_HASH_MAP.get(clusterType).get(SDKTypeEnum.PING);
                if (ping != null) {
                    if (ping.abstractSDKOperation.getClass().equals(metadata.abstractSDKOperation.getClass())) {
                        wrapper.getClientMap().put(SDKTypeEnum.PING, wrapper.getClientMap().get(SDKTypeEnum.ADMIN));
                        wrapper.getCloseActions().put(SDKTypeEnum.PING, wrapper.getCloseActions().get(SDKTypeEnum.ADMIN));
                    } else {
                        addClient(wrapper, SDKTypeEnum.PING, ping, config);
                    }
                }
            }
        } catch (Exception e) {
            closeClients(wrapper);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("Cannot create SDK clients for " + clusterType, e);
        }
        stringMapConcurrentHashMap.remove(base.getUnique());
        ClientWrapper previous = clientMap.get(base.getUnique());
        if (sdkType != SDKTypeEnum.ADMIN && previous != null) {
            AutoCloseable oldAction = previous.getCloseActions().remove(sdkType);
            previous.getClientMap().put(sdkType, wrapper.getClientMap().get(sdkType));
            if (oldAction != null && !previous.getCloseActions().containsValue(oldAction)) {
                closeClient(oldAction);
            }
            previous.getCloseActions().put(sdkType, wrapper.getCloseActions().get(sdkType));
        } else {
            clientMap.put(base.getUnique(), wrapper);
            closeClients(previous);
        }
        return (T) wrapper.getClientMap().get(sdkType);
    }

    private SDKMetadataWrapper requireMetadata(ClusterType clusterType, SDKTypeEnum sdkType) {
        Map<SDKTypeEnum, SDKMetadataWrapper> metadata = CLUSTER_TYPE_MAP_CONCURRENT_HASH_MAP.get(clusterType);
        if (metadata == null || !metadata.containsKey(sdkType)) {
            throw new IllegalArgumentException("No SDK registered for " + clusterType + "/" + sdkType);
        }
        return metadata.get(sdkType);
    }

    private void addClient(ClientWrapper wrapper, SDKTypeEnum sdkType, SDKMetadataWrapper metadata,
        CreateSDKConfig config) throws Exception {
        if (!metadata.createSDKConfigClass.isInstance(config)) {
            throw new IllegalArgumentException("Invalid SDK config for " + sdkType + ": expected "
                + metadata.createSDKConfigClass.getSimpleName());
        }
        Object client = metadata.abstractSDKOperation.createClient(config);
        wrapper.getClientMap().put(sdkType, client);
        wrapper.getCloseActions().put(sdkType, () -> metadata.abstractSDKOperation.close(client));
    }

    /** Removes cached services and closes resources, including shared clients exactly once. */
    public synchronized void deleteClient(SDKTypeEnum sdkType, String uniqueKey) {
        stringMapConcurrentHashMap.remove(uniqueKey);
        if (sdkType == null) {
            closeClients(clientMap.remove(uniqueKey));
            return;
        }
        ClientWrapper wrapper = clientMap.get(uniqueKey);
        if (wrapper == null) {
            return;
        }
        wrapper.getClientMap().remove(sdkType);
        AutoCloseable closeAction = wrapper.getCloseActions().remove(sdkType);
        if (closeAction != null && !wrapper.getCloseActions().containsValue(closeAction)) {
            closeClient(closeAction);
        }
        if (wrapper.getClientMap().isEmpty()) {
            clientMap.remove(uniqueKey);
        }
    }

    private void closeClients(ClientWrapper wrapper) {
        if (wrapper == null) {
            return;
        }
        Set<AutoCloseable> uniqueActions = Collections.newSetFromMap(new IdentityHashMap<>());
        uniqueActions.addAll(wrapper.getCloseActions().values());
        uniqueActions.forEach(this::closeClient);
        wrapper.getCloseActions().clear();
        wrapper.getClientMap().clear();
    }

    private void closeClient(AutoCloseable closeAction) {
        try {
            closeAction.close();
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.error("Cannot close SDK client", e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T getClient(SDKTypeEnum clientTypeEnum, String uniqueKey) {
        ClientWrapper wrapper = clientMap.get(uniqueKey);
        if (wrapper == null || !wrapper.getClientMap().containsKey(clientTypeEnum)) {
            throw new IllegalStateException("No " + clientTypeEnum + " client registered for " + uniqueKey);
        }
        return (T) wrapper.getClientMap().get(clientTypeEnum);
    }

    public ClientWrapper getClientWrapper(String uniqueKey) {
        return clientMap.get(uniqueKey);
    }

    /**
     * TODO
     *  此方法只提供 core 直接调用，如果是 console 调用，需要console 在封装一层。
     *  是否要 缓存 AbstractClientInfo 对象。但 cluster or runtime 卸载的时候需要删除.
     *  直接提供 console 的是否需要一个代理层
     */
    @SuppressWarnings("unchecked")
    public synchronized <T> T createAbstractClientInfo(Class<?> clazz, BaseSyncBase baseSyncBase) {
        try {
            String unique = baseSyncBase.getUnique();
            if (!baseSyncBase.isCluster() && ClusterSyncMetadataEnum.getClusterFramework(baseSyncBase.getClusterType()).isCAP()) {
                unique = ((RuntimeMetadata) baseSyncBase).clusterUnique();
            }
            Map<Class<?>, AbstractClientInfo<Object>> classMap =
                this.stringMapConcurrentHashMap.computeIfAbsent(unique, k -> new ConcurrentHashMap<>());
            if (classMap.containsKey(clazz)) {
                return (T) classMap.get(clazz);
            }

            AbstractClientInfo<Object> abstractClientInfo = (AbstractClientInfo<Object>) clazz.newInstance();
            ClientWrapper wrapper = clientMap.get(unique);
            if (wrapper == null) {
                throw new IllegalStateException("No SDK clients registered for " + unique);
            }
            abstractClientInfo.setClientWrapper(wrapper);
            classMap.put(clazz, abstractClientInfo);
            return (T) abstractClientInfo;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Class<?> getConfig(ClusterType clusterType, SDKTypeEnum sdkTypeEnum) {
        return CLUSTER_TYPE_MAP_CONCURRENT_HASH_MAP.get(clusterType).get(sdkTypeEnum).createSDKConfigClass;
    }


    static class SDKMetadataWrapper {

        private SDKMetadata sdkMetadata;

        private Class<?> createSDKConfigClass;

        private AbstractSDKOperation<Object, CreateSDKConfig> abstractSDKOperation;

    }
}
