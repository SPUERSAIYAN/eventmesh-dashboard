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


package org.apache.eventmesh.dashboard.common.model.remoting.kafka.config;

import org.apache.eventmesh.dashboard.common.model.remoting.Global2Request;

import java.util.Map;

import lombok.Data;
import lombok.EqualsAndHashCode;

/** Explicit resource scope; settings are incremental SET operations, omitted keys are preserved. */
@Data
@EqualsAndHashCode(callSuper = true)
public class ConfigRequest extends Global2Request {

    public enum Scope {
        TOPIC, BROKER, DEFAULT_BROKER
    }

    private Scope scope;
    /** Topic name for TOPIC, numeric broker ID for BROKER, null for DEFAULT_BROKER. */
    private String resourceName;
    private Map<String, String> configs;
}
