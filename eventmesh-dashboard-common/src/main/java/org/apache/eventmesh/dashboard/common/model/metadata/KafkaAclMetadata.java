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


package org.apache.eventmesh.dashboard.common.model.metadata;

/**
 * Kafka ACL binding using the shared ACL fields. Operation and patternType use Kafka protocol codes;
 * resourceType and permissionType use enum names. These RPC fields have no new persistence mapping.
 */
public class KafkaAclMetadata extends AclMetadata {

    @Override
    public String nodeUnique() {
        return this.part(this.getResourceType()) + this.part(this.getResourceName()) + this.part(this.getPatternType())
            + this.part(this.getPrincipal()) + this.part(this.getHost()) + this.part(this.getOperation()) + this.part(this.getPermissionType());
    }

    private String part(Object value) {
        return value == null ? "-1:" : value.toString().length() + ":" + value;
    }
}
