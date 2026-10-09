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

import org.apache.eventmesh.dashboard.core.function.SDK.config.AbstractMultiCreateSDKConfig;
import org.apache.eventmesh.dashboard.core.function.SDK.config.NetAddress;
import org.junit.Test;


import static org.junit.Assert.assertArrayEquals;

public class StatusUnitBoundaryTest {
    @Test
    public void multiAddressConfigDeduplicatesAndRebuildsAddressArrayAfterRemoval() {
        AbstractMultiCreateSDKConfig config = new AbstractMultiCreateSDKConfig();
        NetAddress first = NetAddress.create("broker-a", 9092);
        NetAddress second = NetAddress.create("broker-b", 9092);
        config.addNetAddress(first);
        config.addNetAddress(NetAddress.create("broker-a", 9092));
        config.addNetAddress(second);
        config.addMetaAddress(first);
        config.addMetaAddress(NetAddress.create("broker-a", 9092));
        assertArrayEquals(new String[] {"broker-a:9092", "broker-b:9092"}, config.getNetAddresses());
        assertArrayEquals(new String[] {"broker-a:9092"}, config.getNetAddressesByMeta());

        config.removeNetAddress(first);
        assertArrayEquals(new String[] {"broker-b:9092"}, config.getNetAddresses());
    }

}
