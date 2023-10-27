/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.naming.core.v2.client.factory.impl;

import com.alibaba.nacos.naming.constants.ClientConstants;
import com.alibaba.nacos.naming.core.v2.client.ClientAttributes;
import com.alibaba.nacos.naming.core.v2.client.factory.ClientFactory;
import com.alibaba.nacos.naming.core.v2.client.impl.IpPortBasedClient;

import static com.alibaba.nacos.naming.constants.ClientConstants.REVISION;

/**
 * Client factory for ephemeral {@link IpPortBasedClient}.
 *
 * @author xiweng.yy
 */
public class EphemeralIpPortClientFactory implements ClientFactory<IpPortBasedClient> {
    
    @Override
    public String getType() {
        return ClientConstants.EPHEMERAL_IP_PORT;
    }
    
    @Override
    public IpPortBasedClient newClient(String clientId, ClientAttributes attributes) {
        long revision = attributes.getClientAttribute(REVISION, 0); // 获取客户端的reversion值
        return new IpPortBasedClient(clientId, true, revision);
    }
    
    @Override
    public IpPortBasedClient newSyncedClient(String clientId, ClientAttributes attributes) {
        long revision = attributes.getClientAttribute(REVISION, 0);
        return new IpPortBasedClient(clientId, true, revision);
    }
}
