/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.naming.core;

import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.core.cluster.MemberChangeListener;
import com.alibaba.nacos.core.cluster.MemberUtil;
import com.alibaba.nacos.core.cluster.MembersChangeEvent;
import com.alibaba.nacos.core.cluster.NodeState;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.sys.env.EnvUtil;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Distro mapper, judge which server response input service.
 *
 * @author nkorange
 */
@Component("distroMapper")
public class DistroMapper extends MemberChangeListener {
    
    /**
     * List of service nodes, you must ensure that the order of healthyList is the same for all nodes.
     */
    private volatile List<String> healthyList = new ArrayList<>();
    
    private final SwitchDomain switchDomain; // {"adWeightMap":{},"defaultPushCacheMillis":10000,"clientBeatInterval":5000,"defaultCacheMillis":3000,"distroThreshold":0.7,"healthCheckEnabled":true,"autoChangeHealthCheckEnabled":false,"distroEnabled":true,"enableStandalone":true,"pushEnabled":true,"checkTimes":3,"httpHealthParams":{"max":5000,"min":500,"factor":0.85},"tcpHealthParams":{"max":5000,"min":1000,"factor":0.75},"mysqlHealthParams":{"max":3000,"min":2000,"factor":0.65},"incrementalList":[],"serverStatusSynchronizationPeriodMillis":2000,"serviceStatusSynchronizationPeriodMillis":5000,"disableAddIP":false,"sendBeatOnly":false,"lightBeatEnabled":true,"doubleWriteEnabled":true,"limitedUrlMap":{},"distroServerExpiredMillis":10000,"pushGoVersion":"0.1.0","pushJavaVersion":"0.1.0","pushPythonVersion":"0.4.3","pushCVersion":"1.0.12","pushCSharpVersion":"0.9.0","enableAuthentication":false,"overriddenServerStatus":"UP","defaultInstanceEphemeral":true,"healthCheckWhiteList":[],"name":"00-00---000-NACOS_SWITCH_DOMAIN-000---00-00"}
    
    private final ServerMemberManager memberManager; // 管理集群中的所有Nacos节点
    
    public DistroMapper(ServerMemberManager memberManager, SwitchDomain switchDomain) {
        this.memberManager = memberManager;
        this.switchDomain = switchDomain;
    }
    
    public List<String> getHealthyList() {
        return healthyList;
    }
    
    /**
     * init server list.
     */
    @PostConstruct
    public void init() {
        NotifyCenter.registerSubscriber(this);
        this.healthyList = MemberUtil.simpleMembers(memberManager.allMembers());
    }
    
    public boolean responsible(Cluster cluster, Instance instance) {
        return switchDomain.isHealthCheckEnabled(cluster.getServiceName()) && !cluster.getHealthCheckTask()
                .isCancelled() && responsible(cluster.getServiceName()) && cluster.contains(instance);
    }
    
    /**
     * Judge whether current server is responsible for input tag.
     *
     * @param responsibleTag responsible tag, serviceName for v1 and ip:port for v2
     * @return true if input service is response, otherwise false
     */
    public boolean responsible(String responsibleTag) { // 127.0.0.1:8081
        final List<String> servers = healthyList;
        
        if (!switchDomain.isDistroEnabled() || EnvUtil.getStandaloneMode()) {
            return true;
        }
        
        if (CollectionUtils.isEmpty(servers)) {
            // means distro config is not ready yet
            return false;
        }
        
        String localAddress = EnvUtil.getLocalAddress();
        int index = servers.indexOf(localAddress);
        int lastIndex = servers.lastIndexOf(localAddress);
        if (lastIndex < 0 || index < 0) {
            return true;
        }
        
        int target = distroHash(responsibleTag) % servers.size();
        return target >= index && target <= lastIndex;
    }
    
    /**
     * Calculate which other server response input tag.
     *
     * @param responsibleTag responsible tag, serviceName for v1 and ip:port for v2
     * @return server which response input service
     */
    public String mapSrv(String responsibleTag) {
        final List<String> servers = healthyList;
        
        if (CollectionUtils.isEmpty(servers) || !switchDomain.isDistroEnabled()) {
            return EnvUtil.getLocalAddress();
        }
        
        try {
            int index = distroHash(responsibleTag) % servers.size();
            return servers.get(index);
        } catch (Throwable e) {
            Loggers.SRV_LOG
                    .warn("[NACOS-DISTRO] distro mapper failed, return localhost: " + EnvUtil.getLocalAddress(), e);
            return EnvUtil.getLocalAddress();
        }
    }
    
    private int distroHash(String responsibleTag) {
        return Math.abs(responsibleTag.hashCode() % Integer.MAX_VALUE);
    }
    
    @Override
    public void onEvent(MembersChangeEvent event) {
        // Here, the node list must be sorted to ensure that all nacos-server's
        // node list is in the same order
        List<String> list = MemberUtil.simpleMembers(MemberUtil.selectTargetMembers(event.getMembers(),
                member -> NodeState.UP.equals(member.getState()) || NodeState.SUSPICIOUS.equals(member.getState())));
        Collections.sort(list);
        Collection<String> old = healthyList;
        healthyList = Collections.unmodifiableList(list);
        Loggers.SRV_LOG.info("[NACOS-DISTRO] healthy server list changed, old: {}, new: {}", old, healthyList);
    }
    
    @Override
    public boolean ignoreExpireEvent() {
        return true;
    }
}
