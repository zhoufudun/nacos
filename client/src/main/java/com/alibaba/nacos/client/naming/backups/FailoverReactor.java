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

package com.alibaba.nacos.client.naming.backups;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.client.naming.cache.ConcurrentDiskUtil;
import com.alibaba.nacos.client.naming.cache.DiskCache;
import com.alibaba.nacos.client.naming.cache.ServiceInfoHolder;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * Failover reactor.
 *
 * @author nkorange
 */
public class FailoverReactor implements Closeable {
    
    private static final String FAILOVER_DIR = "/failover";
    
    private static final String IS_FAILOVER_MODE = "1";
    
    private static final String NO_FAILOVER_MODE = "0";
    
    private static final String FAILOVER_MODE_PARAM = "failover-mode";
    
    private Map<String, ServiceInfo> serviceMap = new ConcurrentHashMap<>(); // C:\Users\Administrator\nacos\naming\public/failover目录下排除故障转移文件，key=读取内容例如：key=DEFAULT_GROUP@@ihuman-turl-service@@DEFAULT  value=服务具体信息转化为：ServiceInfo
    
    private final Map<String, String> switchParams = new ConcurrentHashMap<>(); // 故障转移开关本地缓存
    
    private static final long DAY_PERIOD_MINUTES = 24 * 60;
    
    private final String failoverDir; // C:\Users\Administrator\nacos\naming\public/failover
    
    private final ServiceInfoHolder serviceInfoHolder;
    
    private final ScheduledExecutorService executorService;
    
    public FailoverReactor(ServiceInfoHolder serviceInfoHolder, String cacheDir) {
        this.serviceInfoHolder = serviceInfoHolder;
        this.failoverDir = cacheDir + FAILOVER_DIR; // 故障转义本地目录：C:\Users\Administrator\nacos\naming\public/failover
        // init executorService
        this.executorService = new ScheduledThreadPoolExecutor(1, r -> {
            Thread thread = new Thread(r);
            thread.setDaemon(true);
            thread.setName("com.alibaba.nacos.naming.failover");  // 故障转移线程名称：com.alibaba.nacos.naming.failover
            return thread;
        });
        this.init();
    }
    
    /**
     * Init.
     */
    public void init() {
        
        executorService.scheduleWithFixedDelay(new SwitchRefresher(), 0L, 5000L, TimeUnit.MILLISECONDS);
        
        executorService.scheduleWithFixedDelay(new DiskFileWriter(), 30, DAY_PERIOD_MINUTES, TimeUnit.MINUTES);
        
        // backup file on startup if failover directory is empty.
        executorService.schedule(() -> {
            try {
                File cacheDir = new File(failoverDir);

                if (!cacheDir.exists() && !cacheDir.mkdirs()) {
                    throw new IllegalStateException("failed to create cache dir: " + failoverDir);
                }

                File[] files = cacheDir.listFiles();
                if (files == null || files.length <= 0) {
                    new DiskFileWriter().run(); // 10s之后将缓存的服务信息写本地文件
                }
            } catch (Throwable e) {
                NAMING_LOGGER.error("[NA] failed to backup file on startup.", e);
            }

        }, 10000L, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Add day.
     *
     * @param date start time
     * @param num  add day number
     * @return new date
     */
    public Date addDay(Date date, int num) {
        Calendar startDT = Calendar.getInstance();
        startDT.setTime(date);
        startDT.add(Calendar.DAY_OF_MONTH, num);
        return startDT.getTime();
    }
    
    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        NAMING_LOGGER.info("{} do shutdown begin", className);
        ThreadUtils.shutdownThreadPool(executorService, NAMING_LOGGER);
        NAMING_LOGGER.info("{} do shutdown stop", className);
    }
    
    class SwitchRefresher implements Runnable {  // 故障转移文件对应的刷新线程，5s执行一次
        
        long lastModifiedMillis = 0L;
        
        @Override
        public void run() {
            try {
                File switchFile = new File(failoverDir + UtilAndComs.FAILOVER_SWITCH); // 故障转移本地文件开关，文件名表示一个开关器, 文件名：C:\Users\Administrator\nacos\naming\public/failover/00-00---000-VIPSRV_FAILOVER_SWITCH-000---00-00
                if (!switchFile.exists()) {
                    switchParams.put(FAILOVER_MODE_PARAM, Boolean.FALSE.toString());
                    NAMING_LOGGER.debug("failover switch is not found, {}", switchFile.getName());
                    return;
                }
                
                long modified = switchFile.lastModified();
                
                if (lastModifiedMillis < modified) {
                    lastModifiedMillis = modified;
                    String failover = ConcurrentDiskUtil.getFileContent(failoverDir + UtilAndComs.FAILOVER_SWITCH,
                            Charset.defaultCharset().toString());
                    if (!StringUtils.isEmpty(failover)) {
                        String[] lines = failover.split(DiskCache.getLineSeparator());
                        
                        for (String line : lines) {
                            String line1 = line.trim();
                            if (IS_FAILOVER_MODE.equals(line1)) {   // 故障转移本地文件内容：1（表示开启故障转移）或者0（表示不开启故障转移）
                                switchParams.put(FAILOVER_MODE_PARAM, Boolean.TRUE.toString());
                                NAMING_LOGGER.info("failover-mode is on");
                                new FailoverFileReader().run(); // 读取C:\Users\Administrator\nacos\naming\public/failover下所有文件，除了故障转移文件不读取。读取内容例如：key=DEFAULT_GROUP@@ihuman-turl-service@@DEFAULT  value=服务具体信息转化为：ServiceInfo
                            } else if (NO_FAILOVER_MODE.equals(line1)) {
                                switchParams.put(FAILOVER_MODE_PARAM, Boolean.FALSE.toString());
                                NAMING_LOGGER.info("failover-mode is off");
                            }
                        }
                    } else {
                        switchParams.put(FAILOVER_MODE_PARAM, Boolean.FALSE.toString());
                    }
                }
                
            } catch (Throwable e) {
                NAMING_LOGGER.error("[NA] failed to read failover switch.", e);
            }
        }
    }
    
    class FailoverFileReader implements Runnable {
        
        @Override
        public void run() {
            Map<String, ServiceInfo> domMap = new HashMap<>(16);
            
            BufferedReader reader = null;
            try {
                
                File cacheDir = new File(failoverDir);
                if (!cacheDir.exists() && !cacheDir.mkdirs()) {
                    throw new IllegalStateException("failed to create cache dir: " + failoverDir);
                }
                
                File[] files = cacheDir.listFiles();
                if (files == null) {
                    return;
                }
                
                for (File file : files) {
                    if (!file.isFile()) {
                        continue;
                    }
                    
                    if (file.getName().equals(UtilAndComs.FAILOVER_SWITCH)) {  // 排除故障转移文件
                        continue;
                    }
                    
                    ServiceInfo dom = new ServiceInfo(file.getName());
                    
                    try {
                        String dataString = ConcurrentDiskUtil
                                .getFileContent(file, Charset.defaultCharset().toString());
                        reader = new BufferedReader(new StringReader(dataString));
                        
                        String json;
                        if ((json = reader.readLine()) != null) {
                            try {
                                dom = JacksonUtils.toObj(json, ServiceInfo.class);
                            } catch (Exception e) {
                                NAMING_LOGGER.error("[NA] error while parsing cached dom : {}", json, e);
                            }
                        }
                        
                    } catch (Exception e) {
                        NAMING_LOGGER.error("[NA] failed to read cache for dom: {}", file.getName(), e);
                    } finally {
                        try {
                            if (reader != null) {
                                reader.close();
                            }
                        } catch (Exception e) {
                            //ignore
                        }
                    }
                    if (!CollectionUtils.isEmpty(dom.getHosts())) {
                        domMap.put(dom.getKey(), dom);
                    }
                }
            } catch (Exception e) {
                NAMING_LOGGER.error("[NA] failed to read cache file", e);
            }
            
            if (domMap.size() > 0) {
                serviceMap = domMap;
            }
        }
    }
    
    class DiskFileWriter extends TimerTask {  // 1440min（一天）执行一次
        
        @Override
        public void run() {
            Map<String, ServiceInfo> map = serviceInfoHolder.getServiceInfoMap();
            for (Map.Entry<String, ServiceInfo> entry : map.entrySet()) {
                ServiceInfo serviceInfo = entry.getValue();
                if (StringUtils.equals(serviceInfo.getKey(), UtilAndComs.ALL_IPS) || StringUtils
                        .equals(serviceInfo.getName(), UtilAndComs.ENV_LIST_KEY) || StringUtils
                        .equals(serviceInfo.getName(), UtilAndComs.ENV_CONFIGS) || StringUtils
                        .equals(serviceInfo.getName(), UtilAndComs.VIP_CLIENT_FILE) || StringUtils
                        .equals(serviceInfo.getName(), UtilAndComs.ALL_HOSTS)) {
                    continue;
                }
                
                DiskCache.write(serviceInfo, failoverDir); // 服务信息写入本地故障转移目录下，文件名例如：DEFAULT_GROUP%40%40ihuman-turl-service@@DEFAULT， 内容：{"name":"DEFAULT_GROUP@@ihuman-turl-service","clusters":"DEFAULT","cacheMillis":10000,"hosts":[{"instanceId":"10.2.40.18#8765#DEFAULT#DEFAULT_GROUP@@ihuman-turl-service","ip":"10.2.40.18","port":8765,"weight":1.0,"healthy":true,"enabled":true,"ephemeral":true,"clusterName":"DEFAULT","serviceName":"DEFAULT_GROUP@@ihuman-turl-service","metadata":{"management.endpoints.web.base-path":"/insight","preserved.register.source":"SPRING_CLOUD","version":"1.0.0","management.context-path":"/insight"},"lastBeat":1696833589420,"marked":false,"app":"DEFAULT","instanceHeartBeatInterval":5000,"instanceHeartBeatTimeOut":15000,"ipDeleteTimeout":30000},{"instanceId":"10.22.79.184#8765#DEFAULT#DEFAULT_GROUP@@ihuman-turl-service","ip":"10.22.79.184","port":8765,"weight":1.0,"healthy":true,"enabled":true,"ephemeral":true,"clusterName":"DEFAULT","serviceName":"DEFAULT_GROUP@@ihuman-turl-service","metadata":{"management.endpoints.web.base-path":"/insight","preserved.register.source":"SPRING_CLOUD","version":"1.0.0","management.context-path":"/insight"},"lastBeat":1696833591159,"marked":false,"app":"DEFAULT","instanceHeartBeatInterval":5000,"instanceHeartBeatTimeOut":15000,"ipDeleteTimeout":30000}],"lastRefTime":1696833591968,"checksum":"3ef49619c0028482bb898d8114bc1a7f","allIPs":false,"reachProtectionThreshold":false,"valid":true}
            }
        }
    }
    
    public boolean isFailoverSwitch() {
        return Boolean.parseBoolean(switchParams.get(FAILOVER_MODE_PARAM));
    }
    
    public ServiceInfo getService(String key) {
        ServiceInfo serviceInfo = serviceMap.get(key);
        
        if (serviceInfo == null) {
            serviceInfo = new ServiceInfo();
            serviceInfo.setName(key);
        }
        
        return serviceInfo;
    }
}
