package com.alibaba.datax.plugin.util;

import cn.hutool.core.util.StrUtil;
import com.alibaba.datax.plugin.KeyConstant;
import com.alibaba.datax.plugin.domain.IdentityItem;
import com.sensorsdata.analytics.javasdk.SensorsAnalytics;
import com.sensorsdata.analytics.javasdk.consumer.BatchConsumer;
import com.sensorsdata.analytics.javasdk.consumer.ConcurrentLoggingConsumer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class SaUtil {

    /**
     * 若生成json文件则该参数为文件地址，否则是神策系统接收接口地址
     */
    private static String sdkDataAddress;
    /**
     * 该参数确定是否要生成神策json文件，否则直接通过神策系统接收接口地址传输到神策系统,如：http://localhost:8106/sa?project=default
     */
    private static boolean isGenerateLog = true;

    private static boolean enableTimeFree = false;

    private static volatile SensorsAnalytics instance;

    public static void setSdkDataAddress(String sdkDataAddress) {
        SaUtil.sdkDataAddress = sdkDataAddress;
    }

    public static void setEnableTimeFree(Boolean enableTimeFree) {
        if(Objects.nonNull(enableTimeFree)){
            SaUtil.enableTimeFree = enableTimeFree;
        }
    }

    public static void setIsGenerateLog(boolean isGenerateLog) {
        SaUtil.isGenerateLog = isGenerateLog;
    }

    /**
     * 初始化sa实体
     *
     * @return
     * @throws IOException
     */
    public static SensorsAnalytics getInstance() {
        if (instance == null) {
            synchronized (SaUtil.class) {
                if (instance == null) {
                    try {
                        if(isGenerateLog){
                            instance = new SensorsAnalytics(new ConcurrentLoggingConsumer(sdkDataAddress));
                        }else{
                            instance = new SensorsAnalytics(new BatchConsumer(sdkDataAddress));
                        }
                        instance.setEnableTimeFree(enableTimeFree);
                        log.info("SensorsAnalytics初始化完成,isGenerateLog:{}",isGenerateLog);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return instance;
                }
            }
        }
        return instance;
    }

    public static void process(SensorsAnalytics sa, String type, Map<String, Object> map) {
        if (StrUtil.isBlank(type)) {
            log.info("sa类型不能为空");
            return;
        }
        if (KeyConstant.TRACK.equalsIgnoreCase(type)) {
            EventUtil.process(sa, map);
        } else if (KeyConstant.USER.equalsIgnoreCase(type)) {
            ProfileUtil.process(sa, map);
        } else if (KeyConstant.ITEM.equalsIgnoreCase(type)) {
            ItemSetUtil.process(sa, map);
        } else if (KeyConstant.ITEM_DELETE.equalsIgnoreCase(type)) {
            ItemDeleteUtil.process(sa, map);
        } else {
            log.info("sa类型错误,type:{}", type);
        }

    }

    public static void process(SensorsAnalytics sa, String type, Map<String, Object> map, List<IdentityItem> identityList,boolean isUnBind,boolean isBind) {
        if (StrUtil.isBlank(type)) {
            log.info("sa类型不能为空");
            return;
        }
        if (KeyConstant.TRACK.equalsIgnoreCase(type)) {
            EventUtil.process(sa, map,identityList);
        } else if (KeyConstant.USER.equalsIgnoreCase(type)) {
            if(isBind){
                ProfileUtil.processBind(sa, map,identityList);
            }else{
                ProfileUtil.process(sa, map,identityList,isUnBind);
            }
        } else if (KeyConstant.ITEM.equalsIgnoreCase(type)) {
            ItemSetUtil.process(sa, map);
        } else if (KeyConstant.ITEM_DELETE.equalsIgnoreCase(type)) {
            ItemDeleteUtil.process(sa, map);
        } else {
            log.info("sa类型错误,type:{}", type);
        }

    }


}
