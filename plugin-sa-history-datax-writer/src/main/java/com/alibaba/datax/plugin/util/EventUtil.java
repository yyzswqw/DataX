package com.alibaba.datax.plugin.util;

import com.alibaba.datax.plugin.KeyConstant;
import com.alibaba.datax.plugin.domain.IdentityItem;
import com.sensorsdata.analytics.javasdk.SensorsAnalytics;
import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class EventUtil {

    public static void process(SensorsAnalytics sa, Map<String, Object> properties) {
        String eventDistinctIdCol = (String) properties.get(KeyConstant.EVENT_DISTINCT_ID_COL);
        Boolean eventIsLoginId = Boolean.parseBoolean(properties.get(KeyConstant.EVENT_IS_LOGIN_ID).toString());
        String eventEventName = (String) properties.get(KeyConstant.EVENT_EVENT_NAME);

        String distinctId = String.valueOf(properties.get(eventDistinctIdCol));
        properties.remove(eventDistinctIdCol);

        try {
            properties.remove(KeyConstant.EVENT_DISTINCT_ID_COL);
            properties.remove(KeyConstant.EVENT_IS_LOGIN_ID);
            properties.remove(KeyConstant.EVENT_EVENT_NAME);
            sa.track(distinctId, eventIsLoginId, eventEventName, properties);
            sa.flush();
        } catch (Exception e) {
            log.error("Event Exception: {}", e);
            e.printStackTrace();
        }
    }

    public static void process(SensorsAnalytics sa, Map<String, Object> properties, List<IdentityItem> identityList) {
        String eventEventName = (String) properties.get(KeyConstant.EVENT_EVENT_NAME);
        SensorsAnalyticsIdentity.Builder builder = SensorsAnalyticsIdentity.builder();
        int count = 0;
        for (IdentityItem identityItem : identityList) {
            if(identityItem.isColumn()){
                Object value = properties.getOrDefault(identityItem.getColumn(), null);
                if(!NullUtil.isNullOrBlank(value)){
                    builder.addIdentityProperty(identityItem.getIdName(),value.toString());
                }else{
                    //记录空值数
                    count++;
                }
            }else{
                if(!NullUtil.isNullOrBlank(identityItem.getColumn())){
                    builder.addIdentityProperty(identityItem.getIdName(),identityItem.getColumn());
                }else{
                    //记录空值数
                    count++;
                }
            }
        }
        identityList.forEach(i->properties.remove(i.getColumn()));
        SensorsAnalyticsIdentity identity = builder.build();
        Map<String, String> identityMap = identity.getIdentityMap();
        if(Objects.isNull(identityMap) || identityMap.isEmpty() || identityList.size() == count){
            return;
        }
        try {
            properties.remove(KeyConstant.EVENT_DISTINCT_ID_COL);
            properties.remove(KeyConstant.EVENT_IS_LOGIN_ID);
            properties.remove(KeyConstant.EVENT_EVENT_NAME);
            sa.trackById(identity,eventEventName, properties);
            sa.flush();
        } catch (Exception e) {
            log.error("Event Exception: {}", e);
            e.printStackTrace();
        }
    }
}
