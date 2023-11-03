package com.alibaba.datax.plugin.util;

import com.alibaba.datax.plugin.KeyConstant;
import com.alibaba.datax.plugin.domain.IdentityItem;
import com.sensorsdata.analytics.javasdk.SensorsAnalytics;
import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

@Slf4j
public class ProfileUtil {

    public static LongAdder IDENTITY_FILTER_COUNT = new LongAdder();

    public static LongAdder BIND_IDENTITY_SIZE_FILTER_COUNT = new LongAdder();
    public static LongAdder  IDENTITY_COUNT = new LongAdder();
    public static LongAdder  DISTINCT_ID_FILTER_COUNT = new LongAdder();
    public static LongAdder  DISTINCT_ID_COUNT = new LongAdder();
    public static LongAdder  ERROR_COUNT = new LongAdder();

    public static void process(SensorsAnalytics sa, Map<String, Object> properties) {
        String userDistinctId = (String) properties.get(KeyConstant.USER_DISTINCT_ID);
        Boolean userIsLoginId = Boolean.parseBoolean(properties.get(KeyConstant.user_is_login_id).toString());

        String distinctId = String.valueOf(properties.get(userDistinctId));

        properties.remove(userDistinctId);
        properties.remove(KeyConstant.USER_DISTINCT_ID);
        properties.remove(KeyConstant.user_is_login_id);
        if(NullUtil.isNullOrBlank(distinctId)){
            DISTINCT_ID_FILTER_COUNT.increment();
            return;
        }
        try {
            sa.profileSet(distinctId, userIsLoginId, properties);
            sa.flush();
            DISTINCT_ID_COUNT.increment();
            Set<Map.Entry<String, LongAdder>> entries = StatisticsUtil.getAllNullCountColumn().entrySet();
            for (Map.Entry<String, LongAdder> entry : entries) {
                String columnName = entry.getKey();
                LongAdder longAdder = entry.getValue();
                if (NullUtil.isNullOrBlank(properties.getOrDefault(columnName, null))) {
                    longAdder.increment();
                }
            }
        } catch (Exception e) {
            log.error("user Profile Exception:", e);
            ERROR_COUNT.increment();
            log.info("user Profile Exception happened! data：{}",properties);
        }
    }

    public static void processBind(SensorsAnalytics sa, Map<String, Object> properties, List<IdentityItem> identityList) {
        SensorsAnalyticsIdentity.Builder builder = SensorsAnalyticsIdentity.builder();
        //记录空值数
        int count = 0;
        for (IdentityItem identityItem : identityList) {
            if(identityItem.isColumn()){
                Object value = properties.getOrDefault(identityItem.getColumnName(), null);
                if(!NullUtil.isNullOrBlank(value)){
                    builder.addIdentityProperty(identityItem.getIdName(),value.toString());
                }else{
                    count++;
                }
            }else{
                if(!NullUtil.isNullOrBlank(identityItem.getColumnName())){
                    builder.addIdentityProperty(identityItem.getIdName(),identityItem.getColumnName());
                }else{
                    count++;
                }
            }
        }
        identityList.forEach(i->{
            if(i.isDistinctId()){
                properties.remove(i.getColumnName());
            }
        });
        SensorsAnalyticsIdentity identity = builder.build();
        Map<String, String> identityMap = identity.getIdentityMap();
        if(identityList.size() == count){
            IDENTITY_FILTER_COUNT.increment();
            return;
        }
        if(identityMap.size() < 2){
            BIND_IDENTITY_SIZE_FILTER_COUNT.increment();
            return;
        }
        try {
            sa.bind(identity);
            sa.flush();
            IDENTITY_COUNT.increment();
            Set<Map.Entry<String, LongAdder>> entries = StatisticsUtil.getAllNullCountColumn().entrySet();
            for (Map.Entry<String, LongAdder> entry : entries) {
                String columnName = entry.getKey();
                LongAdder longAdder = entry.getValue();
                if (NullUtil.isNullOrBlank(properties.getOrDefault(columnName, null))) {
                    longAdder.increment();
                }
            }
        } catch (Exception e) {
            log.error("user Profile Exception:", e);
            ERROR_COUNT.increment();
            log.info("user Profile happened! identity：{} ,data：{}",identityMap,properties);
        }
    }

    public static void process(SensorsAnalytics sa, Map<String, Object> properties, List<IdentityItem> identityList,boolean isUnBind) {
        SensorsAnalyticsIdentity.Builder builder = SensorsAnalyticsIdentity.builder();
        //记录空值数
        int count = 0;
        for (IdentityItem identityItem : identityList) {
            if(identityItem.isColumn()){
                Object value = properties.getOrDefault(identityItem.getColumnName(), null);
                if(!NullUtil.isNullOrBlank(value)){
                    builder.addIdentityProperty(identityItem.getIdName(),value.toString());
                }else{
                    count++;
                }
            }else{
                if(!NullUtil.isNullOrBlank(identityItem.getColumnName())){
                    builder.addIdentityProperty(identityItem.getIdName(),identityItem.getColumnName());
                }else{
                    count++;
                }
            }
        }
        identityList.forEach(i->{
            if(i.isDistinctId()){
                properties.remove(i.getColumnName());
            }
        });
        SensorsAnalyticsIdentity identity = builder.build();
        Map<String, String> identityMap = identity.getIdentityMap();
        if(identityList.size() == count){
            IDENTITY_FILTER_COUNT.increment();
            return;
        }
        try {
            if(isUnBind){
                sa.unbind(identity);
            }else{
                sa.profileSetById(identity, properties);
            }
            sa.flush();
            IDENTITY_COUNT.increment();
            Set<Map.Entry<String, LongAdder>> entries = StatisticsUtil.getAllNullCountColumn().entrySet();
            for (Map.Entry<String, LongAdder> entry : entries) {
                String columnName = entry.getKey();
                LongAdder longAdder = entry.getValue();
                if (NullUtil.isNullOrBlank(properties.getOrDefault(columnName, null))) {
                    longAdder.increment();
                }
            }
        } catch (Exception e) {
            log.error("user Profile Exception:", e);
            ERROR_COUNT.increment();
            log.info("user Profile happened! identity：{} ,data：{}",identityMap,properties);
        }
    }


}
