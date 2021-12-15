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
public class ProfileUtil {

    public static void process(SensorsAnalytics sa, Map<String, Object> properties) {
        String userDistinctId = (String) properties.get(KeyConstant.USER_DISTINCT_ID);
        Boolean userIsLoginId = Boolean.parseBoolean(properties.get(KeyConstant.user_is_login_id).toString());

        String distinctId = String.valueOf(properties.get(userDistinctId));

        properties.remove(userDistinctId);
        properties.remove(KeyConstant.USER_DISTINCT_ID);
        properties.remove(KeyConstant.user_is_login_id);
        if(NullUtil.isNullOrBlank(distinctId)){
            return;
        }
        try {
            sa.profileSet(distinctId, userIsLoginId, properties);
            sa.flush();
        } catch (Exception e) {
            log.error("user Profile Exception: {}", e);
            e.printStackTrace();
        }
    }

    public static void process(SensorsAnalytics sa, Map<String, Object> properties, List<IdentityItem> identityList,boolean isUnBind) {
        SensorsAnalyticsIdentity.Builder builder = SensorsAnalyticsIdentity.builder();
        //记录空值数
        int count = 0;
        for (IdentityItem identityItem : identityList) {
            if(identityItem.isColumn()){
                Object value = properties.getOrDefault(identityItem.getColumn(), null);
                if(!NullUtil.isNullOrBlank(value)){
                    builder.addIdentityProperty(identityItem.getIdName(),value.toString());
                }else{
                    count++;
                }
            }else{
                if(!NullUtil.isNullOrBlank(identityItem.getColumn())){
                    builder.addIdentityProperty(identityItem.getIdName(),identityItem.getColumn());
                }else{
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
            if(isUnBind){
                sa.unbind(identity);
            }else{
                sa.profileSetById(identity, properties);
            }
            sa.flush();
        } catch (Exception e) {
            log.error("user Profile Exception: {}", e);
            e.printStackTrace();
        }
    }


}
