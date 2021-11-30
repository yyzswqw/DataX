package com.alibaba.datax.plugin.util;

import com.alibaba.datax.plugin.KeyConstant;
import com.alibaba.datax.plugin.domain.IdentityItem;
import com.sensorsdata.analytics.javasdk.SensorsAnalytics;
import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class ProfileUtil {

    public static void process(SensorsAnalytics sa, Map<String, Object> properties) {
        String userDistinctId = (String) properties.get(KeyConstant.USER_DISTINCT_ID);
        Boolean userIsLoginId = Boolean.parseBoolean(properties.get(KeyConstant.user_is_login_id).toString());

        String distinctId = String.valueOf(properties.get(userDistinctId));

        properties.remove(userDistinctId);
        properties.remove(KeyConstant.USER_DISTINCT_ID);
        properties.remove(KeyConstant.user_is_login_id);
        try {
            sa.profileSet(distinctId, userIsLoginId, properties);
        } catch (Exception e) {
            log.error("user Profile Exception: {}", e);
            e.printStackTrace();
        }
    }

    public static void process(SensorsAnalytics sa, Map<String, Object> properties, List<IdentityItem> identityList,boolean isUnBind) {
        SensorsAnalyticsIdentity.Builder builder = SensorsAnalyticsIdentity.builder();
        for (IdentityItem identityItem : identityList) {
            if(identityItem.isColumn()){
                Object value = properties.getOrDefault(identityItem.getColumn(), null);
                if(!NullUtil.isNullOrBlank(value)){
                    builder.addIdentityProperty(identityItem.getIdName(),value.toString());
                }
            }else{
                builder.addIdentityProperty(identityItem.getIdName(),identityItem.getColumn());
            }
        }
        identityList.forEach(i->properties.remove(i.getColumn()));
        SensorsAnalyticsIdentity identity = builder.build();
        try {
            if(isUnBind){
                sa.unbind(identity);
            }else{
                sa.profileSetById(identity, properties);
            }
        } catch (Exception e) {
            log.error("user Profile Exception: {}", e);
            e.printStackTrace();
        }
    }


}
