package com.alibaba.datax.plugin.converter;

import cn.hutool.core.lang.Snowflake;
import cn.hutool.core.util.IdUtil;
import com.alibaba.datax.plugin.Converter;
import com.alibaba.datax.plugin.util.NullUtil;

import java.util.Map;
import java.util.Objects;

public class IdConverter implements Converter {

    private static final String TYPE = "type";
    private static final String WORK_ID = "workId";
    private static final String DATA_CENTER_ID = "dataCenterId";

    private static final String TYPE_SNOWFLAKE_NUM = "snowflake_num";
    private static final String TYPE_SNOWFLAKE_STR = "snowflake_str";
    private static final String TYPE_UUID = "uuid";
    private static final String TYPE_UUID_SEPARATOR = "uuid_no_separator";

    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param, Map<String,Object> resolvedValues) {
        Integer workId = (Integer)param.getOrDefault(WORK_ID, 0);
        Integer dataCenterId = (Integer)param.getOrDefault(DATA_CENTER_ID, 0);
        String type = (String)param.getOrDefault(TYPE, null);

        if(Objects.isNull(type) || NullUtil.isNullOrBlank(type) || TYPE_SNOWFLAKE_NUM.equalsIgnoreCase(type)){
            Snowflake snowflake = new Snowflake(workId, dataCenterId);
            return snowflake.nextId();
        }else if(TYPE_SNOWFLAKE_STR.equalsIgnoreCase(type)){
            Snowflake snowflake = new Snowflake(workId, dataCenterId);
            return snowflake.nextIdStr();
        }else if(TYPE_UUID.equalsIgnoreCase(type)){
            return IdUtil.fastUUID();
        }else if(TYPE_UUID_SEPARATOR.equalsIgnoreCase(type)){
            return IdUtil.fastSimpleUUID();
        }
        Snowflake snowflake = new Snowflake(workId, dataCenterId);
        return snowflake.nextId();
    }
}
