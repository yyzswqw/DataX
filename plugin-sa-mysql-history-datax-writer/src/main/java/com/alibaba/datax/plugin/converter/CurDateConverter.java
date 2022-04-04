package com.alibaba.datax.plugin.converter;

import com.alibaba.datax.plugin.Converter;
import com.alibaba.datax.plugin.util.DateUtil;
import com.alibaba.datax.plugin.util.NullUtil;

import java.util.Date;
import java.util.Map;
import java.util.Objects;

public class CurDateConverter implements Converter {

    private static final String TYPE = "type";
    private static final String PATTERN = "pattern";
    private static final String BLACK = "";
    private static final String TYPE_DATE = "date";
    private static final String TYPE_DATE_STR = "date_str";
    private static final String TYPE_TIMESTAMP = "timestamp";
    private static final String TYPE_TIMESTAMP_STR = "timestamp_str";

    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param, Map<String, Object> resolvedValues) {
        Date curDate = new Date();
        String type = (String)param.getOrDefault(TYPE, null);
        String pattern = (String)param.getOrDefault(PATTERN, "yyyy-MM-dd HH:mm:ss");
        if(Objects.isNull(type) || NullUtil.isNullOrBlank(type) || TYPE_DATE.equalsIgnoreCase(type)){
            return curDate;
        }else if(TYPE_TIMESTAMP.equalsIgnoreCase(type)){
            return curDate.getTime();
        }else if(TYPE_TIMESTAMP_STR.equalsIgnoreCase(type)){
            return curDate.getTime()+BLACK;
        }else if(TYPE_DATE_STR.equalsIgnoreCase(type)){
            return DateUtil.date2Str(curDate, pattern);
        }
        return curDate;
    }

}
