package com.alibaba.datax.plugin.converter;

import com.alibaba.datax.plugin.Converter;
import com.alibaba.datax.plugin.util.NullUtil;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class AutoIncrConverter implements Converter {

    private Map<String,LongAdder> incrMap = new ConcurrentHashMap<>();
    private static final String DEFAULT_COL_NAME = "default";
    private static final String INIT_VALUE = "initValue";

    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param, Map<String, Object> resolvedValues) {
        LongAdder longAdder = null;
        Long initValue = (Long)param.getOrDefault(INIT_VALUE, 0);
        String col = null;
        if(NullUtil.isNullOrBlank(targetColumnName)){
            longAdder = incrMap.get(DEFAULT_COL_NAME);
            col = DEFAULT_COL_NAME;
        }else{
            longAdder = incrMap.get(targetColumnName);
            col = targetColumnName;
        }
        if(Objects.isNull(longAdder)){
            longAdder = getLongAdder(col,initValue);
        }
        long l = longAdder.longValue();
        longAdder.increment();
        return l;
    }

    private synchronized LongAdder getLongAdder(String col,Long initValue) {
        LongAdder adder = incrMap.get(col);
        if(Objects.nonNull(adder)){
            return adder;
        }
        LongAdder longAdder = new LongAdder();
        longAdder.add(initValue);
        return longAdder;
    }
}
