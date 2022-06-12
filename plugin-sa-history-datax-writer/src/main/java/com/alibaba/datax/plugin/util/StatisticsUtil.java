package com.alibaba.datax.plugin.util;

import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

@Slf4j
public class StatisticsUtil {

    private static final String DEFAULT_NULL_COUNT_COLUMN_NAME = "defaultNullCountColumnName";

    private static final Map<String, LongAdder> NULL_COUNT_MAP = new ConcurrentHashMap<>();

    private StatisticsUtil(){}

    public static void addNullCountColumn(String columnName){
        if(StrUtil.isBlank(columnName)){
            columnName = DEFAULT_NULL_COUNT_COLUMN_NAME;
        }
        NULL_COUNT_MAP.put(columnName,new LongAdder());
    }

    public static Map<String, LongAdder> getAllNullCountColumn(){
        return NULL_COUNT_MAP;
    }

}
