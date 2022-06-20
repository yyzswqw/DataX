package com.alibaba.datax.plugin.util;

import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

@Slf4j
public class StatisticsUtil {

    private static final String DEFAULT_NULL_COUNT_COLUMN_NAME = "defaultNullCountColumnName";

    /**
     * 统计列空值数
     */
    private static final Map<String, LongAdder> NULL_COUNT_MAP = new ConcurrentHashMap<>();

    /**
     * 统计ifNUllGiveUp配置过滤的列数量
     */
    private static final Map<String, AtomicLong> NULL_GIVE_UP_COUNT_MAP = new ConcurrentHashMap<>();

    /**
     * 统计ifNUllGiveUp配置过滤的总数量
     */
    private static final AtomicLong NULL_GIVE_UP_TOTAL_COUNT = new AtomicLong();

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

    public static Map<String, AtomicLong> getAllNullGiveUpCountColumn(){
        return NULL_GIVE_UP_COUNT_MAP;
    }

    public static void addNullGiveUpCountColumn(String columnName){
        if(StrUtil.isBlank(columnName)){
            columnName = DEFAULT_NULL_COUNT_COLUMN_NAME;
        }
        NULL_GIVE_UP_COUNT_MAP.put(columnName,new AtomicLong());
    }

    public static void addNullGiveUpCountByColumn(String columnName,long incr){
        if(StrUtil.isBlank(columnName)){
            columnName = DEFAULT_NULL_COUNT_COLUMN_NAME;
        }
        AtomicLong atomicLong = NULL_GIVE_UP_COUNT_MAP.get(columnName);
        if(Objects.isNull(atomicLong)){
            return;
        }
        NULL_GIVE_UP_COUNT_MAP.get(columnName).addAndGet(incr);
    }

    public static AtomicLong getNullGiveUpTotalCount(){
        return NULL_GIVE_UP_TOTAL_COUNT;
    }

}
