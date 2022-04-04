package com.alibaba.datax.plugin.sharding;

import com.alibaba.datax.CustomizeProp;
import com.alibaba.datax.plugin.KeyConstant;
import com.alibaba.datax.plugin.util.DateUtil;
import com.google.common.collect.Range;
import org.apache.shardingsphere.api.sharding.complex.ComplexKeysShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.complex.ComplexKeysShardingValue;

import java.sql.Timestamp;
import java.time.*;
import java.util.*;

public class DefaultYearComplexKeysShardingAlgorithmImpl implements ComplexKeysShardingAlgorithm<Date> {

    private static String COLUMN;
    private static String tableName;
    private static String DELIMITER;
    private static int startYear;
    private static int endYear;
    private static ZoneId ZONE_ID = ZoneId.systemDefault();

    public DefaultYearComplexKeysShardingAlgorithmImpl(){
        Map<String,Object> customProp = (Map<String,Object>)CustomizeProp.get(KeyConstant.MYSQL_WRITER_CUSTOM_PROP);
        String col = (String)customProp.getOrDefault("defaultShardingCol","created_date");
        String delimiter = (String)customProp.getOrDefault("defaultShardingDelimiter","_");
        String tableName = (String)customProp.getOrDefault("defaultShardingTableName",null);
        int startYear = (int)customProp.getOrDefault("defaultShardingStartYear",1970);
        int endYear = (int)customProp.getOrDefault("defaultShardingEndYear",LocalDateTime.now().getYear());
        this.COLUMN = col;
        this.DELIMITER = delimiter;
        this.tableName = tableName;
        this.startYear = startYear;
        this.endYear = endYear;
    }

    @Override
    public Collection<String> doSharding(Collection collection, ComplexKeysShardingValue complexKeysShardingValue) {
        Set<String> set = new HashSet<>();
        String logicTableName;
        if(Objects.isNull(tableName) || "".equals(tableName.trim())){
            logicTableName = complexKeysShardingValue.getLogicTableName();
        }else{
            logicTableName = tableName;
        }

//        解析 = 和 in 查询
        Map<String, Collection<Object>> columnNameAndShardingValuesMap = complexKeysShardingValue.getColumnNameAndShardingValuesMap();
        Collection<Object> createDateList = columnNameAndShardingValuesMap.get(COLUMN);
        if(Objects.nonNull(createDateList) && !createDateList.isEmpty()){
            for (Object createDate : createDateList) {
                Date d = toDate(createDate);
                if(Objects.isNull(d)){
                    continue;
                }
                Instant instant = d.toInstant();
                LocalDateTime localDateTime = instant.atZone(ZONE_ID).toLocalDateTime();
                set.add(logicTableName+DELIMITER+localDateTime.getYear());
            }
        }
// 解析范围查询
        resolveRange(complexKeysShardingValue, set,logicTableName);
        //如果没有值，默认查当前年，不管怎样都查不到数据，如果直接返回空，sharding会报错的
        if(set.isEmpty()){
            set.add(logicTableName+DELIMITER+LocalDateTime.now().getYear());
        }
        return set;
    }

    private void resolveRange(ComplexKeysShardingValue<Comparable<Object>> complexKeysShardingValue, Set<String> set,String logicTableName) {
        Map<String, Range<Comparable<Object>>> columnNameAndRangeValuesMap = complexKeysShardingValue.getColumnNameAndRangeValuesMap();
        Range<Comparable<Object>> createDateRange = columnNameAndRangeValuesMap.get(COLUMN);
        if(Objects.nonNull(createDateRange)){
            Comparable<Object> lowerEndpoint = null;
            Comparable<Object> upperEndpoint = null;
            try {
                lowerEndpoint = createDateRange.lowerEndpoint();
            }catch (Exception e){
                //下界是无限的
            }
            try {
                upperEndpoint = createDateRange.upperEndpoint();
            }catch (Exception e){
                //上界是无限的
            }
            Date lowerDate = toDate(lowerEndpoint);
            Date upperDate = toDate(upperEndpoint);
            List<String> range = getRange(lowerDate, upperDate,logicTableName);
            if( !range.isEmpty()){
                set.addAll(range);
            }
        }
    }

    private Date toDate(Object date){
        Date d = null;
        if(Objects.isNull(date)){
            return d;
        }
        if(date instanceof Timestamp){
            d = (Date) date;
        }
        if(date instanceof Date){
            d = (Date) date;
        }
        if(date instanceof LocalDateTime){
            ZonedDateTime zonedDateTime = ((LocalDateTime)date).atZone(ZONE_ID);
            d = Date.from(zonedDateTime.toInstant());
        }
        if(date instanceof LocalDate){
            d = Date.from(((LocalDate)date).atStartOfDay().atZone(ZONE_ID).toInstant());
        }
        if(date instanceof String){
            d = DateUtil.str2Date((String)date);
        }
        return d;
    }


    private List<String> getRange(Date lowerDate,Date upperDate,String logicTableName){
        LocalDateTime lower = null;
        LocalDateTime upper = null;
        if(Objects.nonNull(lowerDate)){
            Instant instant = lowerDate.toInstant();
            lower = instant.atZone(ZONE_ID).toLocalDateTime();
        }
        if(Objects.nonNull(upperDate)){
            Instant instant = upperDate.toInstant();
            upper = instant.atZone(ZONE_ID).toLocalDateTime();
        }
        return getAllRange(lower,upper,logicTableName);
    }


    private static List<String> getAllRange(LocalDateTime lower, LocalDateTime upper,String logicTableName) {
        List<String> list = new ArrayList<>();
        if(Objects.isNull(lower) && Objects.isNull(upper)){
            return list;
        }
        if(Objects.isNull(lower)){
            return getLowerRange(upper,logicTableName);
        }
        if(Objects.isNull(upper)){
            return getUpperRange(lower,logicTableName);
        }
        LocalDateTime startDate = LocalDateTime.of(startYear,1,1,0,0,0);
        int lowerYear = Math.max(lower.getYear(), startDate.getYear());
        int upperYear = Math.min(upper.getYear(), endYear);
        LocalDateTime s = startDate;
        int startYear = s.getYear();
        while(startYear <= upperYear){
            if(startYear >= lowerYear){
                list.add(logicTableName+DELIMITER+startYear);
            }
            startYear += 1;
        }
        return list;
    }

    /**
     * 解析 >=
     * @param lower
     * @return
     */
    private static List<String> getUpperRange(LocalDateTime lower,String logicTableName) {
        List<String> list = new ArrayList<>();
        if(Objects.isNull(lower)){
            return list;
        }
        LocalDateTime s = LocalDateTime.of(startYear,1,1,0,0,0);
        lower = LocalDateTime.of(lower.getYear(),1,1,0,0,0);
        if(lower.compareTo(s) >= 0){
            s = lower;
        }
        LocalDateTime now = LocalDateTime.of(endYear,1,1,0,0,0);
        while (s.compareTo(now) <= 0){
            if(s.compareTo(lower) >= 0){
                int year = s.getYear();
                list.add(logicTableName+DELIMITER+year);
            }
            s = s.plusYears(1);
        }
        return list;
    }

    /**
     * 解析 <=
     * @param upper
     * @return
     */
    private static List<String> getLowerRange(LocalDateTime upper,String logicTableName) {
        List<String> list = new ArrayList<>();
        if(Objects.isNull(upper)){
            return list;
        }
        LocalDateTime s = LocalDateTime.of(startYear,1,1,0,0,0);
        LocalDateTime now = LocalDateTime.of(endYear,1,1,0,0,0);
        if(upper.compareTo(now) <= 0){
            now = upper;
        }
        while (s.compareTo(now) <= 0){
            int year = s.getYear();
            list.add(logicTableName+DELIMITER+year);
            s = s.plusYears(1);
        }
        return list;
    }
}
