package com.alibaba.datax.plugin.writer;

import com.alibaba.datax.BasePlugin;
import com.alibaba.datax.plugin.domain.IdentityItem;
import com.alibaba.datax.plugin.domain.SaPlugin;
import com.alibaba.datax.plugin.util.*;
import cn.hutool.core.util.StrUtil;
import com.alibaba.datax.plugin.ConverterFactory;
import com.alibaba.datax.plugin.KeyConstant;
import com.alibaba.datax.plugin.classloader.PluginClassLoader;
import com.alibaba.datax.plugin.domain.DataConverter;
import com.alibaba.datax.plugin.domain.SaColumnItem;
import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sensorsdata.analytics.javasdk.SensorsAnalytics;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class SaWriter extends Writer {

    @Slf4j
    public static class Job extends Writer.Job{

        private Configuration originalConfig = null;
        private String type;
        private Boolean useIDM3;

        public List<Configuration> split(int i) {
            List<Configuration> list = new ArrayList<>();
            for (int j = 0; j < i; j++) {
                list.add(this.originalConfig.clone());
            }
            return list;
        }

        public void init() {
            this.originalConfig = super.getPluginJobConf();
            String sdkDataAddress = originalConfig.getString(KeyConstant.SDK_DATA_ADDRESS);
            Boolean useIDM3 = originalConfig.getBool(KeyConstant.USE_IDM3,true);
            this.useIDM3 = useIDM3;
            if(StrUtil.isBlank(sdkDataAddress)){
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"sdkDataAddress不能为空");
            }
            SaUtil.setSdkDataAddress(sdkDataAddress);
            try {
                boolean isGenerateLog = originalConfig.getBool(KeyConstant.IS_GENERATE_LOG);
                if(!Objects.isNull(isGenerateLog)){
                    SaUtil.setIsGenerateLog(isGenerateLog);
                }
            }catch (Exception e){
                log.info("isGenerateLog未配置，使用默认值：true");
            }

            String type = originalConfig.getString(KeyConstant.TYPE);
            this.type = type;
            JSONArray saColumnJsonArray = originalConfig.get(KeyConstant.SA_COLUMN, JSONArray.class);
            if(Objects.isNull(saColumnJsonArray)){
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"column不应该为空！");
            }
            String saColumnStr = saColumnJsonArray.toJSONString();
            List<SaColumnItem> saColumnList = JSONObject.parseArray(saColumnStr, SaColumnItem.class);
            if(Objects.isNull(saColumnList) || saColumnList.isEmpty()){
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"column不应该为空！");
            }
            List<String> saColumnNameList = saColumnList.stream().map(SaColumnItem::getName).collect(Collectors.toList());
            if(KeyConstant.TRACK.equalsIgnoreCase(type)){
                String distinctId = originalConfig.getString(KeyConstant.TRACK.concat(KeyConstant.POINT).concat(KeyConstant.DISTINCT_ID_COLUMN),"");

                String eventName = originalConfig.getString(KeyConstant.TRACK.concat(KeyConstant.POINT).concat(KeyConstant.EVENT_NAME));
                if(StrUtil.isBlank(eventName)){
                    throw new DataXException(CommonErrorCode.CONFIG_ERROR,"type为:track时，track属性配置错误track.eventName不能为空.");
                }
                if(!useIDM3){
                    Boolean isLoginId = originalConfig.getBool(KeyConstant.TRACK.concat(KeyConstant.POINT).concat(KeyConstant.IS_LOGIN_ID));
                    if(StrUtil.isBlank(distinctId) || Objects.isNull(isLoginId) || !saColumnNameList.contains(distinctId)){
                        throw new DataXException(CommonErrorCode.CONFIG_ERROR,"type为:track时，track属性配置错误或者distinctIdColumn属性未在column中.");
                    }
                }else{
                    String identityStr = originalConfig.getString(KeyConstant.IDENTITY,"[]");
                    List<IdentityItem> identityList = JSONObject.parseArray(identityStr, IdentityItem.class);
                    if(identityList.isEmpty()){
                        throw new DataXException(CommonErrorCode.CONFIG_ERROR,"type为:track并且使用IDM3时，identity属性不能为空.");
                    }
                    for (IdentityItem identityItem : identityList) {
                        if(NullUtil.isNullOrBlank(identityItem.getIdName()) || NullUtil.isNullOrBlank(identityItem.getColumnName()) || NullUtil.isNullOrBlank(identityItem.isColumn())){
                            throw new DataXException(CommonErrorCode.CONFIG_ERROR,"type为:track并且使用IDM3时，identity属性配置错误.");
                        }
                        if(identityItem.isColumn() && !saColumnNameList.contains(identityItem.getColumnName())){
                            throw new DataXException(CommonErrorCode.CONFIG_ERROR,"type为:track并且使用IDM3时，identity属性:["+identityItem.getIdName()+"]为列属性时，应该在column中有对应配置列");
                        }
                    }
                }
            }else if(KeyConstant.USER.equalsIgnoreCase(type)){
                String distinctId = originalConfig.getString(KeyConstant.USER.concat(KeyConstant.POINT).concat(KeyConstant.DISTINCT_ID_COLUMN),"");
                if(!useIDM3){
                    Boolean isLoginId = originalConfig.getBool(KeyConstant.USER.concat(KeyConstant.POINT).concat(KeyConstant.IS_LOGIN_ID),true);
                    if(StrUtil.isBlank(distinctId) || Objects.isNull(isLoginId) ||  !saColumnNameList.contains(distinctId)){
                        throw new DataXException(CommonErrorCode.CONFIG_ERROR,"type为:user时，user属性配置错误或者distinctIdColumn属性未在column中.");
                    }
                }else{
                    List<IdentityItem> identityList = originalConfig.getList(KeyConstant.IDENTITY, new ArrayList<>(), IdentityItem.class);
                    if(identityList.isEmpty()){
                        throw new DataXException(CommonErrorCode.CONFIG_ERROR,"type为:user并且使用IDM3时，identity属性不能为空.");
                    }
                }

            }else if(KeyConstant.ITEM.equalsIgnoreCase(type)){
                String itemType = originalConfig.getString(KeyConstant.ITEM.concat(KeyConstant.POINT).concat(KeyConstant.ITEM_TYPE));
                Boolean typeIsColumn = originalConfig.getBool(KeyConstant.ITEM.concat(KeyConstant.POINT).concat(KeyConstant.TYPE_IS_COLUMN));
                String itemIdColumn = originalConfig.getString(KeyConstant.ITEM.concat(KeyConstant.POINT).concat(KeyConstant.ITEM_ID_COLUMN));
                if(StrUtil.isBlank(itemType) || Objects.isNull(typeIsColumn) ||
                        StrUtil.isBlank(itemIdColumn) || !saColumnNameList.contains(itemIdColumn) || (typeIsColumn && !saColumnNameList.contains(itemType))){
                    throw new DataXException(CommonErrorCode.CONFIG_ERROR,"type为:item时，item属性配置错误或者itemType或者itemIdColumn属性未在column中.若typeIsColumn为false，itemType可以不在column中");
                }
            }else if(KeyConstant.ITEM_DELETE.equalsIgnoreCase(type)){
                String itemType = originalConfig.getString(KeyConstant.ITEM_DELETE.concat(KeyConstant.POINT).concat(KeyConstant.ITEM_TYPE));
                Boolean typeIsColumn = originalConfig.getBool(KeyConstant.ITEM_DELETE.concat(KeyConstant.POINT).concat(KeyConstant.TYPE_IS_COLUMN));
                String itemIdColumn = originalConfig.getString(KeyConstant.ITEM_DELETE.concat(KeyConstant.POINT).concat(KeyConstant.ITEM_ID_COLUMN));
                if(StrUtil.isBlank(itemType) || Objects.isNull(typeIsColumn) ||
                        StrUtil.isBlank(itemIdColumn) || !saColumnNameList.contains(itemIdColumn) || (typeIsColumn && !saColumnNameList.contains(itemType))){
                    throw new DataXException(CommonErrorCode.CONFIG_ERROR,"type为:itemDelete时，itemDelete属性配置错误或者itemType或者itemIdColumn属性未在column中.若typeIsColumn为false，itemType可以不在column中");
                }
            }else{
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"不支持的type类型");
            }

            for (SaColumnItem saColumnItem : saColumnList) {
                if(Objects.nonNull(saColumnItem.getIfNullGiveUp()) && saColumnItem.getIfNullGiveUp()){
                    StatisticsUtil.addNullGiveUpCountColumn(saColumnItem.getName());
                }
                if(Objects.nonNull(saColumnItem.getExclude()) && saColumnItem.getExclude()){
                    continue;
                }
                StatisticsUtil.addNullCountColumn(saColumnItem.getName());
            }

        }

        @Override
        public void post() {
            long sendSaTotalCount = 0;
            if(this.useIDM3){
                if(KeyConstant.TRACK.equalsIgnoreCase(type)){
                    sendSaTotalCount = EventUtil.IDENTITY_COUNT.longValue();
                    log.info("ID Mapping version 3:identity is null filter count:{}, send SA count:{},error count:{},ifNullGiveUp filter count:{}", EventUtil.IDENTITY_FILTER_COUNT.longValue(),sendSaTotalCount,EventUtil.ERROR_COUNT.sum(),StatisticsUtil.getNullGiveUpTotalCount().get());
                }else if(KeyConstant.USER.equalsIgnoreCase(type)){
                    sendSaTotalCount = ProfileUtil.IDENTITY_COUNT.longValue();
                    log.info("ID Mapping version 3:identity is null filter count:{}, send SA count:{},error count:{},ifNullGiveUp filter count:{} ,bind identity size is less than 2 count : {}", ProfileUtil.IDENTITY_FILTER_COUNT.longValue(),sendSaTotalCount,ProfileUtil.ERROR_COUNT.sum(),StatisticsUtil.getNullGiveUpTotalCount().get(),ProfileUtil.BIND_IDENTITY_SIZE_FILTER_COUNT.longValue());
                }else if(KeyConstant.ITEM.equalsIgnoreCase(type)){
                    sendSaTotalCount = ItemSetUtil.SEND_SA_COUNT.longValue();
                    log.info("ITEM Item_Id or Item_type is null filter count:{}, send SA count:{},error count:{},ifNullGiveUp filter count:{}",ItemSetUtil.FILTER_COUNT.longValue(), sendSaTotalCount,ItemSetUtil.ERROR_COUNT.sum(),StatisticsUtil.getNullGiveUpTotalCount().get());
                }

            }else{
                if(KeyConstant.TRACK.equalsIgnoreCase(type)){
                    sendSaTotalCount = EventUtil.DISTINCT_ID_COUNT.longValue();
                    log.info("ID Mapping version 2:distinctId is null filter count:{}, send SA count:{},error count:{},ifNullGiveUp filter count:{}",EventUtil.DISTINCT_ID_FILTER_COUNT.longValue(),sendSaTotalCount,EventUtil.ERROR_COUNT.sum(),StatisticsUtil.getNullGiveUpTotalCount().get());
                }else if(KeyConstant.USER.equalsIgnoreCase(type)){
                    sendSaTotalCount = ProfileUtil.DISTINCT_ID_COUNT.longValue();
                    log.info("ID Mapping version 2:distinctId is null filter count:{}, send SA count:{},error count:{},ifNullGiveUp filter count:{}", ProfileUtil.DISTINCT_ID_FILTER_COUNT.longValue(),sendSaTotalCount,ProfileUtil.ERROR_COUNT.sum(),StatisticsUtil.getNullGiveUpTotalCount().get());
                }else if(KeyConstant.ITEM.equalsIgnoreCase(type)){
                    sendSaTotalCount = ItemSetUtil.SEND_SA_COUNT.longValue();
                    log.info("ITEM Item_Id or Item_type is null filter count:{}, send SA count:{},error count:{},ifNullGiveUp filter count:{}",ItemSetUtil.FILTER_COUNT.longValue(), sendSaTotalCount,ItemSetUtil.ERROR_COUNT.sum(),StatisticsUtil.getNullGiveUpTotalCount().get());
                }
            }
            Map<String, LongAdder> allNullCountColumn = StatisticsUtil.getAllNullCountColumn();
            StringBuilder sb = new StringBuilder("\n");
            Set<Map.Entry<String, LongAdder>> entries = allNullCountColumn.entrySet();
            BigDecimal totalCount = new BigDecimal(sendSaTotalCount + "");
            BigDecimal oneHundred = new BigDecimal("100");
            for (Map.Entry<String, LongAdder> entry : entries) {
                String columnName = entry.getKey();
                LongAdder longAdder = entry.getValue();
                long sum = longAdder.sum();
                sb.append("\t\t列名:【").append(columnName).append("】空值数: ").append(sum).append(" ,空值率：").append(totalCount.compareTo(BigDecimal.ZERO) == 0? 0 :new BigDecimal(sum+"").divide(totalCount,2, RoundingMode.HALF_UP).multiply(oneHundred)).append("% \n");
            }
            log.info("列值空值统计：{}",sb.toString());
            StringBuilder nullGiveUpCountSb = new StringBuilder("\n");
            Map<String, AtomicLong> allNullGiveUpCountColumnMap = StatisticsUtil.getAllNullGiveUpCountColumn();
            Set<Map.Entry<String, AtomicLong>> allNullGiveUpCountColumnEntrySet = allNullGiveUpCountColumnMap.entrySet();
            for (Map.Entry<String, AtomicLong> entry : allNullGiveUpCountColumnEntrySet) {
                String columnName = entry.getKey();
                AtomicLong atomicLong = entry.getValue();
                nullGiveUpCountSb.append("\t\t列名:【").append(columnName).append("】过滤数: ").append(atomicLong.get()).append(" \n");
            }
            log.info("ifNullGiveUp配置统计：{}",nullGiveUpCountSb.toString());
            super.post();
        }

        public void destroy() {
            SaUtil.getInstance().shutdown();
        }
    }

    @Slf4j
    public static class Task extends Writer.Task{

        private static SensorsAnalytics sa ;

        private Configuration readerConfig;

        private String type;

        private List<SaColumnItem> saColumnList;

        private Map<String,Object> trackBaseProp = new HashMap<>();
        private Map<String,Object> userBaseProp = new HashMap<>();
        private Map<String,Object> itemBaseProp = new HashMap<>();
        private Map<String,Object> itemDeleteBaseProp = new HashMap<>();

        private List<BasePlugin.SAPlugin> basePluginList;

        private Boolean useIDM3;
        private Boolean isUnBind = false;

        private Boolean isBind = false;
        private List<IdentityItem> identityList;

        public void startWrite(RecordReceiver recordReceiver) {
            Record record = null;
            A:while((record = recordReceiver.getFromReader()) != null) {
                Map<String,Object> properties = new HashMap<>();

                if(KeyConstant.TRACK.equalsIgnoreCase(type)){
                    properties.putAll(trackBaseProp);
                }else if(KeyConstant.USER.equalsIgnoreCase(type)){
                    properties.putAll(userBaseProp);
                }else if(KeyConstant.ITEM.equalsIgnoreCase(type)){
                    properties.putAll(itemBaseProp);
                }else if(KeyConstant.ITEM_DELETE.equalsIgnoreCase(type)){
                    properties.putAll(itemDeleteBaseProp);
                }else{
                    continue;
                }
                for (SaColumnItem col : saColumnList) {
                    Column column = record.getColumn(col.getIndex());
                    if(Objects.isNull(column)){
                        //这里为空，很大的原因是index配置不正确
                        Object value = ConverterUtil.convert(col.getName(),null,col,properties);
                        if(NullUtil.isNullOrBlank(value)){
                            if(!NullUtil.isNullOrBlank(col.getIfNullGiveUp()) && col.getIfNullGiveUp()){
                                StatisticsUtil.addNullGiveUpCountByColumn(col.getName(),1);
                                StatisticsUtil.getNullGiveUpTotalCount().incrementAndGet();
                                continue A;
                            }
                            continue;
                        }
                        properties.put(col.getName(),value);
                    }else if(column instanceof StringColumn){
                        String v = column.asString();
                        Object value = ConverterUtil.convert(col.getName(),v,col,properties);
                        if(NullUtil.isNullOrBlank(value)){
                            if(!NullUtil.isNullOrBlank(col.getIfNullGiveUp()) && col.getIfNullGiveUp()){
                                StatisticsUtil.addNullGiveUpCountByColumn(col.getName(),1);
                                StatisticsUtil.getNullGiveUpTotalCount().incrementAndGet();
                                continue A;
                            }
                            continue;
                        }
                        properties.put(col.getName(),value);
                    }else if(column instanceof BoolColumn){
                        Boolean v = column.asBoolean();
                        Object value = ConverterUtil.convert(col.getName(),v,col,properties);
                        if(NullUtil.isNullOrBlank(value)){
                            if(!NullUtil.isNullOrBlank(col.getIfNullGiveUp()) && col.getIfNullGiveUp()){
                                StatisticsUtil.addNullGiveUpCountByColumn(col.getName(),1);
                                StatisticsUtil.getNullGiveUpTotalCount().incrementAndGet();
                                continue A;
                            }
                            continue;
                        }
                        properties.put(col.getName(),value);
                    }else if(column instanceof DoubleColumn){
                        BigDecimal v = column.asBigDecimal();
                        Object value = ConverterUtil.convert(col.getName(),v,col,properties);
                        if(NullUtil.isNullOrBlank(value)){
                            if(!NullUtil.isNullOrBlank(col.getIfNullGiveUp()) && col.getIfNullGiveUp()){
                                StatisticsUtil.addNullGiveUpCountByColumn(col.getName(),1);
                                StatisticsUtil.getNullGiveUpTotalCount().incrementAndGet();
                                continue A;
                            }
                            continue;
                        }
                        properties.put(col.getName(),value);
                    }else if(column instanceof LongColumn){
                        BigInteger v = column.asBigInteger();
                        Object value = ConverterUtil.convert(col.getName(),v,col,properties);
                        if(NullUtil.isNullOrBlank(value)){
                            if(!NullUtil.isNullOrBlank(col.getIfNullGiveUp()) && col.getIfNullGiveUp()){
                                StatisticsUtil.addNullGiveUpCountByColumn(col.getName(),1);
                                StatisticsUtil.getNullGiveUpTotalCount().incrementAndGet();
                                continue A;
                            }
                            continue;
                        }
                        properties.put(col.getName(),value);
                    }else if(column instanceof DateColumn){
                        Date v = column.asDate();
                        Object value = ConverterUtil.convert(col.getName(),v,col,properties);
                        if(NullUtil.isNullOrBlank(value)){
                            if(!NullUtil.isNullOrBlank(col.getIfNullGiveUp()) && col.getIfNullGiveUp()){
                                StatisticsUtil.addNullGiveUpCountByColumn(col.getName(),1);
                                StatisticsUtil.getNullGiveUpTotalCount().incrementAndGet();
                                continue A;
                            }
                            continue;
                        }
                        properties.put(col.getName(),value);
                    }else if(column instanceof BytesColumn){
                        byte[] v = column.asBytes();
                        Object value = ConverterUtil.convert(col.getName(),v,col,properties);
                        if(NullUtil.isNullOrBlank(value)){
                            if(!NullUtil.isNullOrBlank(col.getIfNullGiveUp()) && col.getIfNullGiveUp()){
                                StatisticsUtil.addNullGiveUpCountByColumn(col.getName(),1);
                                StatisticsUtil.getNullGiveUpTotalCount().incrementAndGet();
                                continue A;
                            }
                            continue;
                        }
                        properties.put(col.getName(),value);
                    }
                }
                boolean process = true;
                if(!Objects.isNull(this.basePluginList) && !this.basePluginList.isEmpty()){
                    for (BasePlugin.SAPlugin saPlugin : this.basePluginList) {
                        process = saPlugin.process(properties);
                        if(!process){
                            continue A;
                        }
                    }
                }
                //在这里排除需要导入的字段是因为IfElse转换器会使用到已转换的列，在这里排除的话，IfElse转换器就可以使用到需要排除的列做判断
                for (SaColumnItem col : saColumnList) {
                    if(!Objects.isNull(col.getExclude()) && col.getExclude()){
                        properties.remove(col.getName());
                    }
                }
                if(!this.useIDM3){
                    SaUtil.process(sa,type,properties);
                }else{
                    SaUtil.process(sa,type,properties,identityList,this.isUnBind,this.isBind);
                }

            }
        }

        public void init() {
            this.readerConfig = super.getPluginJobConf();
            this.type = readerConfig.getString(KeyConstant.TYPE);
            this.useIDM3 = readerConfig.getBool(KeyConstant.USE_IDM3,true);
            this.isUnBind = readerConfig.getBool(KeyConstant.IS_UNBIND,false);
            this.isBind = readerConfig.getBool(KeyConstant.IS_BIND,false);
            String identityStr = readerConfig.getString(KeyConstant.IDENTITY,"[]");
            this.identityList = JSONObject.parseArray(identityStr, IdentityItem.class);
            JSONArray saColumnJsonArray = readerConfig.get(KeyConstant.SA_COLUMN, JSONArray.class);
            if(Objects.isNull(saColumnJsonArray)){
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"column不应该为空！");
            }
            String saColumnStr = saColumnJsonArray.toJSONString();
            this.saColumnList = JSONObject.parseArray(saColumnStr, SaColumnItem.class);
            if(Objects.isNull(saColumnList) || saColumnList.isEmpty()){
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"column不应该为空！");
            }
            for (SaColumnItem col : saColumnList) {
                List<DataConverter> dataConverters = col.getDataConverters();
                if(Objects.isNull(dataConverters) || dataConverters.isEmpty()){
                    continue;
                }
                dataConverters.forEach(con->{
                    con.setConverter(ConverterFactory.converterPrototype(con.getType()));
                });
            }

            this.sa = SaUtil.getInstance();

            if(KeyConstant.TRACK.equalsIgnoreCase(type)){
                String eventEventName = readerConfig.getString(KeyConstant.TRACK.concat(KeyConstant.POINT).concat(KeyConstant.EVENT_NAME));
                if(!this.useIDM3){
                    String eventDistinctIdCol = readerConfig.getString(KeyConstant.TRACK.concat(KeyConstant.POINT).concat(KeyConstant.DISTINCT_ID_COLUMN));
                    Boolean eventIsLoginId = readerConfig.getBool(KeyConstant.TRACK.concat(KeyConstant.POINT).concat(KeyConstant.IS_LOGIN_ID));
                    trackBaseProp.put(KeyConstant.EVENT_DISTINCT_ID_COL,eventDistinctIdCol);
                    trackBaseProp.put(KeyConstant.EVENT_IS_LOGIN_ID,eventIsLoginId);
                }
                trackBaseProp.put(KeyConstant.EVENT_EVENT_NAME,eventEventName);
            }else if(KeyConstant.USER.equalsIgnoreCase(type)){
                if(!this.useIDM3){
                    String userDistinctId = readerConfig.getString(KeyConstant.USER.concat(KeyConstant.POINT).concat(KeyConstant.DISTINCT_ID_COLUMN));
                    Boolean userIsLoginId = readerConfig.getBool(KeyConstant.USER.concat(KeyConstant.POINT).concat(KeyConstant.IS_LOGIN_ID));
                    userBaseProp.put(KeyConstant.USER_DISTINCT_ID,userDistinctId);
                    userBaseProp.put(KeyConstant.user_is_login_id,userIsLoginId);
                }
            }else if(KeyConstant.ITEM.equalsIgnoreCase(type)){
                String itemItemType = readerConfig.getString(KeyConstant.ITEM.concat(KeyConstant.POINT).concat(KeyConstant.ITEM_TYPE));
                Boolean itemTypeIsColumn = readerConfig.getBool(KeyConstant.ITEM.concat(KeyConstant.POINT).concat(KeyConstant.TYPE_IS_COLUMN));
                String itemItemIdColumn = readerConfig.getString(KeyConstant.ITEM.concat(KeyConstant.POINT).concat(KeyConstant.ITEM_ID_COLUMN));
                itemBaseProp.put(KeyConstant.ITEM_ITEM_TYPE,itemItemType);
                itemBaseProp.put(KeyConstant.ITEM_TYPE_IS_COLUMN,itemTypeIsColumn);
                itemBaseProp.put(KeyConstant.ITEM_ITEM_ID_COLUMN,itemItemIdColumn);
            }else if(KeyConstant.ITEM_DELETE.equalsIgnoreCase(type)){
                String itemItemType = readerConfig.getString(KeyConstant.ITEM_DELETE.concat(KeyConstant.POINT).concat(KeyConstant.ITEM_TYPE));
                Boolean itemTypeIsColumn = readerConfig.getBool(KeyConstant.ITEM_DELETE.concat(KeyConstant.POINT).concat(KeyConstant.TYPE_IS_COLUMN));
                String itemItemIdColumn = readerConfig.getString(KeyConstant.ITEM_DELETE.concat(KeyConstant.POINT).concat(KeyConstant.ITEM_ID_COLUMN));
                itemDeleteBaseProp.put(KeyConstant.ITEM_ITEM_TYPE,itemItemType);
                itemDeleteBaseProp.put(KeyConstant.ITEM_TYPE_IS_COLUMN,itemTypeIsColumn);
                itemDeleteBaseProp.put(KeyConstant.ITEM_ITEM_ID_COLUMN,itemItemIdColumn);
            }

            String SaPluginStr = readerConfig.getString(KeyConstant.PLUGIN,"[]");
            List<SaPlugin> SaPluginList = JSONObject.parseArray(SaPluginStr, SaPlugin.class);
            if(!Objects.isNull(SaPluginList) && !SaPluginList.isEmpty()){
                basePluginList = new ArrayList<>();
            }

            SaPluginList.forEach(saPlugin -> {
                String pluginName = saPlugin.getName();
                String pluginClass = saPlugin.getClassName();
                Map<String, Object> pluginParam = saPlugin.getParam();
                if(!NullUtil.isNullOrBlank(pluginName) && !NullUtil.isNullOrBlank(pluginClass)){
                    if(Objects.isNull(pluginParam)){
                        pluginParam = new HashMap<>();
                    }
                    basePluginList.add(PluginClassLoader.getBasePlugin(saPlugin.getName(), pluginClass, pluginParam));
                }

            });
        }

        public void destroy() {}
    }


}
