package com.alibaba.datax.plugin.writer;

import cn.hutool.core.util.StrUtil;
import com.alibaba.datax.BasePlugin;
import com.alibaba.datax.CustomizeProp;
import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.ConverterFactory;
import com.alibaba.datax.plugin.KeyConstant;
import com.alibaba.datax.plugin.classloader.PluginClassLoader;
import com.alibaba.datax.plugin.domain.DataConverter;
import com.alibaba.datax.plugin.domain.SaColumnItem;
import com.alibaba.datax.plugin.domain.SaPlugin;
import com.alibaba.datax.plugin.domain.TableColumnMetaData;
import com.alibaba.datax.plugin.util.ColumnDataUtil;
import com.alibaba.datax.plugin.util.ConverterUtil;
import com.alibaba.datax.plugin.util.MysqlUtil;
import com.alibaba.datax.plugin.util.NullUtil;
import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.Date;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SaWriter extends Writer {

    @Slf4j
    public static class Job extends Writer.Job{

        private Configuration originalConfig = null;

        public List<Configuration> split(int i) {
            List<Configuration> list = new ArrayList<>();
            for (int j = 0; j < i; j++) {
                list.add(this.originalConfig.clone());
            }
            return list;
        }

        public void init() {
            this.originalConfig = super.getPluginJobConf();
            boolean sharding = originalConfig.getBool(KeyConstant.SHARDING, false);
            String shardingYamlFilePath = originalConfig.getString(KeyConstant.SHARDING_YAML_FILE_PATH, null);
            String url = originalConfig.getString(KeyConstant.URL,"");
            String table = originalConfig.getString(KeyConstant.TABLE,"");
            if(sharding){
                if(Objects.isNull(shardingYamlFilePath) || Objects.equals("",shardingYamlFilePath.trim()) ){
                    throw new DataXException(CommonErrorCode.CONFIG_ERROR,"sharding为true时,shardingYamlFilePath不应该为空！");
                }
            }else{
                if(Objects.isNull(url) || Objects.equals("",url.trim()) ){
                    throw new DataXException(CommonErrorCode.CONFIG_ERROR,"url不应该为空！");
                }
            }
            if(Objects.isNull(table) || Objects.equals("",table.trim()) ){
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"table不应该为空！");
            }
            String model = originalConfig.getString(KeyConstant.MODEL);
            if(Objects.isNull(model) || Objects.equals("",model)){
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"model不应该为空！可选值:insert/insertBatch/update/insertUpdate/replace/replaceBatch/cusInsertUpdate");
            }
            if(!(Objects.equals(KeyConstant.MODEL_INSERT,model) || Objects.equals(KeyConstant.MODEL_INSERT_BATCH,model)
                    || Objects.equals(KeyConstant.MODEL_UPDATE,model) || Objects.equals(KeyConstant.MODEL_INSERT_UPDATE,model)
                    || Objects.equals(KeyConstant.MODEL_REPLACE,model) || Objects.equals(KeyConstant.MODEL_REPLACE_BATCH,model)
                    || Objects.equals(KeyConstant.MODEL_CUSTOMIZE_INSERT_UPDATE,model) )){
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"model值不正确！可选值:insert/insertBatch/update/insertUpdate/replace/replaceBatch/cusInsertUpdate");
            }
            if((Objects.equals(KeyConstant.MODEL_UPDATE,model) || Objects.equals(KeyConstant.MODEL_INSERT_UPDATE,model)
                    || Objects.equals(KeyConstant.MODEL_CUSTOMIZE_INSERT_UPDATE,model))
               && originalConfig.getList(KeyConstant.UPDATE_WHERE_COLUMN,new ArrayList<>()).isEmpty()){
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"model为update/insertUpdate/cusInsertUpdate时，updateWhereColumn不能为空！");
            }
            String customizeInsertUpdateJudgeSqlTemplate = originalConfig.getString(KeyConstant.CUSTOMIZE_INSERT_UPDATE_JUDGE, "");
            if(Objects.equals(KeyConstant.MODEL_CUSTOMIZE_INSERT_UPDATE,model) &&
                    StrUtil.isBlank(customizeInsertUpdateJudgeSqlTemplate)){
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"cusInsertUpdate模式时，cusInsUpJudge不应该为空！");
            }
            String userName = originalConfig.getString(KeyConstant.USER_NAME,"");
            String password = originalConfig.getString(KeyConstant.PASSWORD,"");
            Map<String, Object> customProp = originalConfig.getMap(KeyConstant.CUSTOMIZE_PROP,new ConcurrentHashMap<>());
            CustomizeProp.set(KeyConstant.MYSQL_WRITER_CUSTOM_PROP,customProp);
            MysqlUtil.setUrl(url);
            MysqlUtil.setUser(userName);
            MysqlUtil.setPassword(password);
            MysqlUtil.setSharding(sharding);
            MysqlUtil.setShardingYamlFilePath(shardingYamlFilePath);
            DataSource dataSource = MysqlUtil.defaultDataSource();
            Connection connection = null;
            Statement statement = null;
            ResultSet rs = null;
            try {
                connection = dataSource.getConnection();
                statement = connection.createStatement();
                String queryColumnSql = "select * from " + table + " where 1=2";
                List<TableColumnMetaData> tableColumnMetaDataList = new ArrayList<>();
                rs = statement.executeQuery(queryColumnSql);
                ResultSetMetaData rsMetaData = rs.getMetaData();
                for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {
                    TableColumnMetaData meta = new TableColumnMetaData();
                    meta.setField(rsMetaData.getColumnName(i + 1));
                    meta.setType(rsMetaData.getColumnType(i + 1));
                    meta.setTypeName(rsMetaData.getColumnTypeName(i + 1));
                    tableColumnMetaDataList.add(meta);
                }
                if(Objects.isNull(tableColumnMetaDataList) || tableColumnMetaDataList.isEmpty()){
                    throw new DataXException(CommonErrorCode.CONFIG_ERROR,"获取表["+table+"]列元数据为空，请先添加列.");
                }
                originalConfig.set(KeyConstant.TABLE_COLUMN_META_DATA,JSONObject.toJSONString(tableColumnMetaDataList));
            } catch (SQLException throwables) {
                throwables.printStackTrace();
                log.error("获取表[{}]列元数据时发生错误：",table,throwables);
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"获取表["+table+"]列元数据时发生错误.");
            }finally {
                if(rs != null){
                    try {
                        rs.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
                if(statement != null){
                    try {
                        statement.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
                if(connection != null){
                    try {
                        connection.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
            }

            JSONArray saColumnJsonArray = originalConfig.get(KeyConstant.SA_COLUMN, JSONArray.class);
            if(Objects.isNull(saColumnJsonArray)){
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"column不应该为空！");
            }
            String saColumnStr = saColumnJsonArray.toJSONString();
            List<SaColumnItem> saColumnList = JSONObject.parseArray(saColumnStr, SaColumnItem.class);
            if(Objects.isNull(saColumnList) || saColumnList.isEmpty()){
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"column不应该为空！");
            }

        }

        public void destroy() {}
    }

    @Slf4j
    public static class Task extends Writer.Task{

        private Configuration readerConfig;

        private List<SaColumnItem> saColumnList;

        private Map<String, TableColumnMetaData> tableColumnMetaDataMap;

        private List<String> tableColumnOrderList = new ArrayList<>();

        private List<BasePlugin.SAPlugin> basePluginList;

        private String tableName;

        private String model;

        private int batchSize;

        private List<String> updateWhereColumn;

        private List<String> insertUpdateModelNotUpdateColumnList;

        private boolean nullValueIsUpdate;

        private String customizeInsertUpdateJudgeSqlTemplate;

        private static Pattern PATTERN = Pattern.compile("\\{(.*?)}");


        public void startWrite(RecordReceiver recordReceiver) {
            Record record = null;
            List<Map<String,Object>> batchList = new ArrayList<>(this.batchSize);

            A:while((record = recordReceiver.getFromReader()) != null) {
                Map<String,Object> properties = new HashMap<>();

                for (SaColumnItem col : saColumnList) {
                    Column column = record.getColumn(col.getIndex());
                    if(Objects.isNull(column)){
                        //这里为空，很大的原因是index配置不正确
                        Object value = ConverterUtil.convert(col.getName(),null,col,properties);
                        if(NullUtil.isNullOrBlank(value)){
                            if(!NullUtil.isNullOrBlank(col.getIfNullGiveUp()) && col.getIfNullGiveUp()){
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

                String sql = generateSql(tableName, tableColumnOrderList, this.tableColumnMetaDataMap, properties, batchList);
                insertData(sql,tableName,tableColumnOrderList,this.tableColumnMetaDataMap,properties,batchList);
                sql = null;
            }
            if((Objects.equals(KeyConstant.MODEL_INSERT_BATCH,this.model) || Objects.equals(KeyConstant.MODEL_REPLACE_BATCH,this.model)) && !batchList.isEmpty()){
                String model = KeyConstant.REPLACE;
                if(Objects.equals(KeyConstant.MODEL_INSERT_BATCH,this.model)){
                    model = KeyConstant.INSERT;
                }
                String sql = ColumnDataUtil.transformInsertBatchSql(model,tableName,tableColumnOrderList,this.tableColumnMetaDataMap,batchList);
                executeSql(sql);
                batchList.clear();
                sql = null;
                model = null;
            }
        }

        private String generateSql(String tableName, List<String> tableColumnOrderList, Map<String, TableColumnMetaData> tableColumnMetaDataMap,
                                   Map<String, Object> properties, List<Map<String, Object>> batchList){
            String sql = null;
            if(Objects.equals(KeyConstant.MODEL_INSERT,this.model)){
                sql = ColumnDataUtil.transformInsertSql(KeyConstant.INSERT,tableName,tableColumnOrderList,this.tableColumnMetaDataMap,properties);
            } else if(Objects.equals(KeyConstant.MODEL_INSERT_BATCH,this.model)){
                if(!Objects.isNull(properties)){
                    batchList.add(properties);
                }
                if(this.batchSize == batchList.size()){
                    sql = ColumnDataUtil.transformInsertBatchSql(KeyConstant.INSERT,tableName,tableColumnOrderList,this.tableColumnMetaDataMap,batchList);
                }
            }else if(Objects.equals(KeyConstant.MODEL_UPDATE,this.model)){
                sql = ColumnDataUtil.transformUpdateSql(KeyConstant.UPDATE,tableName,tableColumnOrderList,this.tableColumnMetaDataMap,
                        this.updateWhereColumn,properties,this.insertUpdateModelNotUpdateColumnList,
                        this.nullValueIsUpdate);
            }else if(Objects.equals(KeyConstant.MODEL_INSERT_UPDATE,this.model)){
                sql = ColumnDataUtil.transformInsertSql(KeyConstant.INSERT,tableName,tableColumnOrderList,this.tableColumnMetaDataMap,properties);
            }else if(Objects.equals(KeyConstant.MODEL_CUSTOMIZE_INSERT_UPDATE,this.model)){
                sql = ColumnDataUtil.transformInsertSql(KeyConstant.INSERT,tableName,tableColumnOrderList,this.tableColumnMetaDataMap,properties);
            }else if(Objects.equals(KeyConstant.MODEL_REPLACE,this.model)){
                sql = ColumnDataUtil.transformInsertSql(KeyConstant.REPLACE,tableName,tableColumnOrderList,this.tableColumnMetaDataMap,properties);
            }else if(Objects.equals(KeyConstant.MODEL_REPLACE_BATCH,this.model)){
                if(!Objects.isNull(properties)){
                    batchList.add(properties);
                }
                if(this.batchSize == batchList.size()){
                    sql = ColumnDataUtil.transformInsertBatchSql(KeyConstant.REPLACE,tableName,tableColumnOrderList,this.tableColumnMetaDataMap,batchList);
                }
            }else{
                log.info("不支持的模式：{}",this.model);
            }
            return sql;
        }

        private void insertData(String sql,String tableName, List<String> tableColumnOrderList, Map<String, TableColumnMetaData> tableColumnMetaDataMap,
                                Map<String, Object> properties, List<Map<String, Object>> batchList) {
            if(Objects.isNull(sql) || Objects.equals("",sql)){
                return;
            }
            if(Objects.equals(KeyConstant.MODEL_INSERT,this.model) || Objects.equals(KeyConstant.MODEL_REPLACE,this.model) || Objects.equals(KeyConstant.MODEL_UPDATE,this.model)){
                executeSql(sql);
            } else if(Objects.equals(KeyConstant.MODEL_INSERT_BATCH,this.model) || Objects.equals(KeyConstant.MODEL_REPLACE_BATCH,this.model)){
                if(this.batchSize == batchList.size()){
                    executeSql(sql);
                    batchList.clear();
                    return;
                }
                if(!Objects.isNull(properties)){
                    batchList.add(properties);
                }
            }else if(Objects.equals(KeyConstant.MODEL_INSERT_UPDATE,this.model)){
                boolean flag = executeSql(sql);
                if(!flag){
                    String updateSql = ColumnDataUtil.transformUpdateSql(KeyConstant.UPDATE,
                            tableName,tableColumnOrderList,this.tableColumnMetaDataMap,
                            this.updateWhereColumn,properties,this.insertUpdateModelNotUpdateColumnList,
                            this.nullValueIsUpdate);
                    if(Objects.isNull(updateSql) || Objects.equals("",updateSql)){
                        return;
                    }
                    executeSql(updateSql);
                }
            }else if(Objects.equals(KeyConstant.MODEL_CUSTOMIZE_INSERT_UPDATE,this.model)){
                String customizeInsertUpdateJudgeSql = resolveSql(properties,this.customizeInsertUpdateJudgeSqlTemplate);
                boolean isExecuteInsert = executeCusSql(customizeInsertUpdateJudgeSql);
                if(isExecuteInsert){
                    isExecuteInsert = executeSql(sql);
                }
                if(!isExecuteInsert){
                    String updateSql = ColumnDataUtil.transformUpdateSql(KeyConstant.UPDATE,
                            tableName,tableColumnOrderList,this.tableColumnMetaDataMap,
                            this.updateWhereColumn,properties,this.insertUpdateModelNotUpdateColumnList,
                            this.nullValueIsUpdate);
                    if(Objects.isNull(updateSql) || Objects.equals("",updateSql)){
                        return;
                    }
                    executeSql(updateSql);
                }
            }else{
                log.info("不支持的模式：{}",this.model);
            }

        }

        private String resolveSql(Map<String, Object> values,String oldSql){
            String sql = oldSql;
            Matcher matcher = PATTERN.matcher(sql);
            while (matcher.find()) {
                String oldKey = matcher.group();
                String key = oldKey.substring(1,matcher.group().length() - 1).trim();
                sql = sql.replace(oldKey, Objects.isNull(values.get(key))?"null":values.get(key).toString());
            }
            return sql;
        }

        /**
         * 为true时，执行insert,否则应该执行update
         * @param sql
         * @return
         */
        private boolean executeCusSql(String sql){
            if(Objects.isNull(sql) || Objects.equals("",sql)){
                return true;
            }
            try {
                List<Map<String, Object>> data = JdbcUtils.executeQuery(MysqlUtil.defaultDataSource(), sql);
                if(Objects.isNull(data) || data.isEmpty()){
                    return true;
                }
                return false;
            } catch (SQLException throwables) {
                throwables.printStackTrace();
                log.info("执行SQL失败! sql: {}",sql);
                return true;
            }
        }

        private boolean executeSql(String sql){
            if(Objects.isNull(sql) || Objects.equals("",sql)){
                return true;
            }
            PreparedStatement preparedStatement = null;
            Connection connection = null;
            try {
                connection = MysqlUtil.defaultDataSource().getConnection();
                preparedStatement = connection.prepareStatement(sql);
                boolean execute = preparedStatement.execute();
                if(MysqlUtil.isSharding()){
                    return true;
                }
                return execute;
            } catch (SQLException throwables) {
                throwables.printStackTrace();
                log.info("执行SQL失败! sql: {}",sql);
                return false;
            }finally {
                if(!Objects.isNull(preparedStatement)){
                    try {
                        preparedStatement.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
                if(!Objects.isNull(connection)){
                    try {
                        connection.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
            }
        }

        public void init() {
            this.readerConfig = super.getPluginJobConf();
            Object customizeProp = CustomizeProp.get(KeyConstant.MYSQL_WRITER_CUSTOM_PROP);
            if(Objects.isNull(customizeProp)){
                Map<String, Object> customProp = readerConfig.getMap(KeyConstant.CUSTOMIZE_PROP,new ConcurrentHashMap<>());
                CustomizeProp.set(KeyConstant.MYSQL_WRITER_CUSTOM_PROP,customProp);
            }
            this.tableName = this.readerConfig.getString(KeyConstant.TABLE);
            this.model = this.readerConfig.getString(KeyConstant.MODEL);
            this.batchSize = this.readerConfig.getInt(KeyConstant.BATCH_SIZE,500);
            this.updateWhereColumn = this.readerConfig.getList(KeyConstant.UPDATE_WHERE_COLUMN, new ArrayList());
            this.insertUpdateModelNotUpdateColumnList = this.readerConfig.getList(KeyConstant.INSERT_UPDATE_MODEL_NOT_UPDATE_COLUMN, new ArrayList());
            this.nullValueIsUpdate = readerConfig.getBool(KeyConstant.NULL_VALUE_IS_UPDATE, true);
            this.customizeInsertUpdateJudgeSqlTemplate = readerConfig.getString(KeyConstant.CUSTOMIZE_INSERT_UPDATE_JUDGE, "");
            if(Objects.equals(KeyConstant.MODEL_CUSTOMIZE_INSERT_UPDATE,this.model) &&
                    StrUtil.isBlank(this.customizeInsertUpdateJudgeSqlTemplate)){
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"cusInsertUpdate模式时，cusInsUpJudge不应该为空！");
            }
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
                if(!(!Objects.isNull(col.getExclude()) && col.getExclude())){
                    this.tableColumnOrderList.add(col.getName());
                }
                List<DataConverter> dataConverters = col.getDataConverters();
                if(Objects.isNull(dataConverters) || dataConverters.isEmpty()){
                    continue;
                }
                dataConverters.forEach(con->{
                    con.setConverter(ConverterFactory.converterPrototype(con.getType()));
                });
            }

            String tableColumnMetaDataStr = readerConfig.get(KeyConstant.TABLE_COLUMN_META_DATA, String.class);
            if(Objects.isNull(tableColumnMetaDataStr) || Objects.equals("",tableColumnMetaDataStr)){
                String table = readerConfig.getString(KeyConstant.TABLE);
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"获取表["+table+"]列元数据时发生错误.");
            }

            List<TableColumnMetaData> tableColumnMetaDataList = JSONObject.parseArray(tableColumnMetaDataStr, TableColumnMetaData.class);

            this.tableColumnMetaDataMap = tableColumnMetaDataList.stream().collect(Collectors.toMap(TableColumnMetaData::getField, a -> a,(k1, k2)->k1));
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
