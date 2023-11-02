package com.alibaba.datax.plugin.reader;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.db.handler.RsHandler;
import cn.hutool.db.sql.SqlExecutor;
import com.alibaba.datax.BasePlugin;
import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.KeyConstant;
import com.alibaba.datax.plugin.ReaderErrorCode;
import com.alibaba.datax.plugin.classloader.DependencyClassLoader;
import com.alibaba.datax.plugin.domain.SaPlugin;
import com.alibaba.datax.plugin.util.MysqlUtil;
import com.alibaba.datax.plugin.util.NullUtil;
import com.alibaba.datax.plugin.util.TypeUtil;
import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.math.BigInteger;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SaMysqlReader extends Reader {

    @Slf4j
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Job extends Reader.Job{

        private Configuration originalConfig = null;

        private String  rowNumCountSql;
        private String  userName;
        private String  password;
        private String  mysqlUrl;
        private String  sql;
        /**
         * 当使用时间条件过滤时，如果数据量大到不能拆分时，使用分页查询的sql
         */
        private String  sqlRowNum;
        private String  streamSqlRowNum;
        private String  sqlCount;
        private String  rowNumSql;
        private String  streamRowNumSql;
        private String  where;
        private String  tableName;
        private String  linkedTable;
        private List<String>  columnList;
        private boolean useRowNumber;

        @Override
        public List<Configuration> split(int i) {

            List<Configuration> splittedConfigs = new ArrayList<Configuration>();

            Boolean useRowNumber = originalConfig.getBool(KeyConstant.USE_ROW_NUMBER, false);
            Boolean useStream = originalConfig.getBool(KeyConstant.USE_STREAM, false);
            if(useRowNumber){
                if(useStream){
                    Configuration sliceConfig = originalConfig.clone();
                    splittedConfigs.add(sliceConfig);
                }else{
                    long pageSize = originalConfig.getLong(KeyConstant.PAGE_SIZE,10000);
                    long receivePageSize = originalConfig.getLong(KeyConstant.RECEIVE_PAGE_SIZE,5);
                    List<List<Long>> rowNumberPartition = supportRowNumberPartition(pageSize,receivePageSize);
                    rowNumberPartition.forEach(item->{
                        Configuration sliceConfig = originalConfig.clone();
                        sliceConfig.set(KeyConstant.START_PAGE_NO,item.get(0));
                        sliceConfig.set(KeyConstant.END_PAGE_NO,item.get(1));
                        sliceConfig.set(KeyConstant.PAGE_SIZE,pageSize);
                        splittedConfigs.add(sliceConfig);
                    });
                }

            }else{
                String startTime = originalConfig.getString(KeyConstant.START_TIME);
                String endTime = originalConfig.getString(KeyConstant.END_TIME);
                Integer taskNum = originalConfig.getInt(KeyConstant.TASK_NUM,i);
                String datePattern = originalConfig.getString(KeyConstant.DATE_PATTERN);

                if(StrUtil.isBlank(startTime) || StrUtil.isBlank(endTime) || StrUtil.isBlank(datePattern)){
                    throw new DataXException(CommonErrorCode.CONFIG_ERROR,"useRowNumber为空或false时，startTime/endTime/datePattern不能为空");
                }

                SimpleDateFormat sdf = new SimpleDateFormat(datePattern);
                try {
                    Date startDate = sdf.parse(startTime);
                    Date endDate = sdf.parse(endTime);
                    if (startDate.after(endDate)) {
                        throw new DataXException(CommonErrorCode.CONFIG_ERROR,"startTime not should endTime after.");
                    }
                    long startNano = startDate.getTime();
                    long endNano = endDate.getTime();
                    long oneDayNano = 1000*60*60*24*1;
                    if(endNano-startNano <= oneDayNano){
                        taskNum = 1;
                    }
                    List<List<Long>> timePartition = partition(startNano,endNano,taskNum,null);
                    timePartition.forEach(time->{
                        Configuration sliceConfig = originalConfig.clone();
                        Date s = new Date(time.get(0));
                        Date e = new Date(time.get(1));
                        sliceConfig.set(KeyConstant.TASK_START_TIME,sdf.format(s));
                        sliceConfig.set(KeyConstant.TASK_END_TIME,sdf.format(e));
                        splittedConfigs.add(sliceConfig);
                        s = null;
                        e = null;
                    });

                } catch (ParseException e) {
                    e.printStackTrace();
                }

            }

            return splittedConfigs;
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            String where = originalConfig.getString(KeyConstant.WHERE,"");
            this.tableName = originalConfig.getString(KeyConstant.SA.concat(KeyConstant.POINT).concat(KeyConstant.SA_TABLE));
            this.userName = originalConfig.getString(KeyConstant.USER_NAME,"");
            this.password = originalConfig.getString(KeyConstant.PASSWORD,"");
            this.mysqlUrl = originalConfig.getString(KeyConstant.SA.concat(KeyConstant.POINT).concat(KeyConstant.SA_MYSQL_URL));
            String mysqlVersion = originalConfig.getString(KeyConstant.MYSQL_VERSION,"");
            Boolean useStream = originalConfig.getBool(KeyConstant.USE_STREAM, false);
            log.info("use Stream mode : {}",useStream);
//            校验mysql版本
            String curClassPath = DependencyClassLoader.class.getResource("").getFile();
            String rootPath = curClassPath.substring(curClassPath.lastIndexOf(":")+1, curClassPath.lastIndexOf("!"));
            String path = rootPath.substring(0, rootPath.lastIndexOf("/")).concat("/mysqllib/");
            File f = new File(path);
            File[] files = f.listFiles();
            List<String> optionalValueList = new ArrayList<>();
            if(f.exists() && files != null && files.length > 0){
                for (File file : files) {
                    if(file.isDirectory()){
                        optionalValueList.add(file.getName());
                    }
                }
            }
            if(StrUtil.isBlank(mysqlVersion) || !optionalValueList.contains(mysqlVersion)){
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"mysqlVersion不存在或为空,可选值："+optionalValueList);
            }
            //动态加载mysql相关依赖，解决mysql版本兼容
            DependencyClassLoader.loadClassJar("/mysqllib/"+mysqlVersion);
            if(StrUtil.isBlank(this.tableName) || StrUtil.isBlank(this.mysqlUrl)){
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"mysqlUrl和table不能为空");
            }
            this.linkedTable = " ".concat(originalConfig.getString(KeyConstant.LINKED_TABLE, "")).concat(" ");
            this.rowNumCountSql = "select count(*) from ".concat(tableName).concat(this.linkedTable).concat(StrUtil.isBlank(where)?"":"  where ".concat(where));
            MysqlUtil.setUrl(this.mysqlUrl);
            MysqlUtil.setUser(this.userName);
            MysqlUtil.setPassword(this.password);

            String timeFieldName = originalConfig.getString(KeyConstant.TIME_FIELD_NAME,"");
            this.columnList = originalConfig.getList(KeyConstant.COLUMN, String.class);
            if(Objects.isNull(columnList) || columnList.isEmpty() ){
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"column不能为空！");
            }
            String columnStr = CollectionUtil.join(this.columnList, ",");

            this.useRowNumber = originalConfig.getBool(KeyConstant.USE_ROW_NUMBER, true);
            if(!useRowNumber){
                String startTime = originalConfig.getString(KeyConstant.START_TIME,"");
                String endTime = originalConfig.getString(KeyConstant.END_TIME,"");
                if(StrUtil.isBlank(timeFieldName) || StrUtil.isBlank(startTime) || StrUtil.isBlank(endTime) ){
                    throw new DataXException(CommonErrorCode.CONFIG_ERROR,"配置有误，请检查！");
                }
            }

            this.sql = "select ".concat(columnStr).concat(" from ").concat(this.tableName).concat(this.linkedTable).concat(" where ")
                    .concat(timeFieldName).concat(" >= '{}' and ").concat(timeFieldName).concat(" < '{}'")
                    .concat(StrUtil.isBlank(where)?"":"  and ".concat(where));

            this.sqlRowNum = " select ".concat(columnStr).concat(" from ").concat(this.tableName).concat(this.linkedTable).concat(" where ")
                    .concat(timeFieldName).concat(" >= '{}' and ").concat(timeFieldName).concat(" < '{}'")
                    .concat(StrUtil.isBlank(where)?"":"  and ".concat(where))
                    .concat(" limit {},{}  ");

            this.streamSqlRowNum = " select ".concat(columnStr).concat(" from ").concat(this.tableName).concat(this.linkedTable).concat(" where ")
                    .concat(timeFieldName).concat(" >= '{}' and ").concat(timeFieldName).concat(" < '{}'")
                    .concat(StrUtil.isBlank(where)?"":"  and ".concat(where));

            this.sqlCount = "select count(*) ".concat(" from ").concat(this.tableName).concat(this.linkedTable).concat(" where ")
                    .concat(timeFieldName).concat(" >= '{}' and ").concat(timeFieldName).concat(" < '{}'")
                    .concat(StrUtil.isBlank(where)?"":"  and ".concat(where));

            this.rowNumSql = "select ".concat(columnStr).concat(" from ").concat(this.linkedTable).concat(this.tableName)
                    .concat(StrUtil.isBlank(where)?"":"  where ".concat(where)).concat(" limit {},{}");

            this.streamRowNumSql = "select ".concat(columnStr).concat(" from ").concat(this.linkedTable).concat(this.tableName)
                    .concat(StrUtil.isBlank(where)?"":"  where ".concat(where));

            originalConfig.set(KeyConstant.SQL_TEMPLATE,this.sql);
            originalConfig.set(KeyConstant.SQL_COUNT_TEMPLATE,this.sqlCount);
            originalConfig.set(KeyConstant.ROW_NUM_SQL_TEMPLATE,this.rowNumSql);
            originalConfig.set(KeyConstant.SQL_ROW_NUM_TEMPLATE,this.sqlRowNum);
            originalConfig.set(KeyConstant.STREAM_SQL_ROW_NUM_TEMPLATE,this.streamSqlRowNum);
            originalConfig.set(KeyConstant.STREAM_ROW_NUM_SQL_TEMPLATE,this.streamRowNumSql);

        }

        @Override
        public void destroy() {}

        /**
         * 使用hive row_number函数方式分区
         * @param pageSize
         * @param receivePageSize
         * @return
         */
        public List<List<Long>> supportRowNumberPartition(long pageSize,long receivePageSize){
            List<List<Long>> list = new ArrayList<>();
            Double size = 0D;
            try {
                Connection connection = MysqlUtil.defaultDataSource().getConnection();
                size = SqlExecutor.query(connection, this.rowNumCountSql, new RsHandler<Double>() {
                    @Override
                    public Double handle(ResultSet rs) throws SQLException {
                        if(rs.next()){
                            return rs.getDouble(1);
                        }else{
                            return 0D;
                        }
                    }
                });
                connection.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
                throw new DataXException(ReaderErrorCode.SQL_EXECUTION_ERROR,String.format("sql:%s",this.rowNumCountSql));
            }
            long pageNo = 0;
            long rounds = ((Double)Math.ceil(size / pageSize)).longValue();
            if(rounds <= 1){
                List<Long> l = new ArrayList<>();
                l.add(++pageNo);
                l.add(pageNo);
                list.add(l);
                return list;
            }
            while(pageNo < rounds){
                List<Long> l = new ArrayList<>();
                l.add(++pageNo);
                pageNo += receivePageSize-1;
                l.add(pageNo>rounds?rounds:pageNo);
                list.add(l);
            }
            log.info("当前总数据量：{}，pageSize：{}，每个任务负责的页数：{}，分配的任务数量：{}",size,pageSize,receivePageSize,list.size());
            return list;

        }

        /**
         * 将时间段分片
         * @param startNano 开始时间毫秒值
         * @param endNano 结束时间毫秒值
         * @param taskNum 切分数
         * @param stepSize 步长，若传递此参数则taskNum不生效
         * @return
         */
        public static List<List<Long>> partition(long startNano, long endNano, Integer taskNum,Long stepSize) {
            List<List<Long>> list = new ArrayList<>();

            if(stepSize != null){
                while(startNano < endNano){
                    List<Long> l = new ArrayList<>();
                    l.add(startNano);
                    startNano = startNano + stepSize;
                    if(startNano > endNano){
                        startNano = endNano;
                    }
                    l.add(startNano);
                    list.add(l);
                }
                return list;
            }

            if(taskNum <= 1){
                List<Long> l = new ArrayList<>();
                l.add(startNano);
                l.add(endNano);
                list.add(l);
                return list;
            }else{
                long num = (endNano-startNano)/taskNum;
                while(startNano < endNano){
                    List<Long> l = new ArrayList<>();
                    l.add(startNano);
                    startNano = startNano + num;
                    if(startNano > endNano){
                        startNano = endNano;
                    }
                    l.add(startNano);
                    list.add(l);
                }

            }
            return list;

        }

    }


    @Slf4j
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Task extends Reader.Task {

        private Configuration readerConfig;

        private String  startTime;

        private String  endTime;

        private String  sql;
        /**
         * 当使用时间条件过滤时，如果数据量大到不能拆分时，使用分页查询的sql
         */
        private String  sqlRowNum;

        private String  streamSqlRowNum;

        private String  sqlCount;

        private Long  maxQueryNum;

        private String  rowNumSql;

        private String  streamRowNumSql;

        private String  timePattern;

        private Long timeInterval;
        //      时间分片方式是否统计拆分，默认值true,表示优先使用先统计数量再拉取值，当数据量大时可防止OOM,若为false,则直接拉取数据，不在进行统计，可能导致OOM
        private Boolean timeFieldCount;

        //       当使用时间条件过滤时，如果数据量大到不能拆分时，使用分页
        private Long pageSize;

        private SimpleDateFormat sdf;

        private List<String>  columnList;
        /**
         * 列的简称
         */
        private List<String>  columnNameList;

        private Map<String,Integer>  columnIndexMap;

        private boolean useRowNumber;

        private boolean useStream;

        private long batchListSize;

        private List<BasePlugin.SAPlugin> basePluginList;

        private List<String> sqlColumnList;

        private static Pattern PATTERN = Pattern.compile("\\{(.*?)}");

        @Override
        public void startRead(RecordSender recordSender) {
            long sum = 0;
            if(useRowNumber){
                if(this.useStream){
                    log.info("use Stream Mode [RowNumber] start");
                    sum = supportStreamRowNumber(recordSender);
                    log.info("use Stream Mode [RowNumber] end------,本次查询总数：{}",sum);
                    return ;
                }
                Long startPageNo = readerConfig.getLong(KeyConstant.START_PAGE_NO);
                Long endPageNo = readerConfig.getLong(KeyConstant.END_PAGE_NO);
                Long pageSize = readerConfig.getLong(KeyConstant.PAGE_SIZE);

                if(Objects.isNull(startPageNo) || Objects.isNull(endPageNo) || Objects.isNull(pageSize)
                        || (startPageNo > endPageNo) || pageSize<=0){
                    return;
                }
                log.info("startPageNo:{},endPageNo:{},pageSize:{},start",startPageNo,endPageNo,pageSize);
                sum = supportRowNumber(startPageNo,endPageNo,pageSize,recordSender);
                log.info("startPageNo:{},endPageNo:{},pageSize:{},end------,本次查询总数：{}",startPageNo,endPageNo,pageSize,sum);
            }else{
                if(Objects.isNull(startTime) || Objects.isNull(endTime)){
                    return;
                }
                if(this.useStream){
                    log.info("use Stream Mode [startTime:{},endTime:{}] start",startTime,endTime);
                    sum = supportStreamNotRowNumber(startTime, endTime,recordSender);
                    log.info("use Stream Mode [startTime:{},endTime:{}] end------,本次查询总数：{}",startTime,endTime,sum);
                    return ;
                }
                log.info("startTime:{},endTime:{},start",startTime,endTime);
                sum = notSupportRowNumber(startTime, endTime, recordSender);
                log.info("startTime:{},endTime:{},end------,本次查询总数：{}",startTime,endTime,sum);
            }
        }

        @Override
        public void init() {
            //获取到的是上面task的split方法返回的某一个
            this.readerConfig = super.getPluginJobConf();
            this.startTime = readerConfig.getString(KeyConstant.TASK_START_TIME,null);
            this.endTime = readerConfig.getString(KeyConstant.TASK_END_TIME,null);
            this.timePattern = readerConfig.getString(KeyConstant.DATE_PATTERN,null);
            this.timeFieldCount = readerConfig.getBool(KeyConstant.TIME_FIELD_COUNT,true);
            this.useRowNumber = readerConfig.getBool(KeyConstant.USE_ROW_NUMBER, true);
            this.useStream = readerConfig.getBool(KeyConstant.USE_STREAM, false);
            this.batchListSize = readerConfig.getLong(KeyConstant.STREAM_BATCH_SIZE, 2048);
            if(Objects.isNull(this.timePattern) && !this.useRowNumber){
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"useRowNumber为false时,timePattern是必填项，请检查！");
            }
            if(!Objects.isNull(this.timePattern)){
                this.sdf = new SimpleDateFormat(this.timePattern);
            }
            this.timeInterval = readerConfig.getLong(KeyConstant.TIME_INTERVAL,1000*60*60*24*1L);
            if(Objects.isNull(this.timeInterval) || this.timeInterval<=0){
                this.timeInterval = 1000*60*60*24*1L;
            }

            this.columnList = readerConfig.getList(KeyConstant.COLUMN, String.class);
            List<String> pluginColumnNames = readerConfig.getList(KeyConstant.PLUGIN_COLUMN, new ArrayList<>(),String.class);
            columnIndexMap = new HashMap<>(this.columnList.size()+pluginColumnNames.size());
            List<String>  columnNames = new ArrayList<>(this.columnList.size()+pluginColumnNames.size());
            for (int i = 0; i < this.columnList.size(); i++) {
                String col = this.columnList.get(i);
                col = col.contains(".")?col.substring(col.lastIndexOf(".")+1):col;
                col = col.contains(" ")?col.substring(col.lastIndexOf(" ")+1):col;
                columnIndexMap.put(col,i);
                columnNames.add(col);
            }

            for (int i = 0; i < pluginColumnNames.size(); i++) {
                String col = pluginColumnNames.get(i);
                columnIndexMap.put(col,this.columnList.size()+i);
                columnNames.add(col);
            }

            this.columnNameList = columnNames;

            this.sql = readerConfig.getString(KeyConstant.SQL_TEMPLATE);
            this.sqlCount = readerConfig.getString(KeyConstant.SQL_COUNT_TEMPLATE);
            this.maxQueryNum = readerConfig.getLong(KeyConstant.MAX_QUERY_NUM,50000L);
            this.rowNumSql = readerConfig.getString(KeyConstant.ROW_NUM_SQL_TEMPLATE);
            this.sqlRowNum = readerConfig.getString(KeyConstant.SQL_ROW_NUM_TEMPLATE);
            this.pageSize = readerConfig.getLong(KeyConstant.PAGE_SIZE,200000);

            this.streamSqlRowNum = readerConfig.getString(KeyConstant.STREAM_SQL_ROW_NUM_TEMPLATE);
            this.streamRowNumSql = readerConfig.getString(KeyConstant.STREAM_ROW_NUM_SQL_TEMPLATE);
            this.sqlColumnList = readerConfig.getList(KeyConstant.SQL_COLUMN,new ArrayList<>(),String.class);

            String SaPluginStr = readerConfig.getString(KeyConstant.PLUGIN,"[]");
            List<SaPlugin> SaPluginList = JSONObject.parseArray(SaPluginStr, SaPlugin.class);
            this.basePluginList = new ArrayList<>();
            SaPluginList.forEach(saPlugin -> {
                String pluginName = saPlugin.getName();
                String pluginClass = saPlugin.getClassName();
                Map<String, Object> pluginParam = saPlugin.getParam();
                if(!NullUtil.isNullOrBlank(pluginName) && !NullUtil.isNullOrBlank(pluginClass)){
                    if(Objects.isNull(pluginParam)){
                        pluginParam = new HashMap<>();
                    }
                    this.basePluginList.add(DependencyClassLoader.getBasePlugin(saPlugin.getName(), pluginClass, pluginParam));
                }

            });
        }


        @Override
        public void destroy() {
            this.readerConfig = null;
        }

        private String resolveSql(Map<String, Object> item,String oldSql){
            String sql = oldSql;
            Matcher matcher = PATTERN.matcher(sql);
            while (matcher.find()) {
                String oldKey = matcher.group();
                String key = oldKey.substring(1,matcher.group().length() - 1).trim();
                sql = sql.replace(oldKey, Objects.isNull(item.get(key))?"null":item.get(key).toString());
            }
            return sql;
        }

        public List<Map<String, Object>> addSqlColumns(Map<String, Object> item){
            List<Map<String, Object>> ret = new ArrayList<>();
            ret.add(new HashMap<>(item));
            if(!this.sqlColumnList.isEmpty()){
                Map<String, Object> multiOldProp = null;
                for (String sql : this.sqlColumnList) {
                    String sqlTmp = resolveSql(item,sql);
                    List<Map<String, Object>> data = null;
                    try {
                        data = JdbcUtils.executeQuery(MysqlUtil.defaultDataSource(), sqlTmp);
                        if(Objects.isNull(data) || data.isEmpty()){
                            continue;
                        }
                        int oldSize = ret.size();
                        for (int i = 0; i < oldSize; i++) {
                            multiOldProp = new HashMap<>(ret.get(i));
                            ret.get(i).putAll(data.get(0));
                            for (int j = 1; j < data.size(); j++) {
                                HashMap<String, Object> hashMap = new HashMap<>(multiOldProp);
                                hashMap.putAll(data.get(j));
                                ret.add(hashMap);
                            }
                            multiOldProp = null;
                        }
                    } catch (SQLException throwables) {
                        throw new DataXException(ReaderErrorCode.SQL_EXECUTION_ERROR,String.format("sql:%s",sqlTmp));
                    }
                }
            }
            return ret;
        }

        /**
         * 过时，已使用doPlugin(List<Record> recordCollector,RecordSender recordSender,List<Map<String, Object>> values,int pluginStart,int pluginEnd)替换
         * 只执行单行数据的插件，如果是多行，内部会交给子节点执行，子节点最终执行单行数据时还是会调用该方法
         * @param recordCollector
         * @param recordSender
         * @param values
         * @param pluginStart
         * @param pluginEnd
         */
        @Deprecated
        private void doPlugin(List<Record> recordCollector,RecordSender recordSender,Map<String, Object> values,int pluginStart,int pluginEnd){
            if(Objects.isNull(this.basePluginList)){
                return ;
            }
            for (int i = pluginStart; i < pluginEnd; i++) {
                // 第一次是从入口来的主数据调用，一定是单行；子节点调用时，也是单行
                BasePlugin.SAPlugin saPlugin = this.basePluginList.get(i);
                boolean process = saPlugin.process(values);
                if(!process){
                    break;
                }
                //当前插件执行完后如果数据变成了多行，则需要交给子节点执行
                Object isMultiColumnObj = values.getOrDefault("__$$is_multi_column$$__", false);
                boolean isMultiColumn = Boolean.parseBoolean(isMultiColumnObj.toString());
                if(!isMultiColumn) {
                    continue;
                }
                //交给子节点执行
                List<Map<String, Object>> data = (List<Map<String, Object>>)values.get("data");
                if(!Objects.isNull(data) && !values.isEmpty()){
                    for (int j = 0; j < data.size(); j++) {
                        doPluginSub(recordCollector,recordSender,data.get(j),i+1,pluginEnd);
                    }
                    return;
                }
            }
        }

        /**
         * 过时，已使用doPlugin(List<Record> recordCollector,RecordSender recordSender,List<Map<String, Object>> values,int pluginStart,int pluginEnd)替换
         * 子节点执行多行数据的插件，内部单行数据的执行还是交给doPlugin执行
         * @param recordCollector
         * @param recordSender
         * @param values
         * @param pluginStart
         * @param pluginEnd
         */
        @Deprecated
        private void doPluginSub(List<Record> recordCollector, RecordSender recordSender, Map<String, Object> values, int pluginStart, int pluginEnd) {
            if(Objects.isNull(this.basePluginList)){
                return ;
            }
            if(pluginStart>0 && pluginStart < this.basePluginList.size()){
                BasePlugin.SAPlugin saPlugin = this.basePluginList.get(pluginStart);
                boolean process = saPlugin.process(values);
                if(!process){
                    return;
                }
            }
            //该行数据所有插件执行完后需要把数据收集起来，最后一个插件执行完如果变成了多行，则要循环收集
            if(pluginStart+1 >= pluginEnd){
                Object isMultiColumnObj = values.getOrDefault("__$$is_multi_column$$__", false);
                boolean isMultiColumn = Boolean.parseBoolean(isMultiColumnObj.toString());
                if(isMultiColumn){
                    List<Map<String, Object>> data = (List<Map<String, Object>>)values.get("data");
                    if(!Objects.isNull(data) && !values.isEmpty()){
                        for (int j = 0; j < data.size(); j++) {
                            Map<String, Object> val = data.get(j);
                            if(!Objects.isNull(val.get("__$$is_multi_column$$__"))){
                                return ;
                            }
                        Record record = recordSender.createRecord();
                        doBuildRecord(record,val);
                        recordCollector.add(record);
                        }
                    }
                }else{
                    Record record = recordSender.createRecord();
                    doBuildRecord(record,values);
                    recordCollector.add(record);
                }
                return;
            }
            //当前节点执行完后，交给下一个插件执行，如果执行完时是多行，则循环依次单行调用doPlugin方法去执行剩下的所有插件
            Object isMultiColumnObj = values.getOrDefault("__$$is_multi_column$$__", false);
            boolean isMultiColumn = Boolean.parseBoolean(isMultiColumnObj.toString());
            if(!isMultiColumn) {
                doPluginSub(recordCollector,recordSender,values,pluginStart+1,pluginEnd);
            }
            List<Map<String, Object>> data = (List<Map<String, Object>>)values.get("data");
            if(!Objects.isNull(data) && !values.isEmpty()){
                for (int j = 0; j < data.size(); j++) {
                    doPlugin(recordCollector,recordSender,data.get(j),pluginStart+1,pluginEnd);
                }
            }
        }

        /**
         *批量执行插件
         * @param recordCollector 数据收集器
         * @param recordSender 数据发送器
         * @param values 批量待处理出具
         * @param pluginStart 插件开始下标
         * @param pluginEnd 插件结束下标
         */
        private void doPlugin(List<Record> recordCollector,RecordSender recordSender,List<Map<String, Object>> values,int pluginStart,int pluginEnd){
            if(Objects.isNull(this.basePluginList)){
                return ;
            }
            if((pluginStart < pluginEnd) && pluginStart < this.basePluginList.size() && pluginStart >=0){
                BasePlugin.SAPlugin saPlugin = this.basePluginList.get(pluginStart);
                if(saPlugin.isMulti()){
                    boolean process = saPlugin.process(values);
                    if(!process){
                        return;
                    }
                    //交给下个插件节点执行
                    doPlugin(recordCollector,recordSender,values,pluginStart+1,pluginEnd);
                }else{
                    List<Map<String, Object>> data = new ArrayList<>();
                    for (Map<String, Object> value : values) {
                        boolean process = saPlugin.process(value);
                        if(!process){
                            break;
                        }
                        //当前插件执行完后数据变成了多行
                        Object isMultiColumnObj = value.getOrDefault("__$$is_multi_column$$__", false);
                        boolean isMultiColumn = Boolean.parseBoolean(isMultiColumnObj.toString());
                        if(isMultiColumn) {
                            data.addAll((List<Map<String, Object>>)value.get("data"));
                        }else{
                            data.add(value);
                        }
                    }
                    values = null;
                    //交给下个插件节点执行
                    doPlugin(recordCollector,recordSender,data,pluginStart+1,pluginEnd);
                }
            }
            if(pluginStart >= pluginEnd){
                // 所有插件都执行完了，收集Record
                for (Map<String, Object> value : values) {
                    Record record = recordSender.createRecord();
                    doBuildRecord(record,value);
                    recordCollector.add(record);
                }
            }
        }


        /**
         * 过时，已使用buildRecord(List<Record> recordCollector,RecordSender recordSender, List<Map<String, Object>> itemValues,String startTime,String endTime)替换
         *
         * @param recordCollector 数据收集器
         * @param recordSender 数据发送器
         * @param item 当前行数据
         * @param startTime 当前行数据主表查询的开始时间
         * @param endTime 当前行数据主表查询的结束时间
         * @return
         */
        @Deprecated
        private List<Record> buildRecord(List<Record> recordCollector,RecordSender recordSender, Map<String, Object> item,String startTime,String endTime){
            if(Objects.isNull(item) || item.isEmpty()){
                return recordCollector;
            }
            if(StrUtil.isNotBlank(startTime)){
                item.put("$$startTime$$",startTime);
            }
            if(StrUtil.isNotBlank(endTime)){
                item.put("$$endTime$$",endTime);
            }
            if(!Objects.isNull(this.basePluginList)){
                doPlugin(recordCollector, recordSender,item, 0, this.basePluginList.size());
                Object isMultiColumnObj = item.getOrDefault("__$$is_multi_column$$__", false);
                boolean isMultiColumn = Boolean.parseBoolean(isMultiColumnObj.toString());
                if(!isMultiColumn){
                    Record record = recordSender.createRecord();
                    doBuildRecord(record,item);
                    recordCollector.add(record);
                }
            }else{
                Record record = recordSender.createRecord();
                doBuildRecord(record,item);
                recordCollector.add(record);
            }
            return recordCollector;
        }

        /**
         * 批量执行插件并构建Record
         * @param recordCollector 数据收集器
         * @param recordSender 数据发送器
         * @param itemValues 批量待处理出具
         * @param startTime 这一批数据主表查询的开始时间
         * @param endTime 这一批数据主表查询的结束时间
         * @return
         */
        private List<Record> buildRecord(List<Record> recordCollector,RecordSender recordSender, List<Map<String, Object>> itemValues,String startTime,String endTime){
            if(Objects.isNull(itemValues) || itemValues.isEmpty()){
                return recordCollector;
            }
            itemValues.forEach(item -> {
                if(StrUtil.isNotBlank(startTime)){
                    item.put("$$startTime$$",startTime);
                }
                if(StrUtil.isNotBlank(endTime)){
                    item.put("$$endTime$$",endTime);
                }
            });
            if(!Objects.isNull(this.basePluginList)){
                doPlugin(recordCollector, recordSender,itemValues, 0, this.basePluginList.size());
            }else{
                itemValues.forEach(item -> {
                    Record record = recordSender.createRecord();
                    doBuildRecord(record,item);
                    recordCollector.add(record);
                });
            }
            return recordCollector;
        }

        private void doBuildRecord(Record record,Map<String, Object> item ) {
            if(Objects.isNull(item) || item.isEmpty()){
                return;
            }
            Map<String,String> keyMap = new HashMap<>();
            for (String key : item.keySet()) {
                String k = keyMap.get(keyMap);
                Object value = item.get(key);
                if(Objects.isNull(k)){
                    k = key.contains(".")?key.substring(key.lastIndexOf(".")+1):key;
                    keyMap.put(key,k);
                }
                if (this.columnNameList.contains(k)) {
                    Integer index = columnIndexMap.get(k);
                    if(Objects.isNull(value)){
                        record.setColumn(index,new StringColumn((String) value));
                    }else if(value instanceof String){
                        record.setColumn(index,new StringColumn((String) value));
                    }else if(TypeUtil.isPrimitive(value,Boolean.class)){
                        record.setColumn(index,new BoolColumn(Boolean.parseBoolean(value.toString()) ));
                    }else if(TypeUtil.isPrimitive(value,Byte.class) || TypeUtil.isPrimitive(value,Short.class) ||
                            TypeUtil.isPrimitive(value,Integer.class) || TypeUtil.isPrimitive(value,Long.class) ){
                        record.setColumn(index,new LongColumn(Long.parseLong(value.toString())));
                    }else if(TypeUtil.isPrimitive(value,Float.class) || TypeUtil.isPrimitive(value,Double.class)){
                        record.setColumn(index,new DoubleColumn(value.toString()));
                    }else if(value instanceof BigInteger){
                        record.setColumn(index,new LongColumn((BigInteger)value));
                    }else if(value instanceof Number){
                        record.setColumn(index,new DoubleColumn(value.toString()));
                    }else if(value instanceof Date){
                        record.setColumn(index,new DateColumn( (Date)value) );
                    }else if(value instanceof LocalDate){
                        record.setColumn(index,new DateColumn( Date.from(((LocalDate)value).atStartOfDay().atZone(ZoneId.systemDefault()).toInstant()) ));
                    }else if(value instanceof LocalDateTime){
                        record.setColumn(index,new DateColumn( Date.from(((LocalDateTime)value).atZone(ZoneId.systemDefault()).toInstant()) ));
                    }else if(value instanceof java.sql.Date){
                        record.setColumn(index,new DateColumn( new Date(((java.sql.Date)value).getTime()) ));
                    }else if(value instanceof java.sql.Timestamp){
                        record.setColumn(index,new DateColumn( (Date)value) );
                    }else if((value instanceof Byte[]) || (value instanceof byte[])){
                        record.setColumn(index,new BytesColumn( (byte[])value) );
                    }
                    else{
                        DataXException dataXException = DataXException
                                .asDataXException(
                                        ReaderErrorCode.UNSUPPORTED_TYPE, String.format("不支持的数据类型 column:%s,type:%s,value:%s",k, value.getClass().getName(), value));
                        TaskPluginCollector taskPluginCollector = super.getTaskPluginCollector();
                        taskPluginCollector.collectDirtyRecord(record,dataXException);
                    }
                }
            }
        }


        private long supportStreamRowNumber(RecordSender recordSender) {
            AtomicLong sum = new AtomicLong(0);
            Connection conn = null;
            PreparedStatement stmt = null;
            ResultSet rs = null;
            try {
                try {
                    conn = MysqlUtil.defaultDataSource().getConnection();
                    log.info("Stream Mode Row Number sql : {}",this.streamRowNumSql);
                    stmt = conn.prepareStatement(this.streamRowNumSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    stmt.setFetchSize(Integer.MIN_VALUE);
                    rs = stmt.executeQuery();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                try {
                    List<Map<String, Object>> batchList = new ArrayList<>(2048);
                    ResultSetMetaData rsMeta = rs.getMetaData();
                    while(rs.next()){
                        try {
                            Map<String, Object> row = new LinkedHashMap<>();
                            for (int i = 0, size = rsMeta.getColumnCount(); i < size; ++i) {
                                String columName = rsMeta.getColumnLabel(i + 1);
                                Object value = rs.getObject(i + 1);
                                row.put(columName, value);
                            }
                            long siz = sum.addAndGet(1);
                            if(siz % 1000 == 0){
                                log.info("stream mode [RowNumber] read cur data size : {}",siz);
                            }
                            if(batchList.size() >= this.batchListSize){
                                List<Record> vals = new ArrayList<>();
                                List<Map<String, Object>> valueList = new ArrayList<>();
                                for (Map<String, Object> rowTem : batchList) {
                                    List<Map<String, Object>> valueListTemp = addSqlColumns(rowTem);
                                    if(ObjectUtil.isNotEmpty(valueListTemp)){
                                        valueList.addAll(valueListTemp);
                                    }
                                }
                                this.buildRecord(vals,recordSender, valueList,null,null);
                                if(!vals.isEmpty()){
                                    vals.forEach(v-> {
                                        if(!Objects.isNull(v) && v.getColumnNumber() > 0){
                                            recordSender.sendToWriter(v);
                                        }
                                    });
                                }
                                vals.clear();
                                vals = null;
                                valueList.clear();
                                valueList = null;
                                batchList.clear();
                            }else{
                                batchList.add(row);
                            }

                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }

                    if(!batchList.isEmpty()){
                        List<Record> vals = new ArrayList<>();
                        List<Map<String, Object>> valueList = new ArrayList<>();
                        for (Map<String, Object> rowTem : batchList) {
                            List<Map<String, Object>> valueListTemp = addSqlColumns(rowTem);
                            if(ObjectUtil.isNotEmpty(valueListTemp)){
                                valueList.addAll(valueListTemp);
                            }
                        }
                        this.buildRecord(vals,recordSender, valueList,null,null);
                        if(!vals.isEmpty()){
                            vals.forEach(v-> {
                                if(!Objects.isNull(v) && v.getColumnNumber() > 0){
                                    recordSender.sendToWriter(v);
                                }
                            });
                        }
                        vals.clear();
                        vals = null;
                        valueList.clear();
                        valueList = null;
                        batchList.clear();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return sum.get();
            } finally {
                if(rs != null){
                    try {
                        rs.close();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
                if(stmt != null){
                    try {
                        stmt.close();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
                if(conn != null){
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }

            }
        }

        /**
         * 使用mysql limit函数查询分段
         * @param startPageNo
         * @param endPageNo
         * @param pageSize
         * @param recordSender
         * @return
         */
        private long supportRowNumber(Long startPageNo,Long endPageNo,Long pageSize,RecordSender recordSender){
            AtomicLong sum = new AtomicLong(0);
            for (; startPageNo <= endPageNo; startPageNo++) {
                sum.addAndGet(doSupportRowNumber(startPageNo,pageSize,recordSender));
            }
            return sum.get();
        }

        /**
         * 使用mysql limit查询分段
         * @param pageNo
         * @param pageSize
         * @param recordSender
         * @return
         */
        private long doSupportRowNumber(long pageNo,long pageSize,RecordSender recordSender){
            String sqlTmp = StrUtil.format(this.rowNumSql, (pageNo-1)*pageSize,pageSize);
            log.info("sql:{}",sqlTmp);
            List<Map<String, Object>> data = null;
            try {
                data = JdbcUtils.executeQuery(MysqlUtil.defaultDataSource(), sqlTmp);
            } catch (SQLException throwables) {
                throw new DataXException(ReaderErrorCode.SQL_EXECUTION_ERROR,String.format("sql:%s",sqlTmp));
            }
            if(Objects.isNull(data) || data.isEmpty()){
                return 0;
            }
            long size = 0;
            List<Map<String, Object>> valueList = new ArrayList<>();
            for (Map<String, Object> item : data) {
                valueList.addAll(addSqlColumns(item));
            }
            data.clear();
            List<Record> vals = new ArrayList<>();
            this.buildRecord(vals,recordSender, valueList,null,null);
            if(!vals.isEmpty()){
                vals.forEach(v-> {
                    if(!Objects.isNull(v) && v.getColumnNumber() > 0){
                        recordSender.sendToWriter(v);
                    }
                });
            }
            size += vals.size();
            vals = null;
            return size;
        }


        private long supportStreamNotRowNumber(String startTime, String endTime, RecordSender recordSender) {
            AtomicLong sum = new AtomicLong(0);
            try {
                Date start = sdf.parse(startTime);
                Date end = sdf.parse(endTime);
                long startNano = start.getTime();
                long endNano = end.getTime();

                //间隔大于一天，每次只查一天的数据
                String startStr = null;
                String endStr = null;
                long oneDayNano = this.timeInterval;
                if(endNano-startNano > oneDayNano){
                    while (endNano-startNano > oneDayNano){
                        startStr = sdf.format(new Date(startNano));
                        long endTmp = (startNano+oneDayNano)>endNano?endNano:(startNano+oneDayNano);
                        endStr = sdf.format(new Date(endTmp));
                        sum.addAndGet(doNotSupportRowNumberStream(startStr,endStr,recordSender));
                        startNano = endTmp;
                    }
                    //最后一次可能没到endNano，并且不满足大于oneDayNano
                    if( endNano-startNano > 0){
                        startStr = sdf.format(new Date(startNano));
                        sum.addAndGet(doNotSupportRowNumberStream(startStr,endTime,recordSender));
                    }
                }else{
                    sum.addAndGet(doNotSupportRowNumberStream(startTime,endTime,recordSender));
                }

            } catch (ParseException e) {
                log.error("error:{}",e);
            }

            return sum.get();
        }

        private long doNotSupportRowNumberStream(String startTime, String endTime, RecordSender recordSender) {
            if(startTime.equals(endTime)){
                return 0;
            }

            AtomicLong sum = new AtomicLong(0);
            Connection conn = null;
            PreparedStatement stmt = null;
            ResultSet rs = null;
            try {
                try {
                    conn = MysqlUtil.defaultDataSource().getConnection();
                    String sqlTemp = StrUtil.format(this.streamSqlRowNum, startTime, endTime);
                    log.info("stream mode sql : {}",sqlTemp);
                    stmt = conn.prepareStatement(sqlTemp, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    stmt.setFetchSize(Integer.MIN_VALUE);
                    rs = stmt.executeQuery();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                try {
                    List<Map<String, Object>> batchList = new ArrayList<>(2048);
                    ResultSetMetaData rsMeta = rs.getMetaData();
                    while(rs.next()){
                        try {
                            Map<String, Object> row = new LinkedHashMap<>();
                            for (int i = 0, size = rsMeta.getColumnCount(); i < size; ++i) {
                                String columName = rsMeta.getColumnLabel(i + 1);
                                Object value = rs.getObject(i + 1);
                                row.put(columName, value);
                            }
                            long siz = sum.addAndGet(1);
                            if(siz % 1000 == 0){
                                log.info("stream mode startTime : {} , endTime : {}  read cur data size : {}", startTime, endTime,siz);
                            }
                            if(batchList.size() >= this.batchListSize){
                                List<Record> vals = new ArrayList<>();
                                List<Map<String, Object>> valueList = new ArrayList<>();
                                for (Map<String, Object> rowTem : batchList) {
                                    List<Map<String, Object>> valueListTemp = addSqlColumns(rowTem);
                                    if(ObjectUtil.isNotEmpty(valueListTemp)){
                                        valueList.addAll(valueListTemp);
                                    }
                                }
                                this.buildRecord(vals,recordSender, valueList,startTime, endTime);
                                if(!vals.isEmpty()){
                                    vals.forEach(v-> {
                                        if(!Objects.isNull(v) && v.getColumnNumber() > 0){
                                            recordSender.sendToWriter(v);
                                        }
                                    });
                                }
                                vals.clear();
                                vals = null;
                                valueList.clear();
                                valueList = null;
                                batchList.clear();
                            }else{
                                batchList.add(row);
                            }
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                    if(!batchList.isEmpty()){
                        List<Record> vals = new ArrayList<>();
                        List<Map<String, Object>> valueList = new ArrayList<>();
                        for (Map<String, Object> rowTem : batchList) {
                            List<Map<String, Object>> valueListTemp = addSqlColumns(rowTem);
                            if(ObjectUtil.isNotEmpty(valueListTemp)){
                                valueList.addAll(valueListTemp);
                            }
                        }
                        this.buildRecord(vals,recordSender, valueList,startTime, endTime);
                        if(!vals.isEmpty()){
                            vals.forEach(v-> {
                                if(!Objects.isNull(v) && v.getColumnNumber() > 0){
                                    recordSender.sendToWriter(v);
                                }
                            });
                        }
                        vals.clear();
                        vals = null;
                        valueList.clear();
                        valueList = null;
                        batchList.clear();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return sum.get();
            } finally {
                if(rs != null){
                    try {
                        rs.close();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
                if(stmt != null){
                    try {
                        stmt.close();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
                if(conn != null){
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }

            }
        }

        /**
         * 使用时间查询分段
         * @param startTime
         * @param endTime
         * @param recordSender
         * @return
         */
        private long notSupportRowNumber(String startTime,String endTime,RecordSender recordSender){
            AtomicLong sum = new AtomicLong(0);
            try {
                Date start = sdf.parse(startTime);
                Date end = sdf.parse(endTime);
                long startNano = start.getTime();
                long endNano = end.getTime();

                //间隔大于一天，每次只查一天的数据
                String startStr = null;
                String endStr = null;
                long oneDayNano = this.timeInterval;
                if(endNano-startNano > oneDayNano){
                    while (endNano-startNano > oneDayNano){
                        startStr = sdf.format(new Date(startNano));
                        long endTmp = (startNano+oneDayNano)>endNano?endNano:(startNano+oneDayNano);
                        endStr = sdf.format(new Date(endTmp));
                        if(this.timeFieldCount){
                            sum.addAndGet(doNotSupportRowNumber(startStr,endStr,recordSender));
                        }else{
                            sum.addAndGet(doNotSupportRowNumberAndNotCount(startStr,endStr,recordSender));
                        }
                        startNano = endTmp;
                    }
                    //最后一次可能没到endNano，并且不满足大于oneDayNano
                    if( endNano-startNano > 0){
                        startStr = sdf.format(new Date(startNano));
                        if(this.timeFieldCount){
                            sum.addAndGet(doNotSupportRowNumber(startStr,endTime,recordSender));
                        }else{
                            sum.addAndGet(doNotSupportRowNumberAndNotCount(startStr,endTime,recordSender));
                        }

                    }
                }else{
                    if(this.timeFieldCount){
                        sum.addAndGet(doNotSupportRowNumber(startTime,endTime,recordSender));
                    }else{
                        sum.addAndGet(doNotSupportRowNumberAndNotCount(startTime,endTime,recordSender));
                    }
                }

            } catch (ParseException e) {
                log.error("error:{}",e);
            }

            return sum.get();

        }

        private long doNotSupportRowNumberAndNotCount(String startTime,String endTime,RecordSender recordSender){
            if(startTime.equals(endTime)){
                return 0;
            }
            String sqlTmp = StrUtil.format(this.sql, startTime, endTime);
            log.info("sql:{}",sqlTmp);
            List<Map<String, Object>> data = null;
            try {
                data = JdbcUtils.executeQuery(MysqlUtil.defaultDataSource(), sqlTmp);
            } catch (SQLException throwables) {
                throwables.printStackTrace();
                throw new DataXException(ReaderErrorCode.SQL_EXECUTION_ERROR,String.format("sql:%s",sqlTmp));
            }
            if(Objects.isNull(data) || data.isEmpty()){
                log.info("startTime:{},endTime:{}，size:0",startTime, endTime);
                return 0;
            }
            long size = 0;
            List<Map<String, Object>> valueList = new ArrayList<>();
            for (Map<String, Object> item : data) {
                valueList.addAll(addSqlColumns(item));
            }
            data.clear();
            List<Record> vals = new ArrayList<>();
            this.buildRecord(vals,recordSender, valueList,startTime,endTime);
            if(!vals.isEmpty()){
                vals.forEach(v-> {
                    if(!Objects.isNull(v) && v.getColumnNumber() > 0){
                        recordSender.sendToWriter(v);
                    }
                });
            }
            size += vals.size();
            vals = null;
            return size;
        }


        /**
         * 使用时间查询分段
         * @param startTime
         * @param endTime
         * @param recordSender
         * @return
         */
        private long doNotSupportRowNumber(String startTime,String endTime,RecordSender recordSender){
            if(startTime.equals(endTime)){
                return 0;
            }
            String sqlCountTmp = StrUtil.format(this.sqlCount, startTime, endTime);
            Long count = 0L;
            try {
                Connection connection = MysqlUtil.defaultDataSource().getConnection();
                count = SqlExecutor.query(connection, sqlCountTmp, new RsHandler<Long>() {
                    @Override
                    public Long handle(ResultSet rs) throws SQLException {
                        if(rs.next()){
                            return rs.getLong(1);
                        }else{
                            return 0L;
                        }
                    }
                });
                connection.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
                throw new DataXException(ReaderErrorCode.SQL_EXECUTION_ERROR,String.format("sql:%s",sqlCountTmp));
            }

            long maxQueryNum = this.maxQueryNum;
            if(new Long(0).equals(count)){
                log.info("startTime:{},endTime:{},count:{}",startTime,endTime,0);
                return 0;
            }

            List<Map<String, Object>> data = new ArrayList<>();
            if(count > maxQueryNum){
                log.info("startTime:{},endTime:{},count:{},加入队列中将进行拆分。",startTime,endTime,count);
                AtomicLong finallyCount = new AtomicLong(0);
//                最好不要用递归，这里递归可能导致栈内存溢出
                Queue<TimeNode> queue = new LinkedList<TimeNode>();
                queue.add(new TimeNode(startTime,endTime,this.sdf,true,null,null,count,null));
                TimeNode t;
                StringBuilder sb;
                //记录分割后的时间段的兄弟时间段的总数
                Map<String,Long> periodNum = new HashMap<>(128);
                while((t = queue.poll())!=null){
                    //如果程序中断，可使用工具恢复进度
                    log.info("当前处理：{} --> {}",t.getStartTime(),t.getEndTime());
                    sb = new StringBuilder();
                    for (TimeNode timeNode : queue) {
                        sb.append("\t").append(timeNode.getStartTime()).append(" --> ").append(timeNode.getEndTime()).append("\n");
                    }
                    log.info("待处理：\n{}",sb.toString());
                    sb = null;
                    //totalNum只有第一次有值，后面拆分的都没有值，后面的手动赋值在这里是拿不到的，可以将第一次的拆分放在外面
                    if(t.getIsCount() && t.getTotalNum() > maxQueryNum){
                        TimeNode[] splitTimeNode = t.split();
                        TimeNode left = splitTimeNode[0];
                        TimeNode right = splitTimeNode[1];
                        //在第二次及以后拆分的parentTotalNum为空，手动设置
                        left.setParentTotalNum(t.getTotalNum());
                        right.setParentTotalNum(t.getTotalNum());
                        queue.add(left);
                        queue.add(right);
                    }else{
                        if(t.getStartTime().equals(t.getEndTime())){
                            log.info("startTime:{},endTime:{},相同，size:0",t.getStartTime(), t.getEndTime());
                            continue;
                        }
                        String delimiter =  " -> ";
                        String periodNumKey = t.getBrotherStartTime() + delimiter + t.getBrotherEndTime();
                        Long brotherNum = periodNum.get(periodNumKey);
                        Long ct = 0L;
                        if(brotherNum == null){
                            String countTmp = StrUtil.format(this.sqlCount, t.getStartTime(), t.getEndTime());
                            try {
                                Connection connection = MysqlUtil.defaultDataSource().getConnection();
                                ct = SqlExecutor.query(connection, countTmp, new RsHandler<Long>() {
                                    @Override
                                    public Long handle(ResultSet rs) throws SQLException {
                                        if(rs.next()){
                                            return rs.getLong(1);
                                        }else{
                                            return 0L;
                                        }
                                    }
                                });
                                connection.close();
                            } catch (SQLException throwables) {
                                throwables.printStackTrace();
                                throw new DataXException(ReaderErrorCode.SQL_EXECUTION_ERROR,String.format("sql:%s",countTmp));
                            }


                            periodNum.put(t.getStartTime() + delimiter + t.getEndTime(),ct);
                            t.setTotalNum(ct);
                        }else{
                            ct = t.getParentTotalNum()-brotherNum;
                            t.setTotalNum(ct);
                            log.info("startTime:{},endTime:{},获取到缓存size:{}",t.getStartTime(), t.getEndTime(),ct);
                            periodNum.remove(periodNumKey);
                        }

                        if(ct > maxQueryNum){
//                            因为采用的是二分拆法，当间隔小于2秒时，还是大于maxQueryNum，则采用分页方式拉取数据，否则可能导致OOM，这样的情况应该是少数
                            if( (t.endTime2Date().getTime() - t.startTime2Date().getTime()) < 2000 ){
                                long rounds = ((Double)Math.ceil(ct / (this.pageSize*1.0))).longValue();
                                long curPage = 1;
                                log.info("startTime:{},endTime:{}，size:{},使用时间已不能再拆分,采用分页方式.默认分页大小:200000,默认分页大小可通过配置pageSize参数修改",t.getStartTime(), t.getEndTime(),ct);
                                while(curPage <= rounds){
                                    log.info("startTime:{},endTime:{}，curPage:{},totalPages:{},pageSize:{}",t.getStartTime(), t.getEndTime(),curPage,rounds,this.pageSize);
                                    String sqlRowNum = StrUtil.format(this.sqlRowNum, t.getStartTime(), t.getEndTime(),(curPage-1)*this.pageSize,this.pageSize);
                                    log.info("sql:{}",sqlRowNum);
                                    try {
                                        data = JdbcUtils.executeQuery(MysqlUtil.defaultDataSource(), sqlRowNum);
                                    } catch (SQLException throwables) {
                                        throw new DataXException(ReaderErrorCode.SQL_EXECUTION_ERROR,String.format("sql:%s",sqlRowNum));
                                    }
                                    long size = 0;
                                    List<Map<String, Object>> valueList = new ArrayList<>();
                                    for (Map<String, Object> item : data) {
                                        valueList.addAll(addSqlColumns(item));
                                    }
                                    data.clear();
                                    List<Record> vals = new ArrayList<>();
                                    this.buildRecord(vals,recordSender, valueList,t.getStartTime(), t.getEndTime());
                                    if(!vals.isEmpty()){
                                        vals.forEach(v-> {
                                            if(!Objects.isNull(v) && v.getColumnNumber() > 0){
                                                recordSender.sendToWriter(v);
                                            }
                                        });
                                    }
                                    size += vals.size();
                                    vals = null;
                                    finallyCount.addAndGet(size);
                                    curPage++;
                                }
                            }else{
                                log.info("startTime:{},endTime:{},count:{},加入队列中将进行拆分。",t.getStartTime(),t.getEndTime(),ct);
                                TimeNode[] splitTimeNode = t.split();
                                TimeNode left = splitTimeNode[0];
                                TimeNode right = splitTimeNode[1];
                                //在第二次及以后拆分的parentTotalNum为空，手动设置
                                left.setParentTotalNum(ct);
                                right.setParentTotalNum(ct);
                                queue.add(left);
                                queue.add(right);
//                                queue.add(new TimeNode(t.getStartTime(),t.getEndTime(),this.sdf,true));
                            }
                        }else{
                            if(new Long(0).equals(ct)){
                                log.info("startTime:{},endTime:{}，size:0",t.getStartTime(), t.getEndTime());
                                continue;
                            }
                            String sqlTmp = StrUtil.format(this.sql, t.getStartTime(), t.getEndTime());
                            log.info("sql:{}",sqlTmp);
                            try {
                                data = JdbcUtils.executeQuery(MysqlUtil.defaultDataSource(), sqlTmp);
                            } catch (SQLException throwables) {
                                throwables.printStackTrace();
                                throw new DataXException(ReaderErrorCode.SQL_EXECUTION_ERROR,String.format("sql:%s",sqlTmp));
                            }
                            if(Objects.isNull(data) || data.isEmpty()){
                                continue;
                            }
                            long size = 0;
                            List<Map<String, Object>> valueList = new ArrayList<>();
                            for (Map<String, Object> item : data) {
                                valueList.addAll(addSqlColumns(item));
                            }
                            data.clear();
                            List<Record> vals = new ArrayList<>();
                            this.buildRecord(vals,recordSender, valueList, t.getStartTime(), t.getEndTime());
                            if(!vals.isEmpty()){
                                vals.forEach(v-> {
                                    if(!Objects.isNull(v) && v.getColumnNumber() > 0){
                                        recordSender.sendToWriter(v);
                                    }
                                });
                            }
                            size += vals.size();
                            vals = null;
                            finallyCount.addAndGet(size);
                        }
                    }
                }

                return finallyCount.get();
            }else{
                String sqlTmp = StrUtil.format(this.sql, startTime, endTime);
                log.info("sql:{}",sqlTmp);
                try {
                    data = JdbcUtils.executeQuery(MysqlUtil.defaultDataSource(), sqlTmp);
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                    throw new DataXException(ReaderErrorCode.SQL_EXECUTION_ERROR,String.format("sql:%s",sqlTmp));
                }
                if(Objects.isNull(data) || data.isEmpty()){
                    return 0;
                }
                long size = 0;
                List<Map<String, Object>> valueList = new ArrayList<>();
                for (Map<String, Object> item : data) {
                    valueList.addAll(addSqlColumns(item));
                }
                data.clear();
                List<Record> vals = new ArrayList<>();
                this.buildRecord(vals,recordSender, valueList,startTime, endTime);
                if(!vals.isEmpty()){
                    vals.forEach(v-> {
                        if(!Objects.isNull(v) && v.getColumnNumber() > 0){
                            recordSender.sendToWriter(v);
                        }
                    });
                }
                size += vals.size();
                vals = null;
                return size;
            }
        }

    }

    @Slf4j
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class TimeNode{
        private String startTime;
        private String endTime;
        private SimpleDateFormat sdf;
        private Boolean isCount = false;

        private String brotherStartTime;
        private String brotherEndTime;
        private Long totalNum;
        private Long parentTotalNum;

        public Date startTime2Date(){
            try {
                return sdf.parse(startTime);
            } catch (ParseException e) {
                e.printStackTrace();
                return null;
            }
        }

        public Date endTime2Date(){
            try {
                return sdf.parse(endTime);
            } catch (ParseException e) {
                e.printStackTrace();
                return null;
            }
        }

        public TimeNode[] split(){
            long time1 = this.startTime2Date().getTime();
            long time2 = this.endTime2Date().getTime();
            long t = (time2-time1)/2;
            TimeNode[] timeNodes = new TimeNode[2];
            String leftStartTime = this.sdf.format(new Date(time1));
            String leftEndTime = this.sdf.format(new Date(time1 + t));
            String rightStartTime = this.sdf.format(new Date(time2));
            timeNodes[0] = new TimeNode(leftStartTime,leftEndTime,this.sdf,false,leftEndTime,rightStartTime,null,this.totalNum);
            timeNodes[1] = new TimeNode(timeNodes[0].getEndTime(),rightStartTime,this.sdf,false,leftStartTime,leftEndTime,null,this.totalNum);
            return timeNodes;
        }

    }


}
