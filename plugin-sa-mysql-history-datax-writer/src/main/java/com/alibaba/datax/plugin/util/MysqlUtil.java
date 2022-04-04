package com.alibaba.datax.plugin.util;


import cn.hutool.core.lang.Assert;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.druid.pool.DruidDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.shardingjdbc.api.yaml.YamlShardingDataSourceFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

@Slf4j
public class MysqlUtil {

    private static String url;

    private static String user;

    private static String password;

    private static boolean sharding = false;

    private static String shardingYamlFilePath;

    private static String driverClassName = "com.mysql.jdbc.Driver";

    private static DataSource defaultDataSource;

    public static void setUrl(String url) {
        MysqlUtil.url = url;
    }

    public static void setUser(String user) {
        MysqlUtil.user = user;
    }

    public static void setPassword(String password) {
        MysqlUtil.password = password;
    }

    public static void setDriverClassName(String driverClassName) {
        MysqlUtil.driverClassName = driverClassName;
    }

    public static boolean isSharding() {
        return sharding;
    }

    public static void setSharding(boolean sharding) {
        MysqlUtil.sharding = sharding;
    }

    public static String getShardingYamlFilePath() {
        return shardingYamlFilePath;
    }

    public static void setShardingYamlFilePath(String shardingYamlFilePath) {
        MysqlUtil.shardingYamlFilePath = shardingYamlFilePath;
    }

    public static DataSource getDataSource(String url, String user, String password, String driverClassName) {
        DruidDataSource datasource = new DruidDataSource();
        datasource.setUrl(url);
        datasource.setUsername(user);
        datasource.setPassword(password);
        datasource.setDriverClassName(driverClassName);
        return datasource;
    }

    public static DataSource getDataSource(String url, String user, String password) {
        return MysqlUtil.getDataSource(url, user, password, "com.mysql.jdbc.Driver");
    }

    public static DataSource defaultDataSource() {
        Assert.notBlank(url, "url不能为空");
        Assert.notBlank(driverClassName, "driverClassName不能为空");
        if (defaultDataSource == null) {
            synchronized (MysqlUtil.class) {
                if (defaultDataSource == null) {
                    if(sharding){
                        defaultDataSource = getShardingDataSource();
                    }else{
                        defaultDataSource = getDataSource(url, user, password);
                    }
                }
            }
        }
        return defaultDataSource;
    }

    public static DataSource getShardingDataSource() {
        Assert.notBlank(shardingYamlFilePath, "sharding config file path不能为空");
        File file = new File(shardingYamlFilePath);
        try {
            return YamlShardingDataSourceFactory.createDataSource(file);
        } catch (SQLException e) {
            log.error("创建sharding jdbc datasource error!", e);
        } catch (IOException e) {
            log.error("创建sharding jdbc datasource error!", e);
        }
        throw new DataXException(CommonErrorCode.RUNTIME_ERROR,"创建sharding jdbc datasource error!");
    }

}
