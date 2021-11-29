package com.alibaba.datax.plugin.util;


import cn.hutool.core.lang.Assert;
import com.alibaba.druid.pool.DruidDataSource;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;

@Slf4j
public class ConnUtil {

    private static String url;

    private static String user;

    private static String password;

    private static String driverClassName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    private static DataSource defaultDataSource;

    public static void setUrl(String url) {
        ConnUtil.url = url;
    }

    public static void setUser(String user) {
        ConnUtil.user = user;
    }

    public static void setPassword(String password) {
        ConnUtil.password = password;
    }

    public static void setDriverClassName(String driverClassName) {
        ConnUtil.driverClassName = driverClassName;
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
        return ConnUtil.getDataSource(url, user, password, "com.microsoft.sqlserver.jdbc.SQLServerDriver");
    }

    public static DataSource defaultDataSource() {
        Assert.notBlank(url, "url不能为空");
        Assert.notBlank(driverClassName, "driverClassName不能为空");
        if (defaultDataSource == null) {
            synchronized (ConnUtil.class) {
                if (defaultDataSource == null) {
                    defaultDataSource = getDataSource(url, user, password);
                }
            }
        }
        return defaultDataSource;
    }

}
