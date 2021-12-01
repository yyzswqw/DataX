## 快速介绍

```sasqlserverreader```是用于将SQL Server中的数据通过jdbc方式导出的插件，可以使用两种方式之一，第一种是使用row_number分页方式通过分页查询数据，第二种是使用时间字段条件过滤查询数据(当过滤后的数据量依然很大时，会采用row_number分页)，所以需要sql server 版本大于等于2005。

## **实现原理**

```sasqlserverreader```是通过row_number分页方式实现分页批量获取数据，或者时间条件过滤数据，拼接SQL获取数据。

## 配置说明

### 使用row_number分页函数方式

以下配置仅row_number分页方式全部配置参数

```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "samysqlreader",
                    "parameter": {
                        "column": [
                            "name","age","id","update_date","date_str"
                        ],
                        "rowNumberOrderBy": "id",
                        "version": "1.0.0",
                        "pageSize": 15,
                        "password": "",
                        "receivePageSize": 10,
                        "sa": {
                            "driverUrl": "jdbc:sqlserver://localhost:1433;DatabaseName=Test",
                            "table": "apple"
                        },
                        "useRowNumber": true,
                        "username": "",
                        "where": "age > 18",
                        "pluginColumn": ["testA","testB"],
                        "sqlColumn": [
                            "select 'a' as testA from tableA where id = '{id}'",
                            "select 1 as testB from tableB where age > {age}"
                        ],
                        "plugin": [
                            {
                                "name": "",
                                "className": "",
                                "param": {
                                    "ip":""
                                }
                            }
                        ]
                    }
                },
                "writer": {
                    "name": "xxx",
                    "parameter": {
                       ...
                    }
                }
            }
        ],
        "setting": {
            "speed": {
                "channel": "1"
            }
        }
    }
}
```

### 使用时间字段条件过滤方式

以下配置仅时间字段条件过滤方式全部配置参数

```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "samysqlreader",
                    "parameter": {
                        "column": [
                            "name","age","id","update_date","date_str"
                        ],
                        "version": "1.0.0",
                        "datePattern": "yyyy-MM-dd",
                        "endTime": "2021-06-24",
                        "password": "",
                        "sa": {
                            "driverUrl": "jdbc:sqlserver://localhost:1433;DatabaseName=Test",
                            "table": "apple"
                        },
                        "startTime": "2021-06-14",
                        "taskNum": 5,
                        "timeFieldName": "update_date",
                        "timeInterval": 1000,
                        “maxQueryNum”: 50000,
                        "useRowNumber": false,
                        "username": "",
                        "pluginColumn": ["testA","testB"],
                        "sqlColumn": [
                            "select 'a' as testA from tableA where id = '{id}'",
                            "select 1 as testB from tableB where age > {age}"
                        ],
                        "plugin": [
                            {
                                "name": "",
                                "className": "",
                                "param": {
                                    "ip":""
                                }
                            }
                        ]
                    }
                },
                "writer": {
                    "name": "xxxx",
                    "parameter": {
                        ...
                    }
                }
            }
        ],
        "setting": {
            "speed": {
                "channel": "1"
            }
        }
    }
}
```

## **参数说明**

### ``read``

​		```version```：需要使用的驱动版本，该属性取值为读插件的driver目录下的某一文件夹名称，在该文件夹下放入配置的SQL Server版本所需要的依赖（不要存在子文件夹）,该值必须配置。例如：在该读插件的driver下有文件夹1.0.0，则该参数值为1.0.0，在1.0.0目录下需要放置依赖驱动的jar包。

​		`column`：表中需要查询的字段名列表。

​		`rowNumberOrderBy`：使用row_number方式是必填，该配置项对应着row_number语法中order by中的字段。

​		`pageSize`：使用row_number分页方式时，分页的大小，默认值10000。

​		`password`：连接数据库的密码。

​		`receivePageSize`：使用row_number分页方式时，每个task负责的页数，默认值5。

​		`sa.driverUrl`：sql server连接的url。

​		`sa.table`：要查询的表，支持子查询表，例如：``( select * from order o left join order_item i on o.id = i.order_id ) t ``。

​		`useRowNumber`：是否使用row_number分页方式，false或为空时，使用时间字段条件过滤方式。

​		`username`：连接数据库的用户名。

​		`where`：使用任意方式时的查询条件。

​		`datePattern`：使用时间字段条件过滤方式时，时间格式。

​		`endTime`：使用时间字段条件过滤方式时，条件结束时间（不包含）。

​		`startTime`：使用时间字段条件过滤方式时，条件开始时间。

​		`taskNum`：使用时间字段条件过滤方式时，task数量，默认值为dataX框架提供的值。

​		`timeFieldName`：使用时间字段条件过滤方式时，使用的时间条件字段名。

​		`timeInterval`：使用时间字段条件过滤方式时，时间段通过```taskNum```分片后，如果数据量还是过大时可指定每次查多久的，默认值为查询一天的量，单位毫秒。

​		```maxQueryNum```：使用时间字段条件过滤方式时，当```timeInterval```时间段内超过该值时，将继续拆分直到拆分的时间段内的数量小于等于该值，时间段采用二分对半拆分，默认值为50000。

​		```timeFieldCount```：使用时间字段条件过滤方式时，是否先统计数量再查询，默认值true，表示优先使用先统计数量再拉取数据，当数据量大时可防止OOM,若为false,则直接拉取数据，不在进行统计，可能导致OOM。

​		```plugin```：神策写插件的插件列表数组，开发规范见**神策写插件插件规范**。

​		```plugin.name```：插件的名称。

​		```plugin.className```：插件的全限定名。

​		```plugin.param```：插件所需要的参数，具体参数根据插件不同而不同。

​		```pluginColumn```：```plugin```中配置的插件或者``sqlColumn``配置项+中如果加入了一些需要发送给下游写插件，则需要配置该参数列表。值为插件中导入Map中的key，注意key中不要带有```.```点号，实际的key将是点号后边的字符。

​		```sqlColumn```：在需要关联其他表时，除了在``table``配置项中关联外，可以在这里配置上关联查询，使用花括号``{已获取到的值的变量}``可以取到主表sql获取到的值，注意：1、``{已获取到的值的变量}``前后不会添加引号，若是字符串则需要手动添加，如果获取不到值则会被替换为字符``null``，另外在这里配置的sql获取到列如果要发送给写插件还需要在``pluginColumn``配置项中配置列名。2、若sql中是一对多关系，则会分裂为多条数据发送到写插件。例如：

```json
"sqlColumn":[
    "select guid as testA from customer_source where name = '{name}' limit 2",       ## sql1
	"select mobile as testB from customer_source where name = '{ name }' limit 2"    ## sql2
]
```

主查询中有3条数据，每条数据都会执行sql1和sql2关联查询出guid和mobile,如果sql1，sql2都查询到了两条，主数据的每条数据在执行完sql1时会分裂为2条，在执行sql2时会在sql1分裂后的结果基础上再分裂即为4条，最终发送到写插件的数据量将会是12条。

## **神策写插件插件规范**

​			引入插件机制目的：业务上的ETL清洗是多样的，在当前插件不支持转换时，可自定义插件进行转换。

- ​	引入common依赖

  ```xml
  <dependency>
      <groupId>com.alibaba</groupId>
      <artifactId>plugin-sa-history-datax-writer-common-plugin</artifactId>
      <version>1.0-SNAPSHOT</version>
  </dependency>
  ```

- 编写代码

  继承com.alibaba.BasePlugin类，重写instance方法（配置文件中plugin.param的配置项会被传递到该方法中），以及定义内部类继承com.alibaba.BasePlugin的内部类BasePlugin.SAPlugin，重写process方法，当前行的数据将以java.util.Map传递。

- 部署插件

  将插件连同依赖一起打包生成jar包，在datax的```samysqlreader```插件下新建plugin文件夹，然后再新建一个放置该插件的文件夹，命名无要求，配置文件中```plugin.name```参数为该文件夹名，最后将生成的jar包放置到该文件夹下。

  ***实现原理***

  神策写插件会实例化该类，并调用instance方法获取到BasePlugin.SAPlugin插件实例，然后调用SAPlugin的process方法（经过转换器转换后的值会被传递到该方法中，空值将会被丢弃）。

## **类型转换**

###  读插件

|                             java                             |    dataX     |    dataX实际类型     |
| :----------------------------------------------------------: | :----------: | :------------------: |
|                             null                             | StringColumn |   java.lang.String   |
|                       java.lang.String                       | StringColumn |   java.lang.String   |
|                  boolean/java.long.Boolean                   |  BoolColumn  |  java.lang.Boolean   |
| byte/java.long.Byte/short/java.long.Short/int/java.long.Integer/long/java.long.Long |  LongColumn  | java.math.BigInteger |
|        float/java.long.Float/double/java.long.Double         | DoubleColumn |   java.lang.String   |
|                        java.util.Date                        |  DateColumn  |    java.util.Date    |
|                     java.time.LocalDate                      |  DateColumn  |    java.util.Date    |
|                   java.time.LocalDateTime                    |  DateColumn  |    java.util.Date    |
|                        java.sql.Date                         |  DateColumn  |    java.util.Date    |
|                      java.sql.Timestamp                      |  DateColumn  |    java.util.Date    |
