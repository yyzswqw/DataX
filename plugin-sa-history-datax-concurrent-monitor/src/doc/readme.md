## 快速介绍

```plugin-sa-history-datax-concurrent-monitor```是用来解决使用``DataX`` + ``crontab`` 做T+N时，大量的任务在相同时间执行，避免占用太多的系统资源。

## **实现原理**

```plugin-sa-history-datax-concurrent-monitor```是通过引入运行shell命令，配合线程池进行相同时间的任务数限制。

## **使用说明**

使用maven package 命令打包后，执行``java -jar ./plugin-sa-history-datax-concurrent-monitor-0.0.1-SNAPSHOT.jar -h``可查看帮助文档。

```shell
usage: java -jar <*.jar> [-c <arg>] [-cf <arg>] [-f <arg>] [-h] [-m <arg>] [-p <arg>] [-r <arg>]
 -c,--channel <arg>   并发数，同时运行的任务数，默认值为：如任务数大于执行物理机cpu核数,则为cpu核数，否则为任务数(-m指定的数量加上-f中指定的数量)
 -cf,--crfl <arg>     定时任务型命令集文件，文件中第一行为cron表达式，之后每一行一个命令，即一个任务
 -f,--file <arg>      命令集文件，文件中一行为一个任务
 -h,--help            显示帮助文档
 -m,--cmd <arg>       执行的命令集
 -p,--pool <arg>      最大队列数,默认值500
 -r,--cron <arg>      定时任务cron表达式，添加该参数将以定时任务运行
```

``-m`` : 为单条执行命令，可与 ``-f`` 、``-cf``同时使用，可指定多个。

``-f`` : 为执行命令集合的文件，一行即为一条执行命令，也即一个任务，可与 ``-m``、``-cf``同时使用，可指定多个。如：文件内容如下：

```txt
ping www.baidu.com
echo 12
sh s.sh
python datax.py tt.json >> 2.txt 2>&1 </dev/null
```

``-cf`` : 为执行命令集合的文件，一行即为一条执行命令，也即一个任务，可与 ``-m``、``-f``同时使用，可指定多个。如：文件内容如下：

```txt
*/2 * * * * ?
ping www.baidu.com
echo 12
sh s.sh
python datax.py tt.json >> 2.txt 2>&1 </dev/null
```

**注意**：```plugin-sa-history-datax-concurrent-monitor```在不指定``-r``或者``-cf``时只会执行一次，不是定时任务，若需要做T+N,则还需要配合 ``crontab`` 使用。

