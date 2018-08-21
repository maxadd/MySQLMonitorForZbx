# 说明

先说说写这个脚本的初衷，源于某条监控 SQL。事实上，一开始关于 MySQL 的监控是通过 zabbix agent 进行的，自己写 shell、Python 等脚本执行 SQL，然后 zabbix agent 调用这些脚本来获取执行结果，这也是大多数人的做法。

但是有一天我拿到了一条需要监控的 SQL，但是这个 SQL 的执行时间超过了 30s，已经超出 zabbix agent 的最大超时时长，因此这个监控总是会显示执行超时，而无法获取值。没有办法，有问题就要解决。既然无法通过 agent，那就只能自己发送给 zabbix 了。于是我开始了解 zabbix sender，研究该协议，然后就有了该脚本。

该脚本会连接各个 MySQL 服务器（支持多实例），定时执行 SQL 语句，并将执行的结果发送到 zabbix。要完成这些，需要知道 MySQL 服务器的地址、端口、用户名、密码、数据库、zabbix 服务器的地址（如果该 MySQL 由某个 proxy 监控，需要 proxy 地址，而不是 server 的）、需要执行的 SQL、多久执行一次，以及对应的 zabbix key。这些需要在 sql.yml 中配置。

由于该脚本会主动将其采集到的数据发送给 server，无需 zabbix agent，因此需要在 zabbix 上事先创建对应的 key，并且监控项的类型必须是**zabbix 采集器**（trapper item）。

由于该脚本支持多实例，可能存在一台机器跑五六个实例的情况，如果一个个的创建这些 key 会显得很麻烦，因此该脚本本身就是基于低级发现（Low Level Discovery）的，你需要创建一个自动发现的监控项（类型同样是 zabbix 采集器），通过不同端口区别多实例。变量名称为 `{#PORT}`。

## SQL 配置

```yml
10.2.2.2: # mysql ip
  # 监控该 mysql 的 server，如果由 proxy 监控，需要写 proxy 地址
  zbx_addr: 10.3.3.3:10051
  instances:
    # 实例之一
    3306:
      user: hehe
      password: abc@123
      database: 666
      _sql:
        # 这是对应的 zabbix key，这是主从和低级发现的，所以显得复杂，下面会提到
        slave.sql[3306]::slave.running[3306]::behind.master[3306]:
          # 执行的 SQL 语句
          sql: show slave status
          # 多久执行一次，单位是秒
          frequency: 30
          # 表示这是主从监控，另外还有 lld 和 normal 两种
          flag: m/s
          # 主从监控特有，需要关注哪些列
          items: Slave_IO_Running::Slave_SQL_Running::Seconds_Behind_Master
```

这里只列出了主从同步的监控，因为它最复杂。一般而言，一条 SQL 语句只会产出一个值。但是主从同步会输出很多列，根据 MySQL 版本的不同，列数也不同。我们公司关注 `Slave_IO_Running`、`Slave_SQL_Running`、`Seconds_Behind_Master` 这三个指标。这样一来，就需要三个监控项来接收这三个值。所以 key 就不能只是一个，而写成下面这样就太丑了，而且不利于解析：

```yml
slave.sql[3306]:
  sql: show slave status
  frequency: 30
  flag: m/s
  items: Slave_IO_Running
slave.running[3306]:
  sql: show slave status
  frequency: 30
  flag: m/s
  items: Slave_SQL_Running
behind.master[3306]:
  sql: show slave status
  frequency: 30
  flag: m/s
  items: Seconds_Behind_Master
```

因此，干脆就将所有的 key 写成一行，它们之间使用 `::` 分隔。而有些公司关注的列不同，并不只限于我们公司使用的这三列，因此提供 items 字段定义需要监控的列，它们之间同样使用 `::` 分隔。正因为如此，使用 `::` 分割后，key 和 items 必须一一对应。

## 低级发现

某些 SQL 可以返回多个值，比如：

```
a 12
b 232
c 3234
d 35
```

zabbix 监控项只支持一对一，这时要想监控这些值就只能将 SQL 拆成四个了。这是非常蛋疼的做法，还好 zabbix 支持低级发现来做这样的事。

对应 flag 为 `lld`，待完成。

## 正常执行

正常监控就是一对一的：

```yml
_sql:
  key1:
    sql: select xxx
    frequency: 30
    flag: normal
```

## 监控指标

该脚本内置一个 http 服务器，默认监听 6666 端口。访问 `http://127.0.0.1:6666/monitor?uptime` 会得到该程序运行至今的秒数。目前只支持这一种监控指标。

## 参数

使用 `-h` 会打印目前所有支持的参数：

```sh
Usage of /root/mysqlMonitor:
  -alsologtostderr
    	log to standard error as well as files
  -f string
    	config file
  -log_backtrace_at value
    	when Logging hits line file:N, emit a stack trace
  -log_dir string
    	If non-empty, write log files in this directory
  -logtostderr
    	log to standard error instead of files
  -stderrthreshold value
    	logs at or above this threshold go to stderr
  -v value
    	log level for V logs
  -vmodule value
    	comma-separated list of pattern=N settings for file-filtered Logging
```

除了 `-f` 之外，其他都由 glog 模块提供。glog 是谷歌提供的一个日志框架，它有些意思。默认情况下，它会将日志写入到内存，每 30s 刷新一次到磁盘。程序每次执行都会为每个日志级别生成一个日志，总共四个，日志文件为脚本名+主机名+用户名+级别+日期。这样看起来很费劲，因此它提供四个软链接文件，文件名为 `脚本名.级别`，它始终指向当前使用的日志文件。

日志文件格式为：

    [IWEF]mmdd hh:mm:ss.uuuuuu threadid file:line] msg

第一个字符表示级别（共四个），然后就是时间。threadid 表示进程 id，然后就是源文件的文件名和行号，最后就是消息内容了。默认所有日志文件都会输出到 /tmp 目录下，可以通过参数来控制。日志文件还有一个特点就是高级别日志不仅存在于此级别的日志文件中，所有低于它级别的日志文件中都会记录。也就是 error 级别的日志，会出现 error、warning、info 文件中。

它除了支持 info、warning、error、fatal 这四个级别外（不支持 debug）还实现了一种非常特殊的 V 级别，这个 V 级别就是加强版的 debug。debug 只是一个级别，如果我需要 debug 的信息还要分级呢？光一个 debug 级别是不够的。因此 V1 打印一级的 debug 信息，v2 打印二级的，等等。

默认的级别通过 -v 参数指定，数据越大，信息越详细。但是超过 3 没有意义，因为该脚本只支持到 v3。支持运行时修改级别，通过访问 `http://127.0.0.1:6666/admin?verbose=x` 来修改。

参数解释：

- `-alsologtostderr`：是否将日志输出到标准错误输出，默认 false；
- `log_dir`：指定日志目录，默认在 `/tmp` 目录下；
- `-logtostderr`：将日志输出到标准错误输出而非文件，默认 false；
- `-stderrthreshold`：什么级别的日志会输出到标准错误输出，默认为 error，也就是所有 error 级别的日志都会输出到标准错误输出；
- `-v`：指定 debug 级别，默认为 0，不会输出。

需要添加的参数有：http 端口。

