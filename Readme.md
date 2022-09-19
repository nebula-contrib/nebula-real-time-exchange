Nebula Real-Time Exchange是一款 Apache Flink™ 应用，用于在分布式环境中将MySQL集群中的数据实时同步到 Nebula Graph 中。项目基于flink1.13.6及 [flink-cdc-connectors](https://github.com/ververica/flink-cdc-connectors) 2.2.0，通过Yaml配置实现实时Stream ETL。本文以一个示例说明如何使用Real-Time Exchange 实现MySQL 与 Nebula Graph数据库实时同步。

## 数据集

本文以 [basketballplayer 数据集](https://docs-cdn.nebula-graph.com.cn/dataset/dataset.zip)为例。

在本示例中，该数据集已经存入 MySQL 中名为`basketball`的数据库中，以`player`、`team`、`follow`和`serve`四个表存储了所有点和边的信息。以下为各个表的结构。

```mysql
mysql> desc player;
+----------+-------------+------+-----+---------+-------+
| Field    | Type        | Null | Key | Default | Extra |
+----------+-------------+------+-----+---------+-------+
| playerid | varchar(30) | YES  | YES | NULL    |       |
| age      | int         | YES  |     | NULL    |       |
| name     | varchar(30) | YES  |     | NULL    |       |
+----------+-------------+------+-----+---------+-------+

mysql> desc team;
+--------+-------------+------+-----+---------+-------+
| Field  | Type        | Null | Key | Default | Extra |
+--------+-------------+------+-----+---------+-------+
| teamid | varchar(30) | YES  | YES | NULL    |       |
| name   | varchar(30) | YES  |     | NULL    |       |
+--------+-------------+------+-----+---------+-------+

mysql> desc follow;
+------------+-------------+------+-----+---------+-------+
| Field      | Type        | Null | Key | Default | Extra |
+------------+-------------+------+-----+---------+-------+
| src_player | varchar(30) | YES  | YES | NULL    |       |
| dst_player | varchar(30) | YES  | YES | NULL    |       |
| degree     | int         | YES  |     | NULL    |       |
+------------+-------------+------+-----+---------+-------+

mysql> desc serve;
+------------+-------------+------+-----+---------+-------+
| Field      | Type        | Null | Key | Default | Extra |
+------------+-------------+------+-----+---------+-------+
| playerid   | varchar(30) | YES  | YES | NULL    |       |
| teamid     | varchar(30) | YES  | YES | NULL    |       |
| start_year | int         | YES  |     | NULL    |       |
| end_year   | int         | YES  |     | NULL    |       |
+------------+-------------+------+-----+---------+-------+
```

# 配置准备

开始同步数据之前，用户需要确认以下信息：

- 已经[安装部署 Nebula Graph](https://docs.nebula-graph.com.cn/3.0.0/4.deployment-and-installation/2.compile-and-install-nebula-graph/2.install-nebula-graph-by-rpm-or-deb/) 并获取如下信息：

  - Graph 服务和 Meta 服务的的 IP 地址和端口。

  - 拥有 Nebula Graph 写权限的用户名和密码。

- 已经编译 Real-Time Exchange。

- 已经安装 Flink 并启动。

- 了解 Nebula Graph 中创建 Schema 的信息，包括 Tag 和 Edge type 的名称、属性等。

## 操作步骤

### 步骤1：开启MySQL的binlog服务

（要求 MySQL 版本 5.7+）

在 MySQL 配置文件（my.cnf）中开启 binlog 配置，其中 binlog_format 需要是 row 。

```bash
# 5.7及以上版本
server-id=123454
# 日志文件地址
log-bin=/var/lib/mysql/mysql-bin
# 日志格式
binlog_format=row
```

通过 binlog_do_db 配置需要执行同步的数据库：

```bash
binlog_do_db=数据库1
binlog_do_db=数据库2
...
```
然后重启 MySQL

```systemctl restart mysqld```

### 步骤 2：在 Nebula Graph 中创建 Schema

分析数据，按以下步骤在 Nebula Graph 中创建 Schema：

1. 确认 Schema 要素。Nebula Graph 中的 Schema 要素如下表所示。

   | 要素      | 名称     | 属性                           |
   | --------- | -------- | ------------------------------ |
   | Tag       | `player` | `name string, age int`         |
   | Tag       | `team`   | `name string`                  |
   | Edge Type | `follow` | `degree int`                   |
   | Edge Type | `serve`  | `start_year int, end_year int` |

2. 在 Nebula Graph 中创建一个图空间 **basketballplayer**，并创建一个 Schema，如下所示。

```bash
## 创建图空间
nebula> CREATE SPACE basketballplayer \
   (partition_num = 10, \
   replica_factor = 1, \
   vid_type = FIXED_STRING(30));

## 选择图空间 basketballplayer
nebula> USE basketballplayer;

## 创建 Tag player
nebula> CREATE TAG player(name string, age int);

## 创建 Tag team
nebula> CREATE TAG team(name string);

## 创建 Edge type follow
nebula> CREATE EDGE follow(degree int);

## 创建 Edge type serve
nebula> CREATE EDGE serve(start_year int, end_year int);
```

更多信息，请参见[快速开始](https://docs.nebula-graph.com.cn/3.0.0/2.quick-start/1.quick-start-workflow/)

### 步骤 3：修改配置文件

编译 Exchange 后，按照配置示例配置同步任务，本示例的配置名为`mysql_application.yaml`

```yaml
mysqlSourceInList:
# 需同步到Nebula Graph的MySQL数据库，可指定多个数据库，并与下方nebulaSink中数据信息对应
  - sqlName: mysql_01
    address: 127.0.0.1
    port: 3306
    username: root
    password: 162331
nebulaSink:
  # 处理点数据
  tagList:
    - sinkName: player
      graphSpace: basketballplayer # tag所在的图空间
      graphAddress: "127.0.0.1:9669" # Nebula Graph的graph地址，分布式集群可配置多个
      metaAddress: "127.0.0.1:9559" # Nebula Graph的meta地址
      sourceSql: mysql_01	# 对应MySQL地址
      sourceDatabase: test	# 对应数据库的Schema
      sourceTable: player	# 对应数据库的Table
      idIndex:
        sqlCol: "playerid"	# MySQL中的对应 column 名，需保证该column为 Table 主键，且数据类型与所创建的Tag的Vid适配
        position: 0	# 同步信息的index
      fieldList:
        - name: "name"	# NebulaGraph中对应的Tag属性
          sqlCol: "name"	# MySQL中的对应 column 名
          position: 1 
        - name: "age"
          sqlCol: "age"
          position: 2
    - sinkName: team
      graphSpace: basketballplayer
      graphAddress: "127.0.0.1:9669"
      metaAddress: "127.0.0.1:9559"
      sourceSql: mysql_01
      sourceDatabase: test
      sourceTable: team
      idIndex:
        sqlCol: "teamid"
        position: 0
      fieldList:
        - name: "name"
          sqlCol: "name"
          position: 1
  # 处理边数据
  edgeList:
    - sinkName: follow
      graphSpace: basketballplayer
      graphAddress: "127.0.0.1:9669"
      metaAddress: "127.0.0.1:9559"
      sourceSql: mysql_01
      sourceDatabase: test
      sourceTable: follow
      srcIndex:
        sqlCol: "src_player"
        position: 0
      dstIndex:
        sqlCol: "dst_player"
        position: 1
      fieldList:
        - name: "degree"
          sqlCol: "degree"
          position: 2
    - sinkName: serve
      graphSpace: basketballplayer
      graphAddress: "127.0.0.1:9669"
      metaAddress: "127.0.0.1:9559"
      sourceSql: mysql_01
      sourceDatabase: test
      sourceTable: serve
      srcIndex:
        sqlCol: "playerid"
        position: 0
      dstIndex:
        sqlCol: "teamid"
        position: 1
      rankIndex: #可选
        sqlCol: "ranks"
        position: 4
      fieldList:
        - name: "start_year"
          sqlCol: "start_year"
          position: 2
        - name: "end_year"
          sqlCol: "end_year"
          position: 3

```

### 步骤 3：开启向 Nebula Graph 同步数据任务

```bash
$ <nebula-real-time-exchange.jar_path>  <mysql_application.yaml_path>
```

### 步骤 4：（可选）验证数据

用户在MySQL完成增删改后，可以在 Nebula Graph 客户端（例如 Nebula Graph Studio）中执行查询语句，确认数据是否已完成同步。例如：

```
GO FROM "player100" OVER follow;
```

用户也可以使用命令 [`SHOW STATS`](https://docs.nebula-graph.com.cn/3.0.0/3.ngql-guide/7.general-query-statements/6.show/14.show-stats/) 查看统计数据。
