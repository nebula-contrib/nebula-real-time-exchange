Nebula Real-Time Exchange致力于提供从关系型数据库到Nebula Graph的异构数据源实时同步解决方案。项目基于Flink及其配套CDC组件完成不同数据源的插入问题。通过FlinkCDC提供的丰富上游connector，可以构建不同来源数据库的数据流入；在下游接入Nebula开源的nebula-flink-cdc组件，可以良好实现上下游数据的实时同步功能。

# 项目背景

如下图所示，目前Nebula Graph已支持以下工具实现数据迁移：

![Nebula Graph 鸟瞰图](https://s2.loli.net/2022/06/24/UmaxIDyRXSPJ1d9.png)

1. Nebula Importer（简称 Importer）是一款 Nebula Graph 的 CSV 文件单机导入工具。可以读取本地的 CSV 文件，然后导入数据至 Nebula Graph 图数据库中。
2. Nebula Exchange（简称 Exchange）是一款 Apache Spark™ 应用，用于在分布式环境中将集群中的数据批量迁移到 Nebula Graph 中，能支持多种不同格式的批式数据和流式数据的迁移。其由 Reader、Processor 和 Writer 三部分组成。Reader 读取不同来源的数据返回 DataFrame 后，Processor 遍历 DataFrame 的每一行，根据配置文件中`fields`的映射关系，按列名获取对应的值。在遍历指定批处理的行数后，Writer 会将获取的数据一次性写入到 Nebula Graph 中。
3. Nebula Spark Connector 是一个 Spark 连接器，提供通过 Spark 标准形式读写 Nebula Graph 数据的能力。Nebula Spark Connector 由 Reader 和 Writer 两部分组成。Reader提供一个 Spark SQL 接口，用户可以使用该接口编程读取 Nebula Graph 图数据，单次读取一个点或 Edge type 的数据，并将读取的结果组装成 Spark 的 DataFrame。Writer提供一个 Spark SQL 接口，用户可以使用该接口编程将 DataFrame 格式的数据逐条或批量写入 Nebula Graph。
4. Nebula Flink Connector 是一款帮助 Flink 用户快速访问 Nebula Graph 的连接器，支持从 Nebula Graph 图数据库中读取数据，或者将其他外部数据源读取的数据写入 Nebula Graph 图数据库。适用于在不同的 Nebula Graph 集群之间迁移数据、在同一个 Nebula Graph 集群内不同图空间之间迁移数据或Nebula Graph 与其他数据源之间迁移数据的场景中。

由上可得，Nebula Graph目前并未支持从MySQL数据库到Nebula Graph数据库的异构数据库实时同步。本项目拟利用 Canal / FlinkCDC / Debezium 等数据同步工具实现MySQL 到 Nebula Graph 的数据实时同步功能，需要实现ETL工具的功能，将数据从CDC工具经过抽取（extract）、转换（transform）、加载（load）进入目的端Nebula Graph中。

# 需求分析

MySQL由于自身简单、高效、可靠的特点，成为当下使用最广泛的数据库，但是当数据量达到千万/亿级别的时候，MySQL的相关操作会变的非常迟缓；且当下使用图的方式对场景建模，会有利于描述复杂关系。而将目前使用的MySQL修改为nebula图数据库的代价十分巨大，连接前端业务数据系统（MySQL）和后端nebula数据库实现实时同步不失为一种方案，我们需要一套实时同步MySQL数据到Nebula Graph的解决方案。

# 技术选型

对比常见的开源 CDC 方案，我们可以发现：

![img](https://s2.loli.net/2022/06/24/1xtkFULi4SOBd9p.png)



- 对比全量同步能力，基于查询或者日志的 CDC 方案基本都支持，除了 Canal。
- 而对比全量 + 增量同步的能力，只有 Flink CDC、Debezium、Oracle Goldengate 支持较好。

- 从架构角度去看，该表将架构分为单机和分布式，这里的分布式架构不单纯体现在数据读取能力的水平扩展上，更重要的是在大数据场景下分布式系统接入能力，当下游是Nebula Graph的分布式集群时，Flink CDC 的架构能够很好地接入此类系统。

- 在 Flink CDC 上数据转换 / 数据清洗操作简单，可以通过 Flink SQL 去操作这些数据；

- 在生态方面，这里指的是下游的一些数据库或者数据源的支持。Flink CDC 上游有丰富的 Connector，不仅支持从MySQL作为数据源，还支持 TiDB、PostgreSQL、SQLServer等常见系统。同时目前已经有Nebula Flink Connector 完成从Flink的数据接入，具有良好的可扩展性。

# 概要设计

![Untitled Diagram.drawio-2](https://github.com/ToJ112/nebula-real-time-exchange/blob/main/doc/design.png?raw=true)

FlinkCDC通过解析MySQL数据库的Binlog日志捕获变更数据，并通过Reader部分将binlog日志解析为需要处理的DataStream。而后，Processor将根据配置信息将Source数据分库分表进行处理，分流成为Tag和Edge数据，并根据解析数据的不同操作将DataStream分流。最后，Writer部分根据分流结果将流数据通过Nebula Sink对Nebula Graph数据库中的Tag和Edge同步操作。

# 详细设计

## Reader

通过读取Yaml文件中的配置将需要同步的MySQL Table的binlog解析为Flink中的DataStream：

```java
private static HashMap<String, DataStreamSource<String>> mysqlReader(Mysql2NebulaConfig config, StreamExecutionEnvironment env) {
        HashMap<String, Set<String>> sqlDbMap = new HashMap<>();
        HashMap<String, Set<String>> sqlTableMap = new HashMap<>();
        for (SinkTag sinkTag : config.nebulaSink.tagList) {
            sqlDbMap.computeIfAbsent(sinkTag.sourceSql, k -> new HashSet<>())
                    .add(sinkTag.sourceDatabase);
            sqlTableMap.computeIfAbsent(sinkTag.sourceSql, k -> new HashSet<>())
                    .add(sinkTag.sourceDatabase + '.' + sinkTag.sourceTable);

        }
        for (SinkEdge sinkEdge : config.nebulaSink.edgeList) {
            sqlDbMap.computeIfAbsent(sinkEdge.sourceSql, k -> new HashSet<>())
                    .add(sinkEdge.sourceDatabase);
            sqlTableMap.computeIfAbsent(sinkEdge.sourceSql, k -> new HashSet<>())
                    .add(sinkEdge.sourceDatabase + '.' + sinkEdge.sourceTable);
        }
        HashMap<String, DataStreamSource<String>> mysqlSourceMap = new HashMap<>();
        for (MysqlSourceIn mysqlSourceIn : config.mysqlSourceInList) {
            Set<String> dbSet = sqlDbMap.get(mysqlSourceIn.sqlName);
            Set<String> tableSet = sqlTableMap.get(mysqlSourceIn.sqlName);
            MySqlSource<String> sourceFunction = MySqlSource.<String>builder()
                    .hostname(mysqlSourceIn.address)
                    .port(mysqlSourceIn.port)
                    .username(mysqlSourceIn.username)
                    .password(mysqlSourceIn.password)
                    .databaseList(dbSet.toArray(new String[0]))
                    .tableList(tableSet.toArray(new String[0]))
                    .startupOptions(StartupOptions.initial())
                    .deserializer(new CommonStringDebeziumDeserializationSchema())
                    .build();
            DataStreamSource<String> streamSource = env.fromSource(sourceFunction,
                    WatermarkStrategy.noWatermarks(),
                    "MySQL Source"
            );
            mysqlSourceMap.put(mysqlSourceIn.sqlName, streamSource);
        }
        return mysqlSourceMap;
    }
```

在使用默认反序列化器时，从MySQL读取的binlog数据将会解析为如下的SourceRecord

```json
SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={ts_sec=1648043118, file=mysql-bin.000008, pos=16610, gtids=f7e8908c-e34a-11ea-b5e3-00163e128b67:1-120, row=1, server_id=1, event=2}} ConnectRecord{topic='mysql_binlog_source.bank1.record', kafkaPartition=null, key=null, keySchema=null, value=Struct{after=Struct{id=008,name=mike,version=36},source=Struct{version=1.4.1.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1648043118000,db=bank1,table=record,server_id=1,gtid=f7e8908c-e34a-11ea-b5e3-00163e128b67:121,file=mysql-bin.000008,pos=16739,row=0,thread=152},op=c,ts_ms=1648043119363}, valueSchema=Schema{mysql_binlog_source.bank1.record.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
```

## Processor

### 自定义反序列化器

Flink CDC 提供了一种叫做自定义序列化的方式，通过实现DebeziumDeserializationSchema接口，然后重写里面的方法，在重写的方法里面完成日志的解析工作。需要在反序列化器中对读取的SourceRecord进行处理，提取分流所需信息并将MySQL中部分无法解析到Nebula Graph中的数据进行处理：

```java

/**
 * deserialize debezium format binlog
 */
public class CommonStringDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
    private final SimpleDateFormat sdf = new SimpleDateFormat("hh:mm:ss");
    private static final Logger LOG = LoggerFactory.getLogger(CommonStringDebeziumDeserializationSchema.class);
    private final ArrayList<String> geoFormat = new ArrayList<>(Arrays.asList("Point", "LineString", "Polygon"));

    public void deserialize(SourceRecord record, Collector<String> out) {
        System.out.println(record);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("ts_sec", (Long) record.sourceOffset().get("ts_sec"));
        String[] name = record.valueSchema().name().split("\\.");
        jsonObject.put("db", name[1]);
        jsonObject.put("table", name[2]);
        Struct value = (Struct) record.value();
        String operatorType = value.getString("op");
        jsonObject.put("op", operatorType);

        String valueFrom = "d".equals(operatorType) ? "before" : "after";
        JSONObject dataJsonObject = parseRecord(value.getStruct(valueFrom));
        jsonObject.put("data", dataJsonObject);

        Struct key = (Struct) record.key();
        JSONArray keyArray = new JSONArray();
        for (Field field : key.schema().fields()) {
            keyArray.add(field.name());
        }

        //jsonObject.put("key", keyArray);需设置sink所需的Index为主键才能正常update index


        jsonObject.put("parse_time", System.currentTimeMillis() / 1000);
        out.collect(jsonObject.toString());
    }

    protected JSONObject parseRecord(Struct after) {
        JSONObject jo = new JSONObject();

        for (Field field : after.schema().fields()) {
            if (after.get(field.name()) != null) {
                if (field.schema().type().isPrimitive()) {
                    switch (field.schema().type()) {
                        case INT8:
                            Byte resultInt8 = after.getInt8(field.name());
                            jo.put(field.name(), resultInt8);
                            break;
                        case INT16:
                            Short resultInt16 = after.getInt16(field.name());
                            jo.put(field.name(), resultInt16);
                            break;
                        case INT32:
                            Integer resultInt32 = after.getInt32(field.name());
                            jo.put(field.name(), resultInt32);
                            break;
                        case INT64:
                            Long resultInt = after.getInt64(field.name());
                            jo.put(field.name(), resultInt);
                            break;
                        case FLOAT32:
                            Float resultFloat32 = after.getFloat32(field.name());
                            jo.put(field.name(), resultFloat32);
                            break;
                        case FLOAT64:
                            Double resultFloat64 = after.getFloat64(field.name());
                            jo.put(field.name(), resultFloat64);
                            break;
                        case BYTES:
                            // json ignore byte column
                            byte[] resultByte = after.getBytes(field.name());
                            jo.put(field.name(), Arrays.toString(resultByte));
                            break;
                        case STRING:
                            String resultStr = after.getString(field.name());
                            jo.put(field.name(), resultStr);
                            break;
                        default:
                    }
                } else {

                    switch (field.schema().name()) {
                        case Date.SCHEMA_NAME:
                            String resultDateStr = TemporalConversions.toLocalDate(after.get(field.name())).toString();
                            jo.put(field.name(), resultDateStr);
                            break;
                        case Timestamp.SCHEMA_NAME:
                            TimestampData resultTime = TimestampData.fromEpochMillis((Long) after.get(field.name()));
                            jo.put(field.name(), String.valueOf(resultTime));
                            break;
                        case MicroTimestamp.SCHEMA_NAME:
                            long micro = (Long) after.get(field.name());
                            TimestampData resultMicroTimestamp = TimestampData.fromEpochMillis(micro / 1000L,
                                    (int) (micro % 1000L * 1000L));
                            jo.put(field.name(), String.valueOf(resultMicroTimestamp));
                            break;
                        case NanoTimestamp.SCHEMA_NAME:
                            long nano = (Long) after.get(field.name());
                            TimestampData resultNanoTimestamp = TimestampData.fromEpochMillis(nano / 1000000L,
                                    (int) (nano % 1000000L));
                            jo.put(field.name(), String.valueOf(resultNanoTimestamp));
                            break;
                        case ZonedTimestamp.SCHEMA_NAME:
                            if (after.get(field.name()) instanceof String) {
                                String str = (String) after.get(field.name());
                                Instant instant = Instant.parse(str);

                                ZoneId serverTimeZone = ZoneId.systemDefault();
                                TimestampData resultZonedTimestamp = TimestampData.fromLocalDateTime(
                                        LocalDateTime.ofInstant(instant, serverTimeZone));
                                jo.put(field.name(), String.valueOf(resultZonedTimestamp));
                            } else {
                                throw new IllegalArgumentException("Unable to convert to TimestampData from unexpected value ");
                            }
                            break;
                        case MicroTime.SCHEMA_NAME:
                            int resultMicroTime;
                            if (after.get(field.name()) instanceof Long) {
                                resultMicroTime = (int) ((Long) after.get(field.name()) / 1000L);
                            } else if (after.get(field.name()) instanceof Integer) {
                                resultMicroTime = (int) after.get(field.name());
                            } else {
                                resultMicroTime = TemporalConversions.toLocalTime(after.get(field.name()))
                                        .toSecondOfDay() * 1000;
                            }
                            String resultMicroTimeStr = sdf.format(resultMicroTime);
                            jo.put(field.name(), resultMicroTimeStr);
                            break;
                        case NanoTime.SCHEMA_NAME:
                            int resultNanoTime;
                            if (after.get(field.name()) instanceof Long) {
                                resultNanoTime = (int) ((Long) after.get(field.name()) / 1000000L);
                            } else if (after.get(field.name()) instanceof Integer) {
                                resultNanoTime = (int) after.get(field.name());
                            } else {
                                resultNanoTime = TemporalConversions.toLocalTime(after.get(field.name()))
                                        .toSecondOfDay() * 1000;
                            }
                            String resultNanoTimeStr = sdf.format(resultNanoTime);
                            jo.put(field.name(), resultNanoTimeStr);
                            break;
                        case Geometry.LOGICAL_NAME:
                            Struct geoStruct = after.getStruct(field.name());
                            WKBReader wkbReader = new WKBReader();
                            byte[] wkbs = geoStruct.getBytes("wkb");
                            try {
                                org.locationtech.jts.geom.Geometry geoWkb = wkbReader.read(wkbs);
                                String geoWkbStr;
                                if (!geoFormat.contains(geoWkb.getGeometryType())) {
                                    geoWkbStr = null;
                                    LOG.warn("Cannot Sink " + geoWkb.toText() + " to Nebula Graph, because" + geoWkb.getGeometryType());
                                } else {
                                    geoWkbStr = geoWkb.toText();
                                }
                                jo.put(field.name(), geoWkbStr);
                            } catch (ParseException e) {
                                throw new RuntimeException(e);
                            }
                    }
                }
            } else {
                jo.put(field.name(), null);
            }
        }

        return jo;
    }

    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
```

## FlatMap分流

由于NebulaSink对Tag/Edge及Insert/Update/Delete等不同操作所需的配置不同，需要根据DataStream中的操作信息及配置中不同table对应的Tag/Edge对之前处理过的数据进行进一步分流及处理：

```java
 public void flatMap(String s, Collector<Row> collector) {
        JSONObject jsonObject = JSON.parseObject(s, Feature.OrderedField);
        String op = jsonObject.getString("op");
        String table = jsonObject.getString("table");
        String db = jsonObject.getString("db");
        if (operator.contains(op) && db.equals(sourceDatabase) && table.equals(sourceTable)) {
            JSONArray keyArray = jsonObject.getJSONArray("key");
            for (String keyCol : indexColumn) {
                if (!keyArray.contains(keyCol)) {
                    LOG.warn("The Nebula Graph Index " + keyCol + "  might not be set as MySQL key");
                }
            }

            LinkedHashMap dataMap = JSON.parseObject(jsonObject.getString("data"), LinkedHashMap.class,
                    Feature.OrderedField);
            Row row = new Row(columnList.size());
            for (int i = 0; i < columnList.size(); i++) {
                String columnName = columnList.get(i);
                if (dataMap.get(columnName) != null) {
                    row.setField(i, dataMap.get(columnName));
                } else {
                    row.setField(i, null);
                }
            }
            collector.collect(row);
        }

    }
```

在这一部分同时我们完成了NebulaSink所需主键配置的认证，并将所得结果写入Row类型的数据中

## Writer

根据配置信息中不同tag和edge配置不同的sink入口及操作类型，将数据写入Nebula Graph中：

```java
private static void nebulaWriter(Mysql2NebulaConfig config, HashMap<String, DataStreamSource<String>> mysqlSourceMap) {
        for (SinkTag sinkTag : config.nebulaSink.tagList) {
            ArrayList<String> fields = sinkTag.getFields();
            ArrayList<Integer> positions = sinkTag.getPositions();
            ArrayList<String> sqlColumn = sinkTag.getSqlColumns();

            for (WriteModeEnum operator : WriteModeEnum.values()) {
                DataStream<Row> rowDataStream = mysqlSourceMap.get(sinkTag.sourceSql)
                        .flatMap(new OperatorFlatMap(operator, sinkTag, sqlColumn));
//                rowDataStream.print();
                rowDataStream.addSink(getNebulaVertexOption(sinkTag, fields, positions, operator));
            }
        }
        for (SinkEdge sinkEdge : config.nebulaSink.edgeList) {
            ArrayList<String> fields = sinkEdge.getFields();
            ArrayList<Integer> positions = sinkEdge.getPositions();
            ArrayList<String> sqlColumn = sinkEdge.getSqlColumns();

            for (WriteModeEnum operator : WriteModeEnum.values()) {
                DataStream<Row> rowDataStream = mysqlSourceMap.get(sinkEdge.sourceSql)
                        .flatMap(new OperatorFlatMap(operator, sinkEdge, sqlColumn));
//                rowDataStream.print();
                rowDataStream.addSink(getNebulaEdgeOption(sinkEdge, fields, positions, operator));
            }
        }
    }

    private static SinkFunction<Row> getNebulaVertexOption(SinkTag sinkTag, ArrayList<String> fields, ArrayList<Integer> positions, WriteModeEnum op) {
        NebulaClientOptions nebulaClientOptions = new NebulaClientOptions.NebulaClientOptionsBuilder()
                .setGraphAddress(sinkTag.graphAddress)
                .setMetaAddress(sinkTag.metaAddress)
                .build();
        NebulaGraphConnectionProvider graphConnectionProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);
        NebulaMetaConnectionProvider metaConnectionProvider =
                new NebulaMetaConnectionProvider(nebulaClientOptions);

        VertexExecutionOptions.ExecutionOptionBuilder executionOptionBuilder = new VertexExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace(sinkTag.graphSpace)
                .setTag(sinkTag.sinkName)
                .setIdIndex(sinkTag.idIndex.position)
                .setFields(fields)
                .setPositions(positions)
                .setWriteMode(op)
                .setBatch(5)
                .setBathIntervalMs(500L);
        NebulaBatchOutputFormat outPutFormat = new NebulaBatchOutputFormat(graphConnectionProvider, metaConnectionProvider)
                .setExecutionOptions(executionOptionBuilder.builder());
        NebulaSinkFunction nebulaSinkFunction = new NebulaSinkFunction(outPutFormat);
        return nebulaSinkFunction;
    }

    private static SinkFunction<Row> getNebulaEdgeOption(SinkEdge sinkEdge, ArrayList<String> fields, ArrayList<Integer> positions, WriteModeEnum operator) {
        NebulaClientOptions nebulaClientOptions = new NebulaClientOptions.NebulaClientOptionsBuilder()
                .setGraphAddress(sinkEdge.graphAddress)
                .setMetaAddress(sinkEdge.metaAddress)
                .build();
        NebulaGraphConnectionProvider graphConnectionProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);
        NebulaMetaConnectionProvider metaConnectionProvider =
                new NebulaMetaConnectionProvider(nebulaClientOptions);

        EdgeExecutionOptions.ExecutionOptionBuilder executionOptionBuilder = new EdgeExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace(sinkEdge.graphSpace)
                .setEdge(sinkEdge.sinkName)
                .setWriteMode(operator)
                .setSrcIndex(sinkEdge.srcIndex.position)
                .setDstIndex(sinkEdge.dstIndex.position)
                .setFields(fields)
                .setPositions(positions)
                .setBatch(5)
                .setBathIntervalMs(500L);
        if (sinkEdge.isRankIndexPresent()) {
            executionOptionBuilder.setRankIndex(sinkEdge.rankIndex.position);

        }
        NebulaBatchOutputFormat outPutFormat = new NebulaBatchOutputFormat(graphConnectionProvider, metaConnectionProvider)
                .setExecutionOptions(executionOptionBuilder.builder());
        NebulaSinkFunction nebulaSinkFunction = new NebulaSinkFunction(outPutFormat);
        return nebulaSinkFunction;

    }
```

