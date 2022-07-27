import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.nebula.connection.NebulaClientOptions;
import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.sink.NebulaBatchOutputFormat;
import org.apache.flink.connector.nebula.sink.NebulaSinkFunction;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.constructor.Constructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import yaml.*;

import java.io.InputStream;
import java.util.*;

public class Runner {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Yaml configYaml = new Yaml(new Constructor(Mysql2NebulaConfig.class));
        InputStream configInput = Runner.class.getClassLoader().getResourceAsStream(args[0]);
        Mysql2NebulaConfig config = configYaml.loadAs(configInput, Mysql2NebulaConfig.class);
        HashMap<String, Set<String>> sqlDbMap = new HashMap<>();
        HashMap<String, Set<String>> sqlTableMap = new HashMap<>();
        for (SinkTag sinkTag : config.nebulaSink.tagList) {
            sqlDbMap.computeIfAbsent(sinkTag.sourceSql, k -> new HashSet<String>())
                    .add(sinkTag.sourceDatabase);
            sqlTableMap.computeIfAbsent(sinkTag.sourceSql, k -> new HashSet<String>())
                    .add(sinkTag.sourceDatabase + '.' + sinkTag.sourceTable);

        }
        for (SinkEdge sinkEdge : config.nebulaSink.edgeList) {
            sqlDbMap.computeIfAbsent(sinkEdge.sourceSql, k -> new HashSet<String>())
                    .add(sinkEdge.sourceDatabase);
            sqlTableMap.computeIfAbsent(sinkEdge.sourceSql, k -> new HashSet<String>())
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
                    .deserializer(new CommonStringDebeziumDeserializationSchema())
                    .build();
            DataStreamSource<String> streamSource = env.fromSource(sourceFunction,
                    WatermarkStrategy.noWatermarks(),
                    "MySQL Source"
            );
            streamSource.print();
            mysqlSourceMap.put(mysqlSourceIn.sqlName, streamSource);
        }
        for (SinkTag sinkTag : config.nebulaSink.tagList) {
            ArrayList<String> fields = new ArrayList<>();
            ArrayList<Integer> positions = new ArrayList<>();

            for (fieldMap fieldMap : sinkTag.fieldList) {
                fields.add(fieldMap.name);
                positions.add(fieldMap.position);
            }
            for (WriteModeEnum operator : WriteModeEnum.values()) {
                DataStream<Row> rowDataStream = mysqlSourceMap.get(sinkTag.sourceSql)
                        .flatMap(new OperatorFlatMap(operator, sinkTag.sourceDatabase, sinkTag.sourceTable));
                rowDataStream.addSink(getNebulaVertexOption(sinkTag, fields, positions, operator));
            }
        }
        for (SinkEdge sinkEdge : config.nebulaSink.edgeList) {
            ArrayList<String> fields = new ArrayList<>();
            ArrayList<Integer> positions = new ArrayList<>();

            for (fieldMap fieldMap : sinkEdge.fieldList) {
                fields.add(fieldMap.name);
                positions.add(fieldMap.position);
            }
            for (WriteModeEnum operator : WriteModeEnum.values()) {
                DataStream<Row> rowDataStream = mysqlSourceMap.get(sinkEdge.sourceSql)
                        .flatMap(new OperatorFlatMap(operator, sinkEdge.sourceDatabase, sinkEdge.sourceTable));
                rowDataStream.print();
                rowDataStream.addSink(getNebulaEdgeOption(sinkEdge, fields, positions, operator));
            }
        }

        env.execute("Print MySQL Snapshot + Binlog");


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
                .setEdge(sinkEdge.edgeName)
                .setSrcIndex(sinkEdge.srcIndex)
                .setDstIndex(sinkEdge.dstIndex)
                .setRankIndex(sinkEdge.rankIndex)
                .setFields(fields)
                .setPositions(positions)
                .setBatch(5)
                .setBathIntervalMs(500L);
        NebulaBatchOutputFormat outPutFormat = new NebulaBatchOutputFormat(graphConnectionProvider, metaConnectionProvider)
                .setExecutionOptions(executionOptionBuilder.builder());
        NebulaSinkFunction nebulaSinkFunction = new NebulaSinkFunction(outPutFormat);
        return nebulaSinkFunction;

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
                .setTag(sinkTag.tagName)
                .setIdIndex(sinkTag.idIndex)
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


}
