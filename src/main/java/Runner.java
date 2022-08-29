import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
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
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yaml.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Runner {
    private static final Logger LOG = LoggerFactory.getLogger(Runner.class);

    public static void main(String[] args) throws IOException {


        Yaml configYaml = new Yaml(new Constructor(Mysql2NebulaConfig.class));
        InputStream configInput = Files.newInputStream(Paths.get(args[0]));
        Mysql2NebulaConfig config = configYaml.loadAs(configInput, Mysql2NebulaConfig.class);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);

        HashMap<String, DataStreamSource<String>> mysqlSourceMap = sourceFromMysql(config,env);

        // TODO:Add persistent backend support
        //env.setStateBackend(new FsStateBackend("hdfs://127.0.0.1:9000/test`"));
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));

        sinkToNebula(config, mysqlSourceMap);
        try {
            env.execute("MySQL real-time synchronization to Nebula Graph");
        } catch (Exception e) {
            LOG.error("Cannot start real-time synchronization %s", e);
        }
    }


    private static HashMap<String, DataStreamSource<String>> sourceFromMysql(Mysql2NebulaConfig config, StreamExecutionEnvironment env) {
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

    private static void sinkToNebula(Mysql2NebulaConfig config, HashMap<String, DataStreamSource<String>> mysqlSourceMap) {
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
}
