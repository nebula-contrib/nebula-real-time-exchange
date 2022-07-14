import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.nebula.connection.NebulaClientOptions;
import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.sink.NebulaBatchOutputFormat;
import org.apache.flink.connector.nebula.sink.NebulaSinkFunction;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class CdcTest {

    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2、连接mysql数据源
        DebeziumSourceFunction<Map> sourceFunction = MySqlSource.<Map>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("root")
                .password("162331")
                .databaseList("test")
                .tableList("test.person")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyJsonDebeziumDeserializationSchema())
                .build();

        //3、添加到env
        DataStreamSource<Map> rowDataStreamSource = env.addSource(sourceFunction);
        SingleOutputStreamOperator<Map> streamInsertOperator = rowDataStreamSource.filter(row -> {
            System.out.println(row.getClass().getName());
                    return (row.get("type").equals("insert") | row.get("type").equals("insert"));
                }
        );
        SingleOutputStreamOperator<Map> streamUpdateOperator = rowDataStreamSource.filter(row ->
                (row.get("type").equals("update")));
        SingleOutputStreamOperator<Map> streamDeleteOperator = rowDataStreamSource.filter(row ->
                (row.get("type").equals("delete")));
        DataStream<Row> rowInsertDataStream = streamInsertOperator.map(row -> {
            LinkedTreeMap data = (LinkedTreeMap) row.get("data");
            Row record = new Row(data.size());
            record.setField(0, data.get("id"));
            record.setField(1, data.get("name"));
            record.setField(2, data.get("age"));
            return record;

        });
        DataStream<Row> rowUpdateDataStream = streamUpdateOperator.map(row -> {
            LinkedTreeMap data = (LinkedTreeMap) row.get("data");
            Row record = new Row(data.size());
            record.setField(0, data.get("id"));
            record.setField(1, data.get("name"));
            record.setField(2, data.get("age"));
            return record;

        });
        DataStream<Row> rowDeleteDataStream = streamDeleteOperator.map(row -> {
            LinkedTreeMap data = (LinkedTreeMap) row.get("data");

            Row record = new Row(data.size());
            record.setField(0, data.get("id"));
            record.setField(1, data.get("name"));
            record.setField(2, data.get("age"));
            return record;

        });


        //输出
        rowInsertDataStream.addSink(getNebulaInsertOptions());
        rowUpdateDataStream.addSink(getNebulaUpdateOptions());
        rowDeleteDataStream.addSink(getNebulaDeleteOptions());



//        env.setRestartStrategy(
//                RestartStrategies.fixedDelayRestart(
//                        3, // 尝试重启的次数
//                        Time.of(10, TimeUnit.SECONDS) // 间隔
//                ));


        env.execute("mysql_nebula_sync");
    }

    private static SinkFunction<Row> getNebulaDeleteOptions() {
        NebulaClientOptions nebulaClientOptions =
                new NebulaClientOptions.NebulaClientOptionsBuilder()
                        .setGraphAddress("127.0.0.1:9669")
                        .setMetaAddress("127.0.0.1:9559")
                        .build();
        NebulaGraphConnectionProvider graphConnectionProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);
        NebulaMetaConnectionProvider metaConnectionProvider =
                new NebulaMetaConnectionProvider(nebulaClientOptions);

        ExecutionOptions executionOptions = new VertexExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSink")
                .setTag("person")
                .setIdIndex(0)
                .setFields(Arrays.asList("name", "age"))
                .setPositions(Arrays.asList(1, 2))
                .setWriteMode(WriteModeEnum.DELETE)
                .setBatch(5)
                .setBathIntervalMs(500L)
                .builder();


        NebulaBatchOutputFormat outPutFormat =
                new NebulaBatchOutputFormat(graphConnectionProvider, metaConnectionProvider)
                        .setExecutionOptions(executionOptions);
        NebulaSinkFunction nebulaSinkFunction = new NebulaSinkFunction(outPutFormat);
        return nebulaSinkFunction;
    }

    private static SinkFunction<Row> getNebulaUpdateOptions() {
        NebulaClientOptions nebulaClientOptions =
                new NebulaClientOptions.NebulaClientOptionsBuilder()
                        .setGraphAddress("127.0.0.1:9669")
                        .setMetaAddress("127.0.0.1:9559")
                        .build();
        NebulaGraphConnectionProvider graphConnectionProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);
        NebulaMetaConnectionProvider metaConnectionProvider =
                new NebulaMetaConnectionProvider(nebulaClientOptions);

        ExecutionOptions executionOptions = new VertexExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSink")
                .setTag("person")
                .setIdIndex(0)
                .setFields(Arrays.asList("name", "age"))
                .setPositions(Arrays.asList(1, 2))
                .setWriteMode(WriteModeEnum.UPDATE)
                .setBatch(5)
                .setBathIntervalMs(500L)
                .builder();


        NebulaBatchOutputFormat outPutFormat =
                new NebulaBatchOutputFormat(graphConnectionProvider, metaConnectionProvider)
                        .setExecutionOptions(executionOptions);
        NebulaSinkFunction nebulaSinkFunction = new NebulaSinkFunction(outPutFormat);
        return nebulaSinkFunction;
    }

    public static NebulaSinkFunction getNebulaInsertOptions() {
        NebulaClientOptions nebulaClientOptions =
                new NebulaClientOptions.NebulaClientOptionsBuilder()
                        .setGraphAddress("127.0.0.1:9669")
                        .setMetaAddress("127.0.0.1:9559")
                        .build();
        NebulaGraphConnectionProvider graphConnectionProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);
        NebulaMetaConnectionProvider metaConnectionProvider =
                new NebulaMetaConnectionProvider(nebulaClientOptions);

        ExecutionOptions executionOptions = new VertexExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSink")
                .setTag("person")
                .setIdIndex(0)
                .setFields(Arrays.asList("name", "age"))
                .setPositions(Arrays.asList(1, 2))
                .setBatch(5)
                .setBathIntervalMs(500L)
                .builder();


        NebulaBatchOutputFormat outPutFormat =
                new NebulaBatchOutputFormat(graphConnectionProvider, metaConnectionProvider)
                        .setExecutionOptions(executionOptions);
        NebulaSinkFunction nebulaSinkFunction = new NebulaSinkFunction(outPutFormat);
        return nebulaSinkFunction;
    }

}
