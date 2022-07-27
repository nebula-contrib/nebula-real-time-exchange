import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.sink.NebulaBatchOutputFormat;
import org.apache.flink.connector.nebula.sink.NebulaSinkFunction;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.connector.nebula.connection.NebulaClientOptions;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Arrays;
import java.util.HashMap;

public class edgeTest {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("root")
                .password("162331")
                .databaseList("test")
                .tableList("test.person")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyJsonDebeziumDeserializationSchema())
                .build();
        NebulaClientOptions nebulaClientOptions =
                new NebulaClientOptions.NebulaClientOptionsBuilder()
                        .setGraphAddress("127.0.0.1:9669")
                        .setMetaAddress("127.0.0.1:9559")
                        .build();
        NebulaGraphConnectionProvider graphConnectionProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);
        NebulaMetaConnectionProvider metaConnectionProvider =
                new NebulaMetaConnectionProvider(nebulaClientOptions);
        DataStream<String> rowInsertFilter = env.addSource(sourceFunction).filter(row -> {
            HashMap map = new Gson().fromJson(row, HashMap.class);
            return (map.get("type").equals("insert") | map.get("type").equals("read"));
        });
        DataStream<Row> rowInsertDataStream = rowInsertFilter.map(row -> {
            HashMap map = new Gson().fromJson(row, HashMap.class);
            LinkedTreeMap data = (LinkedTreeMap) map.get("data");
            System.out.println(data.size());
            Row record = new Row(data.size());
            record.setField(0, data.get("id"));
            record.setField(1, data.get("name"));
            record.setField(2, data.get("age"));
            return record;
        });
        rowInsertDataStream.print();

        DataStream<String> rowUpdateFilter = env.addSource(sourceFunction).filter(row -> {
            HashMap map = new Gson().fromJson(row, HashMap.class);
            return (map.get("type").equals("update"));
        });
        DataStream<Row> rowUpdateDataStream = rowUpdateFilter.map(row -> {
            HashMap map = new Gson().fromJson(row, HashMap.class);
            LinkedTreeMap data = (LinkedTreeMap) map.get("data");
            Row record = new Row(data.size());
            record.setField(0, data.get("id"));
            record.setField(1, data.get("name"));
            record.setField(2, data.get("age"));
            return record;
        });

        DataStream<String> rowDeleteFilter = env.addSource(sourceFunction).filter(row -> {
            HashMap map = new Gson().fromJson(row, HashMap.class);
            return (map.get("type").equals("delete"));
        });
        DataStream<Row> rowDeleteDataStream = rowDeleteFilter.map(row -> {
            HashMap map = new Gson().fromJson(row, HashMap.class);
            LinkedTreeMap data = (LinkedTreeMap) map.get("data");
            Row record = new Row(data.size());
            record.setField(0, data.get("id"));
            record.setField(1, data.get("name"));
            record.setField(2, data.get("age"));
            return record;
        });
        rowInsertDataStream.addSink(getNebulaInsertOptions(graphConnectionProvider,metaConnectionProvider));
        rowUpdateDataStream.addSink(getNebulaUpdateOptions(graphConnectionProvider,metaConnectionProvider));
        rowDeleteDataStream.addSink(getNebulaDeleteOptions(graphConnectionProvider,metaConnectionProvider));
        try {
            env.execute("mysql_nebula_sync");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static SinkFunction<Row> getNebulaDeleteOptions(NebulaGraphConnectionProvider graphConnectionProvider, NebulaMetaConnectionProvider metaConnectionProvider) {

        ExecutionOptions executionOptions = new VertexExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSink")
                .setTag("person_simple")
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

    private static SinkFunction<Row> getNebulaUpdateOptions(NebulaGraphConnectionProvider graphConnectionProvider, NebulaMetaConnectionProvider metaConnectionProvider) {


        ExecutionOptions executionOptions = new VertexExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSink")
                .setTag("person_simple")
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

    public static NebulaSinkFunction getNebulaInsertOptions(NebulaGraphConnectionProvider graphConnectionProvider, NebulaMetaConnectionProvider metaConnectionProvider) {


        ExecutionOptions executionOptions = new VertexExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSink")
                .setTag("person_simple")
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
