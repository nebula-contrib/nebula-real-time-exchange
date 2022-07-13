import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.connector.nebula.connection.NebulaClientOptions;
import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.sink.NebulaBatchOutputFormat;
import org.apache.flink.connector.nebula.sink.NebulaSinkFunction;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.HashMap;
import java.util.logging.Logger;

public class CdcTest {

    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2、连接mysql数据源
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

        //3、添加到env
        DataStreamSource<String> rowDataStreamSource = env.addSource(sourceFunction);
        DataStream<Row> rowDataStream = rowDataStreamSource.map(row -> {
            HashMap map = new Gson().fromJson(row, HashMap.class);

            LinkedTreeMap data = (LinkedTreeMap) map.get("data");
            Row record = new Row(data.size());
            record.setField(0, data.get("id"));
            record.setField(1, data.get("name"));
            record.setField(2, data.get("age"));
            return record;

        });

        //输出
        rowDataStream.addSink(getNebulaOptions());
//        env.setRestartStrategy(
//                RestartStrategies.fixedDelayRestart(
//                        3, // 尝试重启的次数
//                        Time.of(10, TimeUnit.SECONDS) // 间隔
//                ));


        env.execute("mysql_nebula_sync");
    }

    public static NebulaSinkFunction getNebulaOptions() {
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
