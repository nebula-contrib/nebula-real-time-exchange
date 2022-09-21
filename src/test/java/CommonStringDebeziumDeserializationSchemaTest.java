import com.alibaba.fastjson.JSONObject;
import com.github.jsonzou.jmockdata.JMockData;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CommonStringDebeziumDeserializationSchemaTest {


    @Before
    public void init() {

//        Map<String, Object> sourcePartition = new HashMap<>();
//        sourcePartition.put("server", "mysql_binlog_source");
//        Map<String, Object> sourceOffset = new HashMap<>();
//        sourceOffset.put("transaction_id", null);
//        sourceOffset.put("ts_sec", 1661332619);
//        sourceOffset.put("file", "");
//        sourceOffset.put("pos", 0);

    }

    @Test
    public void parseRecordTest() {
        Schema schema = new SchemaBuilder(Schema.Type.STRUCT)
                .field("int8", Schema.INT8_SCHEMA)
                .field("int16", Schema.INT16_SCHEMA)
                .field("int32", Schema.INT32_SCHEMA)
                .field("int64", Schema.INT64_SCHEMA)
                .field("float32", Schema.FLOAT32_SCHEMA)
                .field("float64", Schema.FLOAT64_SCHEMA)
                .field("bytes", Schema.BYTES_SCHEMA)
                .field("string", Schema.STRING_SCHEMA)
                .build();
        byte int8 = JMockData.mock(Byte.class);
        short int16 = JMockData.mock(Short.class);
        int int32 = JMockData.mock(Integer.class);
        long int64 = JMockData.mock(Long.class);
        float float32 = JMockData.mock(Float.class);
        double float64 = JMockData.mock(Double.class);
        byte[] bytes = JMockData.mock(byte[].class);
        String string = JMockData.mock(String.class);


        JSONObject expected = new JSONObject();
        expected.put("int8", int8);
        expected.put("int16", int16);
        expected.put("int32", int32);
        expected.put("int64", int64);
        expected.put("float32", float32);
        expected.put("float64", float64);
        expected.put("bytes", Arrays.toString(bytes));
        expected.put("string", string);

        Struct data = new Struct(schema)
                .put("int8", int8)
                .put("int16", int16)
                .put("int32", int32)
                .put("int64", int64)
                .put("float32", float32)
                .put("float64", float64)
                .put("bytes", bytes)
                .put("string", string);

        JSONObject actual = new CommonStringDebeziumDeserializationSchema().parseRecord(data);
        assertEquals(expected, actual);


    }
}
