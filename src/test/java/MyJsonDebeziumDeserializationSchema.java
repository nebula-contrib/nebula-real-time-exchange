import com.google.gson.Gson;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import java.util.HashMap;

public class MyJsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema {


    @Override
    public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {
        HashMap<String, Object> hashMap = new HashMap<>();

        String topic = sourceRecord.topic();
        String[] split = topic.split("[.]");
        String database = split[1];
        String table = split[2];
        hashMap.put("database",database);
        hashMap.put("table",table);


        //获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        //获取数据本身
        Struct struct = (Struct)sourceRecord.value();
        Struct after = struct.getStruct("after");
        Struct before = struct.getStruct("before");
        /*
            1，同时存在 beforeStruct 跟 afterStruct数据的话，就代表是update的数据
             2,只存在 beforeStruct 就是delete数据
             3，只存在 afterStruct数据 就是insert数据
        */


        if (after != null) {
            //insert
            Schema schema = after.schema();
            HashMap<String, Object> hm = new HashMap<>();
            for (Field field : schema.fields()) {
                hm.put(field.name(), after.get(field.name()));
            }
            hashMap.put("data",hm);
        }else if (before !=null){
            //delete
            Schema schema = before.schema();
            HashMap<String, Object> hm = new HashMap<>();
            for (Field field : schema.fields()) {
                hm.put(field.name(), before.get(field.name()));
            }
            hashMap.put("data",hm);
        }else if(before !=null && after !=null){
            //update
            Schema schema = after.schema();
            HashMap<String, Object> hm = new HashMap<>();
            for (Field field : schema.fields()) {
                hm.put(field.name(), after.get(field.name()));
            }
            hashMap.put("data",hm);
        }

        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }else if("delete".equals(type)) {
            type = "delete";
        }else if("update".equals(type)) {
            type = "update";
        }

        hashMap.put("type",type);

        Gson gson = new Gson();
        collector.collect(gson.toJson(hashMap));
    }

    @Override
    public TypeInformation getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
