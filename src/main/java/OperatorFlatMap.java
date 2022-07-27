import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.LinkedHashMap;

public class OperatorFlatMap implements FlatMapFunction<String, Row> {

    public String operator;
    public String sourceDatabase;
    public String sourceTable;

    public OperatorFlatMap(WriteModeEnum writeModeEnum, String sourceDatabase, String sourceTable) {
        switch (writeModeEnum) {
            case INSERT:
                this.operator = "r|c";
                break;
            case UPDATE:
                this.operator = "u";
                break;
            default:
                this.operator = "d";
        }
        this.sourceDatabase = sourceDatabase;
        this.sourceTable = sourceTable;
    }

    @Override
    public void flatMap(String s, Collector<Row> collector) throws Exception {
        JSONObject jsonObject = JSON.parseObject(s, Feature.OrderedField);
        String op = jsonObject.getString("op");
        String table = jsonObject.getString("table");
        String db = jsonObject.getString("db");
        LinkedHashMap dataMap;
        if (!op.equals("d")) {
            dataMap = JSON.parseObject(jsonObject.getString("after"), LinkedHashMap.class, Feature.OrderedField);
        } else {
            dataMap = JSON.parseObject(jsonObject.getString("before"), LinkedHashMap.class, Feature.OrderedField);
        }
        ArrayList data = new ArrayList<>(dataMap.values());
        Row row = new Row(data.size());
        if (operator.contains(op) && db.equals(sourceDatabase) && table.equals(sourceTable)) {
            for (int i = 0; i < data.size(); i++) {
                    row.setField(i, data.get(i));
            }
            collector.collect(row);
        }

    }


}
