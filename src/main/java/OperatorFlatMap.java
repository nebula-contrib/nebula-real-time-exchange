import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yaml.AbstractSinkType;
import yaml.SinkEdge;
import yaml.SinkTag;

import java.util.ArrayList;
import java.util.LinkedHashMap;

public class OperatorFlatMap implements FlatMapFunction<String, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(OperatorFlatMap.class);
    private final ArrayList<String> columnList;
    private final String sourceDatabase;
    private final String sourceTable;
    private final ArrayList<String> indexColumn;
    public String operator;


    public OperatorFlatMap(WriteModeEnum writeModeEnum, AbstractSinkType abstractSinkType, ArrayList<String> sqlColumn) {
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
        this.sourceDatabase = abstractSinkType.sourceDatabase;
        this.sourceTable = abstractSinkType.sourceTable;
        ArrayList<String> indexColumn = new ArrayList<>();

        if (abstractSinkType.getSinkType().equals("tag")) {
            SinkTag tag = (SinkTag) abstractSinkType;
            indexColumn.add(tag.idIndex.sqlCol);
        } else {
            SinkEdge edge = (SinkEdge) abstractSinkType;
            indexColumn.add(edge.srcIndex.sqlCol);
            indexColumn.add(edge.dstIndex.sqlCol);
            indexColumn.add(edge.rankIndex.sqlCol);
        }
        this.indexColumn = indexColumn;
        this.columnList = sqlColumn;


    }

    @Override
    public void flatMap(String s, Collector<Row> collector) {
        JSONObject jsonObject = JSON.parseObject(s, Feature.OrderedField);
        String op = jsonObject.getString("op");
        String table = jsonObject.getString("table");
        String db = jsonObject.getString("db");
        JSONArray keyArray = jsonObject.getJSONArray("key");
        if (operator.contains(op) && db.equals(sourceDatabase) && table.equals(sourceTable)) {
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


}
