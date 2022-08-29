import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.types.Row;
import org.junit.Test;
import yaml.NebulaPropIndex;
import yaml.SinkEdge;
import yaml.SinkTag;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class OperatorFlatMapTest {

    @Test
    public void testTagFlatMap() {
        String insertOp1 = "{\"op\":\"r\",\"ts_sec\":1661328784,\"data\":{\"name\":\"djs\",\"id\":4,\"age\":12},\"parse_time\":1661328785,\"db\":\"test\",\"table\":\"person\",\"key\":[\"id\"]}";
        String insertOp2 = "{\"op\":\"c\",\"ts_sec\":1661328784,\"data\":{\"name\":\"djs\",\"id\":4,\"age\":12},\"parse_time\":1661328785,\"db\":\"test\",\"table\":\"person\",\"key\":[\"id\"]}";
        String updateOp = "{\"op\":\"u\",\"ts_sec\":1661328784,\"data\":{\"name\":\"djs\",\"id\":4,\"age\":12},\"parse_time\":1661328785,\"db\":\"test\",\"table\":\"person\",\"key\":[\"id\"]}";
        String deleteOp = "{\"op\":\"d\",\"ts_sec\":1661328784,\"data\":{\"name\":\"djs\",\"id\":4,\"age\":12},\"parse_time\":1661328785,\"db\":\"test\",\"table\":\"person\",\"key\":[\"id\"]}";
        SinkTag sinkTag = new SinkTag();
        sinkTag.setSourceDatabase("test");
        sinkTag.setSourceTable("person");
        NebulaPropIndex tagIndex = new NebulaPropIndex();
        tagIndex.setSqlCol("id");
        sinkTag.setIdIndex(tagIndex);
        ArrayList<String> tagColumn = new ArrayList<>(Arrays.asList("id", "name", "age"));
        ArrayList<Integer> positions = new ArrayList<>(Arrays.asList(0, 1, 2));
        OperatorFlatMap operatorFlatMapInsert = new OperatorFlatMap(WriteModeEnum.INSERT, sinkTag, tagColumn);
        OperatorFlatMap operatorFlatMapUpdate = new OperatorFlatMap(WriteModeEnum.UPDATE, sinkTag, tagColumn);
        OperatorFlatMap operatorFlatMapDelete = new OperatorFlatMap(WriteModeEnum.DELETE, sinkTag, tagColumn);
        List<Row> out = new ArrayList<>();
        Row row = new Row(3);
        row.setField(0, 4);
        row.setField(1, "djs");
        row.setField(2, 12);
        ListCollector<Row> collector = new ListCollector<>(out);
        operatorFlatMapInsert.flatMap(insertOp1, collector);
        assertEquals(row, out.get(0));
        out.clear();
        operatorFlatMapInsert.flatMap(insertOp2, collector);
        assertEquals(row, out.get(0));
        out.clear();
        operatorFlatMapUpdate.flatMap(updateOp, collector);
        assertEquals(row, out.get(0));
        out.clear();
        operatorFlatMapDelete.flatMap(deleteOp, collector);
        assertEquals(row, out.get(0));

    }

    @Test
    public void testEdgeFlatMap() {
        String insertOp1 = "{\"op\":\"r\",\"ts_sec\":1661330815,\"data\":{\"col14\":\"POINT (108.9465236664 34.2598766768)\",\"col13\":\"10:12:13\",\"col12\":1.0,\"col11\":1.2,\"ranks\":6,\"srcId\":2,\"col8\":\"2019-01-01T12:12:12\",\"dstId\":12,\"col9\":\"2022-07-26T12:50:43\",\"col6\":6412233,\"col10\":0,\"col7\":\"2019-01-03\",\"col4\":1233,\"col5\":35353,\"col2\":\"adbbb\",\"col1\":\"aasd\"},\"parse_time\":1661330816,\"db\":\"test\",\"table\":\"friend\",\"key\":[\"srcId\",\"dstId\",\"ranks\"]}";
        String insertOp2 = "{\"op\":\"c\",\"ts_sec\":1661330815,\"data\":{\"col14\":\"POINT (108.9465236664 34.2598766768)\",\"col13\":\"10:12:13\",\"col12\":1.0,\"col11\":1.2,\"ranks\":6,\"srcId\":2,\"col8\":\"2019-01-01T12:12:12\",\"dstId\":12,\"col9\":\"2022-07-26T12:50:43\",\"col6\":6412233,\"col10\":0,\"col7\":\"2019-01-03\",\"col4\":1233,\"col5\":35353,\"col2\":\"adbbb\",\"col1\":\"aasd\"},\"parse_time\":1661330816,\"db\":\"test\",\"table\":\"friend\",\"key\":[\"srcId\",\"dstId\",\"ranks\"]}";
        String updateOp = "{\"op\":\"u\",\"ts_sec\":1661330815,\"data\":{\"col14\":\"POINT (108.9465236664 34.2598766768)\",\"col13\":\"10:12:13\",\"col12\":1.0,\"col11\":1.2,\"ranks\":6,\"srcId\":2,\"col8\":\"2019-01-01T12:12:12\",\"dstId\":12,\"col9\":\"2022-07-26T12:50:43\",\"col6\":6412233,\"col10\":0,\"col7\":\"2019-01-03\",\"col4\":1233,\"col5\":35353,\"col2\":\"adbbb\",\"col1\":\"aasd\"},\"parse_time\":1661330816,\"db\":\"test\",\"table\":\"friend\",\"key\":[\"srcId\",\"dstId\",\"ranks\"]}";
        String deleteOp = "{\"op\":\"d\",\"ts_sec\":1661330815,\"data\":{\"col14\":\"POINT (108.9465236664 34.2598766768)\",\"col13\":\"10:12:13\",\"col12\":1.0,\"col11\":1.2,\"ranks\":6,\"srcId\":2,\"col8\":\"2019-01-01T12:12:12\",\"dstId\":12,\"col9\":\"2022-07-26T12:50:43\",\"col6\":6412233,\"col10\":0,\"col7\":\"2019-01-03\",\"col4\":1233,\"col5\":35353,\"col2\":\"adbbb\",\"col1\":\"aasd\"},\"parse_time\":1661330816,\"db\":\"test\",\"table\":\"friend\",\"key\":[\"srcId\",\"dstId\",\"ranks\"]}";
        SinkEdge sinkEdge = new SinkEdge();
        sinkEdge.setSourceDatabase("test");
        sinkEdge.setSourceTable("friend");
        NebulaPropIndex edgeIndex = new NebulaPropIndex();
        edgeIndex.setSqlCol("srcId");
        sinkEdge.setSrcIndex(edgeIndex);
        edgeIndex.setSqlCol("dstId");
        sinkEdge.setDstIndex(edgeIndex);
        edgeIndex.setSqlCol("ranks");
        sinkEdge.setRankIndex(edgeIndex);
        ArrayList<String> tagColumn = new ArrayList<>(Arrays.asList("srcId", "dstId", "col1", "col2", "ranks", "col4", "col5", "col6", "col7", "col8", "col9", "col10", "col11", "col12", "col13", "col14"));
        ArrayList<Integer> positions = new ArrayList<>(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16));
        List<Row> out = new ArrayList<>();
        Row row = new Row(16);
        row.setField(0, 2);
        row.setField(1, 12);
        row.setField(2, "aasd");
        row.setField(3, "adbbb");
        row.setField(4, 6);
        row.setField(5, 1233);
        row.setField(6, 35353);
        row.setField(7, 6412233);
        row.setField(8, "2019-01-03");
        row.setField(9, "2019-01-01T12:12:12");
        row.setField(10, "2022-07-26T12:50:43");
        row.setField(11, 0);
        row.setField(12, new BigDecimal(1.2).setScale(1, BigDecimal.ROUND_HALF_UP));
        row.setField(13, new BigDecimal(1.0).setScale(1, BigDecimal.ROUND_HALF_UP));
        row.setField(14, "10:12:13");
        row.setField(15, "POINT (108.9465236664 34.2598766768)");
        ListCollector<Row> collector = new ListCollector<>(out);
        OperatorFlatMap operatorFlatMapInsert = new OperatorFlatMap(WriteModeEnum.INSERT, sinkEdge, tagColumn);
        OperatorFlatMap operatorFlatMapUpdate = new OperatorFlatMap(WriteModeEnum.UPDATE, sinkEdge, tagColumn);
        OperatorFlatMap operatorFlatMapDelete = new OperatorFlatMap(WriteModeEnum.DELETE, sinkEdge, tagColumn);
        operatorFlatMapInsert.flatMap(insertOp1, collector);
        assertEquals(row, out.get(0));
        out.clear();
        operatorFlatMapInsert.flatMap(insertOp2, collector);
        assertEquals(row, out.get(0));
        out.clear();
        operatorFlatMapUpdate.flatMap(updateOp, collector);
        assertEquals(row, out.get(0));
        out.clear();
        operatorFlatMapDelete.flatMap(deleteOp, collector);
        assertEquals(row, out.get(0));
    }
}
