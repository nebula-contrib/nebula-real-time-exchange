import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;
import java.util.stream.Collectors;

public class typeMap implements DebeziumDeserializationSchema {

    @Override
    public void deserialize(SourceRecord record, Collector out) throws Exception {
        Envelope.Operation op = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            String insert = extractAfterRow(value, valueSchema);
            out.collect(new Tuple2<>(true, insert));
        } else if (op == Envelope.Operation.DELETE) {
            String delete = extractBeforeRow(value, valueSchema);
            out.collect(new Tuple2<>(false, delete));
        } else {
            String after = extractAfterRow(value, valueSchema);
            out.collect(new Tuple2<>(true, after));
        }
    }

    private Map<String, Object> getRowMap(Struct after) {
        return after.schema().fields().stream().collect(Collectors.toMap(Field::name, f -> after.get(f)));
    }

    private String extractAfterRow(Struct value, Schema valueSchema) throws Exception {
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        Map<String, Object> rowMap = getRowMap(after);
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(rowMap);
    }

    private String extractBeforeRow(Struct value, Schema valueSchema) throws Exception {
        Struct after = value.getStruct(Envelope.FieldName.BEFORE);
        Map<String, Object> rowMap = getRowMap(after);
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(rowMap);
    }

    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple2<Boolean, String>>() {
        });
    }
}



