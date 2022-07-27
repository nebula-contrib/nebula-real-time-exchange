import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.DeserializationRuntimeConverter;
import com.ververica.cdc.debezium.table.DeserializationRuntimeConverterFactory;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class RowDeserializeSchema implements DebeziumDeserializationSchema<Row> {
    private final DeserializationRuntimeConverter physicalConverter;

    public RowDeserializeSchema() {
        this.physicalConverter = (DeserializationRuntimeConverter) DeserializationRuntimeConverterFactory.DEFAULT;
    }

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<Row> collector) throws Exception {
        Envelope.Operation op = Envelope.operationFor(sourceRecord);
        Struct value = (Struct)sourceRecord.value();
        Schema valueSchema = sourceRecord.valueSchema();
        Row row;
        if (op != Envelope.Operation.CREATE && op != Envelope.Operation.READ) {
            if (op == Envelope.Operation.UPDATE) {
                row = this.extractAfterRow(value, valueSchema);
                row.setKind(RowKind.UPDATE_AFTER);
            } else {
                row = this.extractBeforeRow(value, valueSchema);
                row.setKind(RowKind.DELETE);
            }
        } else {
        }


    }

    private Row extractAfterRow(Struct value, Schema valueSchema) {
        Schema afterSchema = valueSchema.field("after").schema();
        Struct after = value.getStruct("after");
        return null;
    }

    private Row extractBeforeRow(Struct value, Schema valueSchema) {
        Schema beforeSchema = valueSchema.field("before").schema();
        Struct before = value.getStruct("before");
//        return (Row) this.convert(before,beforeSchema);
        return null;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return BasicTypeInfo.of(Row.class);
    }
}
