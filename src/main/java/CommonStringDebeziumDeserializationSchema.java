import com.google.gson.JsonObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.utils.TemporalConversions;
import io.debezium.data.geometry.Geometry;
import io.debezium.time.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;

/**
 * deserialize debezium format binlog
 */
public class CommonStringDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

    public void deserialize(SourceRecord record, Collector<String> out) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("ts_sec", (Long) record.sourceOffset().get("ts_sec"));
        String[] name = record.valueSchema().name().split("\\.");
        jsonObject.addProperty("db", name[1]);
        jsonObject.addProperty("table", name[2]);
        Struct value = ((Struct) record.value());
        String operatorType = value.getString("op");
        jsonObject.addProperty("op", operatorType);
        // c : create, u: update, d: delete, r: read
        // insert update
        if (!"d".equals(operatorType)) {
            Struct after = value.getStruct("after");
            JsonObject afterJsonObject = parseRecord(after);
            jsonObject.add("after", afterJsonObject);
        }
        // update & delete
        if ("u".equals(operatorType) || "d".equals(operatorType)) {
            Struct source = value.getStruct("before");
            JsonObject beforeJsonObject = parseRecord(source);
            jsonObject.add("before", beforeJsonObject);
        }
        jsonObject.addProperty("parse_time", System.currentTimeMillis() / 1000);

        out.collect(jsonObject.toString());
    }

    private JsonObject parseRecord(Struct after) {
        JsonObject jo = new JsonObject();
        SimpleDateFormat sdf = new SimpleDateFormat("hh:mm:ss");
        for (Field field : after.schema().fields()) {

            if (field.schema().name() == null) {
                switch ((field.schema()).type()) {
                    case INT8:
                        int resultInt8 = after.getInt8(field.name());
                        jo.addProperty(field.name(), resultInt8);
                        break;
                    case INT16:
                        int resultInt16 = after.getInt16(field.name());
                        jo.addProperty(field.name(), resultInt16);
                        break;
                    case INT32:
                        int resultInt32 = after.getInt32(field.name());
                        jo.addProperty(field.name(), resultInt32);
                        break;
                    case INT64:
                        Long resultInt = after.getInt64(field.name());
                        jo.addProperty(field.name(), resultInt);
                        break;
                    case FLOAT32:
                        Float resultFloat32 = after.getFloat32(field.name());
                        jo.addProperty(field.name(), resultFloat32);
                        break;
                    case FLOAT64:
                        Double resultFloat64 = after.getFloat64(field.name());
                        jo.addProperty(field.name(), resultFloat64);
                        break;
                    case BYTES:
                        // json ignore byte column
                         byte[] resultByte = after.getBytes(field.name());
                         jo.addProperty(field.name(), Arrays.toString(resultByte));
                        break;
                    case STRING:
                        String resultStr = after.getString(field.name());
                        jo.addProperty(field.name(), resultStr);
                        break;
                    default:
                }
            } else {
                switch (field.schema().name()) {
                    case Date.SCHEMA_NAME:
                        String resultDateStr = TemporalConversions.toLocalDate(after.get(field.name())).toString();
                        jo.addProperty(field.name(), resultDateStr);
                        break;
                    case Timestamp.SCHEMA_NAME:
                        TimestampData resultTime = TimestampData.fromEpochMillis((Long) after.get(field.name()));
                        jo.addProperty(field.name(), String.valueOf(resultTime));
                        break;
                    case MicroTimestamp.SCHEMA_NAME:
                        long micro = (Long) after.get(field.name());
                        TimestampData resultMicroTimestamp = TimestampData.fromEpochMillis(micro / 1000L,
                                (int) (micro % 1000L * 1000L));
                        jo.addProperty(field.name(), String.valueOf(resultMicroTimestamp));
                        break;
                    case NanoTimestamp.SCHEMA_NAME:
                        long nano = (Long) after.get(field.name());
                        TimestampData resultNanoTimestamp = TimestampData.fromEpochMillis(nano / 1000000L,
                                (int) (nano % 1000000L));
                        jo.addProperty(field.name(), String.valueOf(resultNanoTimestamp));
                        break;
                    case ZonedTimestamp.SCHEMA_NAME:
                        if (after.get(field.name()) instanceof String) {
                            String str = (String) after.get(field.name());
                            Instant instant = Instant.parse(str);

                            ZoneId serverTimeZone = ZoneId.systemDefault();
                            TimestampData resultZonedTimestamp = TimestampData.fromLocalDateTime(
                                    LocalDateTime.ofInstant(instant, serverTimeZone));
                            jo.addProperty(field.name(), String.valueOf(resultZonedTimestamp));
                        } else {
                            throw new IllegalArgumentException("Unable to convert to TimestampData from unexpected value ");
                        }
                        break;
                    case MicroTime.SCHEMA_NAME:
                        int resultMicroTime;
                        if (after.get(field.name()) instanceof Long) {
                            resultMicroTime = (int) ((Long) after.get(field.name()) / 1000L);
                        } else if (after.get(field.name()) instanceof Integer) {
                            resultMicroTime = (int) after.get(field.name());
                        } else {
                            resultMicroTime = TemporalConversions.toLocalTime(after.get(field.name()))
                                    .toSecondOfDay() * 1000;
                        }
                        String resultMicroTimeStr = sdf.format(resultMicroTime);
                        jo.addProperty(field.name(), resultMicroTimeStr);
                        break;
                    case NanoTime.SCHEMA_NAME:
                        int resultNanoTime;
                        if (after.get(field.name()) instanceof Long) {
                            resultNanoTime = (int) ((Long) after.get(field.name()) / 1000000L);
                        } else if (after.get(field.name()) instanceof Integer) {
                            resultNanoTime = (int) after.get(field.name());
                        } else {
                            resultNanoTime = TemporalConversions.toLocalTime(after.get(field.name()))
                                    .toSecondOfDay() * 1000;
                        }
                        String resultNanoTimeStr = sdf.format(resultNanoTime);
                        jo.addProperty(field.name(), resultNanoTimeStr);
                        break;
                    case Geometry.LOGICAL_NAME:
                        Struct geoStruct = after.getStruct(field.name());
                        WKBReader wkbReader = new WKBReader();
                        byte[] wkbs = geoStruct.getBytes("wkb");
                        try {
                            org.locationtech.jts.geom.Geometry geoWkb = wkbReader.read(wkbs);
                            String geoWkbStr = geoWkb.toText();
                            jo.addProperty(field.name(), geoWkbStr);
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }


                }
            }
        }

        return jo;
    }

    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}