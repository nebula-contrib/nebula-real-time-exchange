import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * deserialize debezium format binlog
 */
public class CommonStringDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
    private final SimpleDateFormat sdf = new SimpleDateFormat("hh:mm:ss");
    private static final Logger LOG = LoggerFactory.getLogger(CommonStringDebeziumDeserializationSchema.class);
    private final ArrayList<String> geoFormat = new ArrayList<>(Arrays.asList("Point", "LineString", "Polygon"));

    public void deserialize(SourceRecord record, Collector<String> out) {
        System.out.println(record);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("ts_sec", (Long) record.sourceOffset().get("ts_sec"));
        String[] name = record.valueSchema().name().split("\\.");
        jsonObject.put("db", name[1]);
        jsonObject.put("table", name[2]);
        Struct value = (Struct) record.value();
        String operatorType = value.getString("op");
        jsonObject.put("op", operatorType);

        String valueFrom = "d".equals(operatorType) ? "before" : "after";
        JSONObject dataJsonObject = parseRecord(value.getStruct(valueFrom));
        jsonObject.put("data", dataJsonObject);

        Struct key = (Struct) record.key();
        JSONArray keyArray = new JSONArray();
        for (Field field : key.schema().fields()) {
            keyArray.add(field.name());
        }

        //jsonObject.put("key", keyArray);需设置sink所需的Index为主键才能正常update index


        jsonObject.put("parse_time", System.currentTimeMillis() / 1000);
        out.collect(jsonObject.toString());
    }

    protected JSONObject parseRecord(Struct after) {
        JSONObject jo = new JSONObject();

        for (Field field : after.schema().fields()) {
            if (after.get(field.name()) != null) {
                if (field.schema().type().isPrimitive()) {
                    switch (field.schema().type()) {
                        case INT8:
                            Byte resultInt8 = after.getInt8(field.name());
                            jo.put(field.name(), resultInt8);
                            break;
                        case INT16:
                            Short resultInt16 = after.getInt16(field.name());
                            jo.put(field.name(), resultInt16);
                            break;
                        case INT32:
                            Integer resultInt32 = after.getInt32(field.name());
                            jo.put(field.name(), resultInt32);
                            break;
                        case INT64:
                            Long resultInt = after.getInt64(field.name());
                            jo.put(field.name(), resultInt);
                            break;
                        case FLOAT32:
                            Float resultFloat32 = after.getFloat32(field.name());
                            jo.put(field.name(), resultFloat32);
                            break;
                        case FLOAT64:
                            Double resultFloat64 = after.getFloat64(field.name());
                            jo.put(field.name(), resultFloat64);
                            break;
                        case BYTES:
                            // json ignore byte column
                            byte[] resultByte = after.getBytes(field.name());
                            jo.put(field.name(), Arrays.toString(resultByte));
                            break;
                        case STRING:
                            String resultStr = after.getString(field.name());
                            jo.put(field.name(), resultStr);
                            break;
                        default:
                    }
                } else {

                    switch (field.schema().name()) {
                        case Date.SCHEMA_NAME:
                            String resultDateStr = TemporalConversions.toLocalDate(after.get(field.name())).toString();
                            jo.put(field.name(), resultDateStr);
                            break;
                        case Timestamp.SCHEMA_NAME:
                            TimestampData resultTime = TimestampData.fromEpochMillis((Long) after.get(field.name()));
                            jo.put(field.name(), String.valueOf(resultTime));
                            break;
                        case MicroTimestamp.SCHEMA_NAME:
                            long micro = (Long) after.get(field.name());
                            TimestampData resultMicroTimestamp = TimestampData.fromEpochMillis(micro / 1000L,
                                    (int) (micro % 1000L * 1000L));
                            jo.put(field.name(), String.valueOf(resultMicroTimestamp));
                            break;
                        case NanoTimestamp.SCHEMA_NAME:
                            long nano = (Long) after.get(field.name());
                            TimestampData resultNanoTimestamp = TimestampData.fromEpochMillis(nano / 1000000L,
                                    (int) (nano % 1000000L));
                            jo.put(field.name(), String.valueOf(resultNanoTimestamp));
                            break;
                        case ZonedTimestamp.SCHEMA_NAME:
                            if (after.get(field.name()) instanceof String) {
                                String str = (String) after.get(field.name());
                                Instant instant = Instant.parse(str);

                                ZoneId serverTimeZone = ZoneId.systemDefault();
                                TimestampData resultZonedTimestamp = TimestampData.fromLocalDateTime(
                                        LocalDateTime.ofInstant(instant, serverTimeZone));
                                jo.put(field.name(), String.valueOf(resultZonedTimestamp));
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
                            jo.put(field.name(), resultMicroTimeStr);
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
                            jo.put(field.name(), resultNanoTimeStr);
                            break;
                        case Geometry.LOGICAL_NAME:
                            Struct geoStruct = after.getStruct(field.name());
                            WKBReader wkbReader = new WKBReader();
                            byte[] wkbs = geoStruct.getBytes("wkb");

//                            int srid = 4326;
//                            if (geoStruct.get("srid") != null) {
//                                srid = (int) geoStruct.get("srid");
//                            }
                            try {
                                org.locationtech.jts.geom.Geometry geoWkb = wkbReader.read(wkbs);
                                //FIXME:NebulaGraph中没有srid，是否需要投影转换
                                //geoWkb.setSRID(srid);
                                String geoWkbStr;
                                if (!geoFormat.contains(geoWkb.getGeometryType())) {
                                    geoWkbStr = null;
                                    LOG.warn("Cannot Sink " + geoWkb.toText() + " to Nebula Graph, because" + geoWkb.getGeometryType());
                                } else {
                                    geoWkbStr = geoWkb.toText();
                                }
                                jo.put(field.name(), geoWkbStr);
                            } catch (ParseException e) {
                                throw new RuntimeException(e);
                            }
                    }
                }
            } else {
                jo.put(field.name(), null);
            }
        }

        return jo;
    }

    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}