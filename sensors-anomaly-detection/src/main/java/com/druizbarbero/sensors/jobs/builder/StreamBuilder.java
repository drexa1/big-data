package com.druizbarbero.sensors.jobs.builder;

import com.druizbarbero.sensors.data.input.SensorsTupleType;
import com.druizbarbero.sensors.functions.TimestampExtractor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.TupleCsvInputFormat;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.IntStream;

public class StreamBuilder {

    private final Logger log = LoggerFactory.getLogger(StreamBuilder.class);

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;

    private String TIMESTAMP_HEADER = "date";
    private String SENSOR_HEADER = "Sensor-";

    public StreamBuilder(StreamExecutionEnvironment env) {
        this.env = env;
        this.tEnv = TableEnvironment.getTableEnvironment(env);
    }

    public DataStream build(String inputFilePath, boolean withAssignTimestampsAndWatermarks) {

        CsvTableSource.Builder builder = CsvTableSource.builder().path(inputFilePath)
                .ignoreFirstLine()
                .fieldDelimiter(",");

        // Date COLUMN
        builder.field(TIMESTAMP_HEADER, BasicTypeInfo.STRING_TYPE_INFO);
        // For as many sensor COLUMNS
        int nColumns = SensorsTupleType.tupleTypeInfo.getTotalFields() - 1;
        IntStream.rangeClosed(1, nColumns).forEach(column -> {
            builder.field(SENSOR_HEADER+column, SensorsTupleType.types[column]);
        });

        CsvTableSource sensorsSource = builder.build();
        tEnv.registerTableSource("sensorsSource", sensorsSource);

        Table allSensors = tEnv.scan("sensorsSource").select("*");

        DataStream stream0 = tEnv.toAppendStream(allSensors, SensorsTupleType.tupleTypeInfo);

        DataStream streamTS = null;
        if(withAssignTimestampsAndWatermarks) {
            streamTS = stream0.assignTimestampsAndWatermarks(new TimestampExtractor());
        }

        return (withAssignTimestampsAndWatermarks ? streamTS : stream0);
    }


    /**
     * To use in case that for some reason using the Flink Table API is completely prohibited.
     * @param inputFilePath
     * @return
     */
    public DataStream buildNoTableAPI(String inputFilePath) {
        TupleTypeInfo tupleTypeInfo = SensorsTupleType.tupleTypeInfo;
        TupleCsvInputFormat tupleCsvInputFormat = new TupleCsvInputFormat(new Path(inputFilePath),"\n",",", tupleTypeInfo);
        DataStream stream = env.createInput(tupleCsvInputFormat, tupleTypeInfo);
        return stream;
    }

}
