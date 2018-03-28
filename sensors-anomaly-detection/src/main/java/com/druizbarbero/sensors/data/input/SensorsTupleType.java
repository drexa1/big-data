package com.druizbarbero.sensors.data.input;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import java.sql.Timestamp;

/**
 * Encapsulates the columns format of the input CSV so the main job class does not look a mess.
 * In an ideal world there is SCHEMA-REGISTRY
 */

public class SensorsTupleType {

    public static BasicTypeInfo[] types = {
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO
    };

    public static TupleTypeInfo<Tuple11<String,String,String,String,String,String,String,String,String,String,String>> tupleTypeInfo = new TupleTypeInfo(SensorsTupleType.types);
}
