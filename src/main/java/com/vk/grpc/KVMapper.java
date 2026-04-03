package com.vk.grpc;

import com.google.protobuf.ByteString;

import java.util.List;

public class KVMapper {

    public static List<Object> toTuple(String key, ByteString value) {
        byte[] valueBytes = value.isEmpty() ? null : value.toByteArray();
        return List.of(key, valueBytes);
    }

    public static ByteString toByteString(byte[] value) {
        return value == null ? ByteString.EMPTY : ByteString.copyFrom(value);
    }

    @SuppressWarnings("unchecked")
    public static KVRecord fromTuple(List<?> tuple) {
        if (tuple == null || tuple.size() < 2) return null;

        String key = (String) tuple.get(0);
        byte[] value = (byte[]) tuple.get(1);
        return new KVRecord(key, value);
    }
}