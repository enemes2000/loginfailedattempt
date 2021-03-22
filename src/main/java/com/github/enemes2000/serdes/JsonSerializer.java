package com.github.enemes2000.serdes;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class JsonSerializer<T> implements Serializer<T> {

    private Gson gson = new Gson();

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) return  null;
        return gson.toJson(data).getBytes(StandardCharsets.UTF_8);
    }
}
