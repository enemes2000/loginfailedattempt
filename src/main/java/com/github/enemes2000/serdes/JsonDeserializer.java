package com.github.enemes2000.serdes;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;

public class JsonDeserializer<T> implements Deserializer<T> {

    private final Gson gson = new Gson();
    private Class<T> destinationClass;
    private Type reflectionType;

    public JsonDeserializer(Class<T> destinationClass) {
        this.destinationClass = destinationClass;
    }

    public JsonDeserializer(Type reflectionType){
        this.reflectionType = reflectionType;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) return  null;
        Type type = destinationClass!= null ? destinationClass:reflectionType;
        return gson.fromJson(new String(data, StandardCharsets.UTF_8), type);
    }
}
