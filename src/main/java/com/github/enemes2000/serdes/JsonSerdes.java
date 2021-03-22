package com.github.enemes2000.serdes;

import com.github.enemes2000.Login;
import com.github.enemes2000.model.LoginFailCount;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class JsonSerdes {

    public static Serde<LoginFailCount> LoginFailCount() {
        JsonSerializer<LoginFailCount> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<LoginFailCount> jsonDeserializer = new JsonDeserializer<>(LoginFailCount.class);
        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }

    public static Serde<Login> Login(){
        JsonSerializer<Login> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Login> jsonDeserializer = new JsonDeserializer<>(Login.class);
        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }
}
