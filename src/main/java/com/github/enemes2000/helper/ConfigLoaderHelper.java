package com.github.enemes2000.helper;

import com.github.enemes2000.topology.LoginTimestampExtractor;
import io.confluent.common.utils.TestUtils;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class ConfigLoaderHelper {

    public static Properties loadEnvProperties(String fileName) throws IOException {
        Properties envProps = new Properties();
        FileInputStream input = new FileInputStream(fileName);
        envProps.load(input);
        input.close();

        return envProps;
    }

    public  static Properties getConfig(Properties properties) {
        Properties props = new Properties();

        String host = properties.getProperty("server.host");
        int port = Integer.parseInt(properties.getProperty("server.port"));

        String endpoint = String.format("%s:%s", host, port);

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, properties.getProperty("application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(SCHEMA_REGISTRY_URL_CONFIG, properties.getProperty("schema.registry.url"));
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, LoginTimestampExtractor.class.getName());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, endpoint);
        props.put(StreamsConfig.STATE_DIR_CONFIG, properties.getProperty("app.stateDir"));


        props.put("input.topic", properties.getProperty("login.topic.name"));
        props.put("output.topic", properties.get("failattempt.count.topic.name"));
        props.put("store.name", properties.getProperty("store.name"));

        return props;
    }

    public  static Properties getConfig(Environment env) {
        Properties props = new Properties();

        String host = env.getProperty("server.host");
        int port = Integer.parseInt(env.getProperty("server.port"));

        String endpoint = String.format("%s:%s", host, port);

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, env.getProperty("application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(SCHEMA_REGISTRY_URL_CONFIG, env.getProperty("schema.registry.url"));
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, LoginTimestampExtractor.class.getName());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,5);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, endpoint);
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());


        props.put("input.topic", env.getProperty("login.topic.name"));
        props.put("output.topic", env.getProperty("failattempt.count.topic.name"));
        props.put("store.name", env.getProperty("store.name"));

        return props;
    }
}
