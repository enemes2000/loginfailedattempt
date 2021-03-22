package com.github.enemes2000;

import com.github.enemes2000.model.LoginFailCount;
import com.github.enemes2000.topology.LoginAttemptTumblingConfig;
import com.github.enemes2000.topology.LoginTimestampExtractor;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.junit.Assert.assertEquals;

@TestPropertySource(locations="classpath:test.properties")
@RunWith(SpringRunner.class)
@SpringBootTest
public class LoginAttemptTumblingWindowTest {

    @Autowired
    private Environment env;

    private TopologyTestDriver testDriver;

    @Autowired
    private LoginAttemptTumblingConfig loginAttemptTumblingConfig;


    private SpecificAvroSerializer<Login> loginSpecificAvroSerializer(Properties props) {
        SpecificAvroSerializer<Login> serializer = new SpecificAvroSerializer<>();

        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", props.getProperty("schema.registry.url"));
        serializer.configure(config, false);

        return serializer;
    }

    private List<LoginFailCountForTest> readOutputTopic(TopologyTestDriver testDriver,
                                        String outputTopic,
                                        Deserializer<String> keyDeserializer,
                                        Deserializer<String> valueDeserializer) {
        return testDriver
                .createOutputTopic(outputTopic, keyDeserializer, valueDeserializer)
                .readKeyValuesToList()
                .stream()
                .filter(Objects::nonNull)
                .map(record -> new LoginFailCountForTest(record.key, record.value))
                .collect(Collectors.toList());
    }

    @Test
    public void testWindows() throws IOException {
        String host = env.getProperty("server.host");
        int port = Integer.parseInt(env.getProperty("server.port"));
        String stateDir = env.getProperty("app.stateDir");
        String endpoint = String.format("%s:%s", host, port);
        Properties streamProps = getConfig(endpoint, stateDir);



        String inputTopic = env.getProperty("login.topic.name");
        String outputTopic = env.getProperty("failattempt.count.topic.name");

        Topology topology = loginAttemptTumblingConfig.buildTopology();
        testDriver = new TopologyTestDriver(topology, streamProps);

        Serializer<String> stringSerializer = Serdes.String().serializer();
        SpecificAvroSerializer<Login> loginSerializer = loginSpecificAvroSerializer(streamProps);
        Deserializer<String> stringDeserializer = Serdes.String().deserializer();

        List<Login> logins = new ArrayList<>();

        Instant fromTime = Instant.parse("2021-03-17T09:02:00.000Z");
        Instant toTime = Instant.parse("2021-03-17T09:02:02.500Z");
        logins.add(Login.newBuilder().setUsername("jean").setLoginattempt(1).setIpadress("192.13.12.15").setTimestamp("2021-03-17T09:02:00.000Z").build());
        logins.add(Login.newBuilder().setUsername("jean").setLoginattempt(1).setIpadress("192.13.12.15").setTimestamp("2021-03-17T09:02:00.500Z").build());
        logins.add(Login.newBuilder().setUsername("jean").setLoginattempt(1).setIpadress("192.13.12.15").setTimestamp("2021-03-17T09:02:01.000Z").build());
        logins.add(Login.newBuilder().setUsername("thomas").setLoginattempt(1).setIpadress("192.13.14.15").setTimestamp("2021-03-17T09:02:00.000Z").build());
        logins.add(Login.newBuilder().setUsername("thomas").setLoginattempt(1).setIpadress("192.13.14.15").setTimestamp("2021-03-17T09:02:01.000Z").build());

        List<LoginFailCount> loginFailCountList = new ArrayList<>();
        loginFailCountList.add(new LoginFailCount("jean", 3, "192.13.12.15"));
        loginFailCountList.add(new LoginFailCount("thomas", 2, "192.13.14.15"));


        final TestInputTopic<String, Login>
                testDriverInputTopic =
                testDriver.createInputTopic(inputTopic, stringSerializer, loginSerializer);

        for (Login login : logins) {
            testDriverInputTopic.pipeInput(login.getUsername(), login);
        }

        List<LoginFailCountForTest> actualOutput = readOutputTopic(testDriver,
                outputTopic,
                stringDeserializer,
                stringDeserializer);

        assertEquals(loginFailCountList.size(), actualOutput.size());
        for(int n = 0; n < loginFailCountList.size(); n++) {
            assertEquals(loginFailCountList.get(n).toString(), actualOutput.get(n).toString());
        }
    }



    private Properties getConfig(String endpoint, String stateDir) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, env.getProperty("application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(SCHEMA_REGISTRY_URL_CONFIG, env.getProperty("schema.registry.url"));
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, LoginTimestampExtractor.class.getName());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, endpoint);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);

        return props;
    }

    @After
    public void cleanup() {
        testDriver.close();
    }
}

class LoginFailCountForTest {

    private final String key;
    private final String value;

    public LoginFailCountForTest(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String toString() {
        return key + "=" + value;
    }
}
