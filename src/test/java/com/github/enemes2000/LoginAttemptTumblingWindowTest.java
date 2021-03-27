package com.github.enemes2000;

import com.github.enemes2000.helper.ConfigLoaderHelper;
import com.github.enemes2000.model.LoginFailCount;
import com.github.enemes2000.serdes.JsonSerdes;
import com.github.enemes2000.topology.LoginAttemptTumblingConfig;
import com.github.enemes2000.topology.LoginTimestampExtractor;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.junit.Before;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.test.context.junit4.SpringRunner;


import java.io.IOException;

import java.util.*;
import java.util.stream.Collectors;


import static org.junit.Assert.assertTrue;


@Profile("Test")
@RunWith(SpringRunner.class)
@SpringBootTest
public class LoginAttemptTumblingWindowTest {

    @Autowired
    private Environment env;

    private TopologyTestDriver testDriver;

    private  TestInputTopic<String, Login>  testDriverInputTopic;

    private  TestOutputTopic<String, Login>  testDriverOutTopic;

    private LoginAttemptTumblingConfig loginAttemptTumblingConfig;

    private List<Login> loginList;


    @BeforeEach
    public void setup(){

        loginAttemptTumblingConfig = new LoginAttemptTumblingConfig();

        String host = env.getProperty("server.host");
        String stateDir = env.getProperty("app.stateDir");

        Properties streamProps = ConfigLoaderHelper.getConfig(env);
        String inputTopic = env.getProperty("login.topic.name");
        String outputTopic = env.getProperty("failattempt.count.topic.name");


        Topology topology = loginAttemptTumblingConfig.buildTopology(streamProps);
        testDriver = new TopologyTestDriver(topology, streamProps);

        Serializer<String> stringSerializer = Serdes.String().serializer();
        SpecificAvroSerializer<Login> loginSerializer = loginSpecificAvroSerializer(streamProps);
        SpecificAvroDeserializer<Login> loginDeSerializer = loginSpecificAvroDeSerializer(streamProps);
        Deserializer<String> stringDeserializer = Serdes.String().deserializer();


        testDriverInputTopic = testDriver.createInputTopic(inputTopic, stringSerializer, loginSerializer);

        testDriverOutTopic = testDriver.createOutputTopic(outputTopic, stringDeserializer,JsonSerdes.Login().deserializer());
    }


    private SpecificAvroSerializer<Login> loginSpecificAvroSerializer(Properties props) {
        SpecificAvroSerializer<Login> serializer = new SpecificAvroSerializer<>();

        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", props.getProperty("schema.registry.url"));
        serializer.configure(config, false);

        return serializer;
    }

    private SpecificAvroDeserializer<Login> loginSpecificAvroDeSerializer(Properties props) {
        SpecificAvroDeserializer<Login> deserializer = new SpecificAvroDeserializer<>();

        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", props.getProperty("schema.registry.url"));
        deserializer.configure(config, false);

        return deserializer;
    }

    @Test
    public void testWindows() throws IOException {

        List<Login> logins = new ArrayList<>();

        logins.add(Login.newBuilder().setUsername("jean").setLoginattempt(1).setIpadress("192.13.12.15").setTimestamp("2021-03-17T09:02:00.000Z").build());
        logins.add(Login.newBuilder().setUsername("jean").setLoginattempt(1).setIpadress("192.13.12.15").setTimestamp("2021-03-17T09:02:00.500Z").build());
        logins.add(Login.newBuilder().setUsername("jean").setLoginattempt(1).setIpadress("192.13.12.15").setTimestamp("2021-03-17T09:02:01.000Z").build());
        logins.add(Login.newBuilder().setUsername("thomas").setLoginattempt(1).setIpadress("192.13.14.15").setTimestamp("2021-03-17T09:02:00.000Z").build());
        logins.add(Login.newBuilder().setUsername("thomas").setLoginattempt(1).setIpadress("192.13.14.15").setTimestamp("2021-03-17T09:02:01.000Z").build());

        List<LoginFailCount> loginFailCountList = new ArrayList<>();
        loginFailCountList.add(new LoginFailCount("jean", 3, "192.13.12.15"));
        loginFailCountList.add(new LoginFailCount("thomas", 2, "192.13.14.15"));

        //Push a dummy record to allow window to close
        logins.add(Login.newBuilder().setUsername("jean").setLoginattempt(1).setIpadress("").setTimestamp("2021-03-17T09:02:15.500Z").build());

        testDriverInputTopic.pipeValueList(logins);

        loginList = testDriverOutTopic
                .readValuesToList()
                .stream()
                .map(record -> Login.newBuilder()
                        .setUsername(record.getUsername())
                        .setIpadress(record.getIpadress())
                        .setLoginattempt(record.getLoginattempt())
                        .setTimestamp(record.getTimestamp())
                        .build())
                .collect(Collectors.toList());

         assertTrue(loginList.size() == loginFailCountList.size());

    }

    @AfterEach
    public void cleanup() {
        testDriver.close();
    }

}

