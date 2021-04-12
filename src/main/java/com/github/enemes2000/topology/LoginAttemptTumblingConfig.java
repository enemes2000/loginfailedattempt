package com.github.enemes2000.topology;

import com.github.enemes2000.Login;
import com.github.enemes2000.helper.ConfigLoaderHelper;
import com.github.enemes2000.model.LoginFailCount;
import com.github.enemes2000.serdes.JsonSerdes;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;


import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Configuration
@Profile({"dev | prod"})
public class LoginAttemptTumblingConfig {

    Logger LOGGER = LoggerFactory.getLogger(LoginAttemptTumblingConfig.class);

    @Autowired
    private Environment env;

    private static String STORE_NAME;

    public KafkaStreams getStreams() {
        return streams;
    }

    public void setStreams(KafkaStreams streams) {
        this.streams = streams;
    }

    private KafkaStreams streams;

    public Topology buildTopology(Properties properties){
        final StreamsBuilder builder = new StreamsBuilder();

        final String loginTopic = properties.getProperty("input.topic");

        final String failAttemptCountTopic = properties.getProperty("output.topic");

        LOGGER.info("Topology build");
        KStream<String, Login> loginStream = builder.<String, Login>stream(loginTopic)
                .map(LoginAttemptTumblingConfig::getLoginKeyValueKeyValueMapper);


        KStream<String, LoginFailCount>  failCountKStream= loginStream.mapValues(LoginAttemptTumblingConfig::getLoginFailCountValueMapper);

        //How to print a stream
        //failCountKStream.print(Printed.<String, LoginFailCount>toSysOut().withLabel("FailcountKStream"));

        KGroupedStream<String, Login> loginKGroupedStream =  loginStream.groupByKey();

        /*
        * Tumbling Window
         */
        TimeWindowedKStream<String, Login> loginTimeWindowedKStream = loginKGroupedStream
                .windowedBy(TimeWindows.of(Duration.ofSeconds(10))
                .grace(Duration.ofMillis(5)));


        if (STORE_NAME == null){
            setStoreName(properties.getProperty("store.name"));
        }

        /*
         * Examples of how to use other windows operations
         *
         * TimeWindows hoppingWindow =
         *     TimeWindows.of(Duration.ofSeconds(5)).advanceBy(Duration.ofSeconds(4));
         *
         * SessionWindows sessionWindow = SessionWindows.with(Duration.ofSeconds(5));
         *
         * JoinWindows joinWindow = JoinWindows.of(Duration.ofSeconds(5));
         *
         * SlidingWindows slidingWindow =
         *     SlidingWindows.withTimeDifferenceAndGrace(Duration.ofSeconds(5), Duration.ofSeconds(0));
         */

        KTable<Windowed<String>, Login> loginFailCountKTable = loginTimeWindowedKStream.reduce(
                LoginAttemptTumblingConfig::getLoginReducer
                , Materialized.<String, Login, WindowStore<Bytes, byte[]>>
                        as(STORE_NAME)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(JsonSerdes.Login()))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));


        loginFailCountKTable.toStream()
                .map(LoginAttemptTumblingConfig::getKeyValue)
                .to(failAttemptCountTopic, Produced.with(Serdes.String(), Serdes.String()));

        LOGGER.info("Ending building of the topology");
        return builder.build();
    }

    public static KeyValue<String, String> getKeyValue(Windowed<String> key, Login login){
        return KeyValue.pair(key.key(), login.toString());
    }

    public static Login getLoginReducer(Login x, Login y){
        return Login
                .newBuilder()
                .setUsername(y.getUsername())
                .setLoginattempt(x.getLoginattempt() + y.getLoginattempt())
                .setIpadress(y.getIpadress())
                .setTimestamp(y.getTimestamp())
                .build();
    }


    public static LoginFailCount getLoginFailCountValueMapper(Login login) {
        return new LoginFailCount(login.getUsername().toString(), login.getLoginattempt(), login.getIpadress().toString());
    }

    public static KeyValue<String,Login> getLoginKeyValueKeyValueMapper(String key, Login login) {
        return KeyValue.pair(login.getUsername().toString(), login);
    }


    public void createTopics() throws ExecutionException, InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", env.getProperty("bootstrap.servers"));
        AdminClient client = AdminClient.create(config);

        List<NewTopic> topics = new ArrayList<>();
        Map<String, String> topicConfigs = new HashMap<>();
        topicConfigs.put("retention.ms", Long.toString(Long.MAX_VALUE));

        ListTopicsResult listTopics = client.listTopics();
        Set<String> names = listTopics.names().get();
        if (!names.contains(env.getProperty("login.topic.name"))){
            NewTopic logins = new NewTopic(env.getProperty("login.topic.name"),
                    Integer.parseInt(env.getProperty("login.topic.partitions")),
                    Short.parseShort(env.getProperty("login.topic.replication.factor")));
            logins.configs(topicConfigs);
            topics.add(logins);
            client.createTopics(topics);
        }

        if (!names.contains(env.getProperty("failattempt.count.topic.name"))){
            NewTopic counts = new NewTopic(env.getProperty("failattempt.count.topic.name"),
                    Integer.parseInt(env.getProperty("failattempt.count.topic.partitions")),
                    Short.parseShort(env.getProperty("failattempt.count.topic.replication.factor")));
            counts.configs(topicConfigs);
            topics.add(counts);
            client.createTopics(topics);
        }

        client.close();
    }

    @Bean
    public KafkaStreams KafkaStreams() throws IOException, ExecutionException, InterruptedException {

        Properties props = ConfigLoaderHelper.getConfig(env);

        /*
         This just for testing purpose
         */
        createTopics();

        Topology topology = buildTopology(props);

        final  KafkaStreams kafkaStreams = new KafkaStreams(topology, props);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        setStreams(kafkaStreams);

        kafkaStreams.start();


        return kafkaStreams;
    }

    @Value("${store.name}")
     public void setStoreName(String storeName){
        LoginAttemptTumblingConfig.STORE_NAME = storeName;
    }

}
