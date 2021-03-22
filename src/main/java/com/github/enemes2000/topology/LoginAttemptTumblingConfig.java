package com.github.enemes2000.topology;

import com.github.enemes2000.Login;
import com.github.enemes2000.model.LoginFailCount;
import com.github.enemes2000.serdes.JsonSerdes;
import com.github.enemes2000.service.LoginAttemptService;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
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
import org.springframework.core.env.Environment;


import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

@Configuration
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

    @Bean
    public Topology buildTopology(){
        final StreamsBuilder builder = new StreamsBuilder();
        final String loginTopic = env.getProperty("login.topic.name");
        final String failAttemptCountTopic = env.getProperty("failattempt.count.topic.name");

        /*
         This is just for testing purpose
         */
        try{
            createTopics();
        }catch(ExecutionException|InterruptedException ex){
                throw new RuntimeException(ex);
        }

        LOGGER.info("Starting the building of the topology");
        KStream<String, Login> loginStream = builder.<String, Login>stream(loginTopic)
                .map((key, login) -> new KeyValue<>(login.getUsername().toString(), login));


        KStream<String, LoginFailCount>  failCountKStream= loginStream.mapValues(v -> new LoginFailCount(
                v.getUsername().toString(),
                v.getLoginattempt(),
                v.getIpadress().toString()) );

        //failCountKStream.print(Printed.<String, LoginFailCount>toSysOut().withLabel("FailcountKStream"));

        KGroupedStream<String, Login> loginKGroupedStream =  loginStream.groupByKey();

        /*
        * Tumbling Window
         */
        TimeWindowedKStream<String, Login> loginTimeWindowedKStream = loginKGroupedStream
                .windowedBy(TimeWindows.of(Duration.ofSeconds(10))
                .grace(Duration.ofSeconds(5)));

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
                (x,y) -> Login
                        .newBuilder()
                        .setUsername(y.getUsername())
                        .setLoginattempt(x.getLoginattempt() + y.getLoginattempt())
                        .setIpadress(y.getIpadress())
                        .setTimestamp(y.getTimestamp())
                        .build()
                ,Materialized.<String, Login, WindowStore<Bytes, byte[]>>
                        as(STORE_NAME)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(JsonSerdes.Login()));

        loginFailCountKTable.toStream()
                .map((key,count) -> new KeyValue<>(key.key(), count.toString()))
                .to(failAttemptCountTopic, Produced.with(Serdes.String(), Serdes.String()));

        LOGGER.info("Ending building of the topology");
        return builder.build();
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
    public KafkaStreams KafkaStreams(){

        String host = env.getProperty("server.host");
        int port = Integer.parseInt(env.getProperty("server.port"));
        String stateDir = env.getProperty("app.stateDir");

        String endpoint = String.format("%s:%s", host, port);

        Properties properties = getConfig(endpoint, stateDir);

        final  KafkaStreams kafkaStreams = new KafkaStreams(buildTopology(), properties);

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
