package com.github.enemes2000.service;

import com.github.enemes2000.Login;
import com.github.enemes2000.model.LoginFailCount;
import com.github.enemes2000.topology.LoginAttemptTumblingConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Profile({"dev | prod"})
public class LoginAttemptService {

    Logger LOGGER = LoggerFactory.getLogger(LoginAttemptService.class);

    @Autowired
    private LoginAttemptTumblingConfig config;

    @Autowired
    private Environment env;

    @Value("${store.name}")
    private String STORE_NAME;

    public List<LoginFailCount> getLoginFailCount(String username, String from, String to) {

        LOGGER.info("Get the  streams");
        KafkaStreams streams = config.getStreams();

        LOGGER.info("Get the  store");
        ReadOnlyWindowStore<String, Login> store = getQueryableStore(streams);

        KeyQueryMetadata metadata =
                streams.queryMetadataForKey(STORE_NAME, username, Serdes.String().serializer());

        HostInfo hostInfo = new HostInfo(env.getProperty("server.host"), Integer.parseInt(env.getProperty("server.port")));

        List<Login> loginFailCounts = new ArrayList<>();

        if (hostInfo.equals(metadata.getActiveHost())) {

            Instant fromTime = Instant.parse(from);
            Instant toTime = Instant.parse(to);

            LOGGER.info("Build the windowstore iterator for range from: {} - to: {}", fromTime, toTime);
            WindowStoreIterator<Login> range = store.fetch(username, fromTime, toTime);

            while (range.hasNext()){
                KeyValue<Long, Login> next = range.next();

                Long timestamp = next.key;

                Login login = next.value;

                if (login != null)
                    loginFailCounts.add(login);
            }
            range.close();
        }
        else {
            //If a remote instance has the key
            String remoteHost = metadata.getActiveHost().host();

            int remotePort = metadata.getActiveHost().port();

            //query from the remote host
            RestTemplate restTemplate = new RestTemplate();

            LoginFailCount ctx = restTemplate.postForObject(
                    String.format("http://%s:%d/%s", remoteHost,
                            remotePort, "/login/failattempt?username="+username),"",LoginFailCount.class);
        }
        return failCounts(loginFailCounts);
    }



    ReadOnlyWindowStore<String, Login> getQueryableStore(KafkaStreams streams) {
        return streams.store(
                StoreQueryParameters.fromNameAndType(
                        STORE_NAME, QueryableStoreTypes.windowStore()));
    }


    private List<LoginFailCount> failCounts(Collection<Login> logins){

       return logins.stream().map(x-> new LoginFailCount(x.getUsername(), x.getLoginattempt(), x.getIpadress()))
                       .collect(Collectors.toList());

    }

    public List<LoginFailCount> getAllFromRange(String from, String to){
        LOGGER.info("Get the  streams");
        KafkaStreams streams = config.getStreams();

        LOGGER.info("Get the  store");
        ReadOnlyWindowStore<String, Login> store = getQueryableStore(streams);

        Instant fromTime = Instant.parse(from);
        Instant toTime = Instant.parse(to);
        KeyValueIterator<Windowed<String>, Login> range = store.fetchAll(fromTime, toTime);
        List<Login> loginFailCounts = new ArrayList<>();
        while (range.hasNext()){
            KeyValue<Windowed<String>, Login> next = range.next();
            String key = next.key.key();
            Window window = next.key.window();
            Login login = next.value;
            if (login != null)
                loginFailCounts.add(login);
        }
        range.close();
        return failCounts(loginFailCounts);
    }
}
