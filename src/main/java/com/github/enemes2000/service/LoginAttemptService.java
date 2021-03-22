package com.github.enemes2000.service;

import com.github.enemes2000.Login;
import com.github.enemes2000.controller.LoginAttemptFailController;
import com.github.enemes2000.model.LoginFailCount;
import com.github.enemes2000.topology.LoginAttemptTumblingConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class LoginAttemptService {

    Logger LOGGER = LoggerFactory.getLogger(LoginAttemptService.class);

    @Autowired
    private LoginAttemptTumblingConfig config;

    @Autowired
    private Environment env;

    @Value("${store.name}")
    private String STORE_NAME;


    public List<LoginFailCount> getLoginFailCount(String username) {

        LOGGER.info("Get the  streams");
        KafkaStreams streams = config.getStreams();

        LOGGER.info("Get the  store");
        ReadOnlyWindowStore<String, Login> store = getStore(streams);

        KeyQueryMetadata metadata =
                streams.queryMetadataForKey(STORE_NAME, username, Serdes.String().serializer());

        HostInfo hostInfo = new HostInfo(env.getProperty("server.host"), Integer.parseInt(env.getProperty("server.port")));

        List<Login> loginFailCounts = new ArrayList<>();

        if (hostInfo.equals(metadata.getActiveHost())) {
            //This is just for testing purpose
            Instant fromTime = Instant.parse("2021-03-17T09:02:00.000Z");
            Instant toTime = Instant.parse("2021-03-17T09:02:02.500Z");

            LOGGER.info("Build the windowstore iterator for range from: %s - to: %s", fromTime, toTime);
            WindowStoreIterator<Login> range = store.fetch(username, fromTime, toTime);

            while (range.hasNext()){
                KeyValue<Long, Login> next = range.next();

                Long timestamp = next.key;

                Login login = next.value;

                if (login != null)
                    loginFailCounts.add(login);
            }
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



    ReadOnlyWindowStore<String, Login> getStore(KafkaStreams streams) {

        return streams.store(
                StoreQueryParameters.fromNameAndType(
                        STORE_NAME,
                        QueryableStoreTypes.windowStore()));
    }

    /*
    Build a Treeset of LoginFailCount form login objects that will be exposed to the controller
     */
    private List<LoginFailCount> failCounts(Collection<Login> logins){
        Set<LoginFailCount> failCounts = new TreeSet<>();
        for (Login login : logins){
            failCounts.add(new LoginFailCount(login.getUsername(), login.getLoginattempt(), login.getIpadress()));
        }

        return failCounts.stream().collect(Collectors.toList());
    }
}
