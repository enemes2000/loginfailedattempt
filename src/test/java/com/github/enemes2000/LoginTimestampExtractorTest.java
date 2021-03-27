package com.github.enemes2000;

import com.github.enemes2000.topology.LoginTimestampExtractor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;

@Profile("Test")
@RunWith(SpringRunner.class)
@SpringBootTest
public class LoginTimestampExtractorTest {

    @Test
    public void testTimestampExtraction() {
       LoginTimestampExtractor loginTimestampExtractor = new LoginTimestampExtractor();

       Login login = Login.newBuilder().setUsername("jean").setLoginattempt(1).setIpadress("192.13.12.15").setTimestamp("2021-03-17T09:02:00.000Z").build();

        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("login", 0, 1, "mike", login);

        long timestamp = loginTimestampExtractor.extract(record, 0);

        assertEquals(1615971720000L, timestamp);
    }
}
