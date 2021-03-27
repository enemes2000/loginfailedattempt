package com.github.enemes2000.topology;

import com.github.enemes2000.Login;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class LoginTimestampExtractor implements TimestampExtractor {

    Logger LOGGER = LoggerFactory.getLogger(LoginTimestampExtractor.class);


    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {

        String eventTime = ((Login)record.value()).getTimestamp().toString();

        LOGGER.info("parsing the EventTime {}", eventTime);
        return Instant.parse(eventTime).toEpochMilli();

    }
}
