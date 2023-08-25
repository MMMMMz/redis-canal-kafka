package com.meituan.rediscanalkafka.mq;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * @author mazhe
 * @date 2023/8/25 10:04
 */
@Configuration

public class EventAutoConfiguration {
    @Bean
    public MyEventPublisher myEventPublisher(@Qualifier("kafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate) {
        return new MyEventPublisher(kafkaTemplate);
    }
}
