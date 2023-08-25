package com.meituan.rediscanalkafka.mq;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * @author mazhe
 * @date 2023/8/25 10:02
 */
@Slf4j
public class MyEventPublisher {

    private KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${spring.kafka.consumer.group-id}")
    private String topic;

    public MyEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishEvent(Object event) {
        if (log.isInfoEnabled()) {
            log.info("topic发送:{}", event.getClass().getName());
        }
        kafkaTemplate.send("redis-topic", event);
    }

}