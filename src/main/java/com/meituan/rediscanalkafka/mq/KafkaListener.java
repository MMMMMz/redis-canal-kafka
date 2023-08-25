package com.meituan.rediscanalkafka.mq;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.stereotype.Component;

/**
 * @author mazhe
 * @date 2023/8/24 20:18
 */
@Configuration
@EnableKafka
@Slf4j
public class KafkaListener {
    @org.springframework.kafka.annotation.KafkaListener(topics = "redis-topic")
    public void listen (ConsumerRecord consumer){
        log.info("收到消息了！");
        log.info("topic名称:" + consumer.topic() + ",key:" +
                consumer.key() + "," +
                "分区位置:" + consumer.partition()
                + ", 下标" + consumer.offset() + "," + consumer.value());
        String json = (String) consumer.value();
        log.info("消息的json为： " + json);
//        JSONObject jsonObject = JSONObject.parseObject(json);
//        String type = jsonObject.getString("type");
//        String pkNames = jsonObject.getJSONArray("pkNames").getString(0);
//        JSONArray data = jsonObject.getJSONArray("data");
//        for (int i = 0; i < data.size(); i++) {
//            JSONObject dataObject = data.getJSONObject(i);
//            String key = dataObject.getString(pkNames);
//            switch (type) {
//                case "UPDATE":
//                    log.info("update " + dataObject.toJSONString());
//                    break;
//                case "INSERT":
//                    log.info("insert " + dataObject.toJSONString());
//                    break;
//                case "DELETE":
//                    log.info(dataObject.toJSONString());
//                    break;
//            }
//        }
    }
}
