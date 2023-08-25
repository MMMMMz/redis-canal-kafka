package com.meituan.rediscanalkafka.mq;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
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
public class KafkaListener {
    @org.springframework.kafka.annotation.KafkaListener(topics = "redis-topic")
    public void listen (ConsumerRecord consumer){
        System.out.println("topic名称:" + consumer.topic() + ",key:" +
                consumer.key() + "," +
                "分区位置:" + consumer.partition()
                + ", 下标" + consumer.offset() + "," + consumer.value());
        String json = (String) consumer.value();
        JSONObject jsonObject = JSONObject.parseObject(json);
        String type = jsonObject.getString("type");
        String pkNames = jsonObject.getJSONArray("pkNames").getString(0);
        JSONArray data = jsonObject.getJSONArray("data");
        for (int i = 0; i < data.size(); i++) {
            JSONObject dataObject = data.getJSONObject(i);
            String key = dataObject.getString(pkNames);
            switch (type) {
                case "UPDATE":
                    System.out.println(dataObject.toJSONString());
                    break;
                case "INSERT":
                    System.out.println(dataObject.toJSONString());
                    break;
                case "DELETE":
                    System.out.println(key);
                    break;
            }
        }
    }
}
