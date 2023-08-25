package com.meituan.rediscanalkafka.controller;

import com.meituan.rediscanalkafka.util.RedisUtil;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @author mazhe
 * @date 2023/8/25 14:31
 */
@RestController
@RequestMapping("redis")
public class RedisController {

    @Resource
    private  RedisUtil redisUtil;

    @GetMapping("save")
    public String save(String key, String value){
        redisUtil.set(key, value);
        return "success";
    }

    @GetMapping("get")
    public String get(String key){
        return (String) redisUtil.get(key);
    }

}
