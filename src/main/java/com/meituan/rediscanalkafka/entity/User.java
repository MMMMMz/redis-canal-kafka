package com.meituan.rediscanalkafka.entity;

import lombok.Data;

/**
 * @author mazhe
 * @date 2023/8/24 20:01
 */
@Data
public class User {
    int id;

    String userId;

    String userName;

    String sex;

    String address;
}
