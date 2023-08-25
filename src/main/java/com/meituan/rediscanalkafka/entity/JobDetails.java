package com.meituan.rediscanalkafka.entity;

import lombok.Data;

import java.util.Date;

/**
 * @author mazhe
 * @date 2023/8/25 14:20
 */
@Data
public class JobDetails {
    private String cronExpression;
    private String jobClassName;
    private String triggerGroupName;
    private String triggerName;
    private String jobGroupName;
    private String jobName;
    private Date nextFireTime;
    private Date previousFireTime;
    private Date startTime;
    private String timeZone;
    private String status;
}
