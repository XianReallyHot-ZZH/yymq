package cn.youyou.yymq.model;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * used by server
 * 消息队列的订阅关系
 */
@Data
@AllArgsConstructor
public class Subscription {

    // 消息队列的topic
    private String topic;
    // 消费者id
    private String consumerId;
    // 消费者消费的offset
    private int offset = -1;

}