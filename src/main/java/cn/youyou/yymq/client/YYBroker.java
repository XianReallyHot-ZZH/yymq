package cn.youyou.yymq.client;

import cn.youyou.yymq.core.YYMq;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MQ服务端
 * 1、维护所有的MQ队列
 * 2、负责与发送端和接收端的交互
 */
public class YYBroker {

    // 队列映射,key为topic，value为对应的mq队列
    Map<String, YYMq> mqMapping = new ConcurrentHashMap<>(64);

    // 根据topic查找mq队列
    public YYMq find(String topic) {
        return mqMapping.get(topic);
    }

    // 创建topic
    public YYMq createTopic(String topic) {
        return mqMapping.computeIfAbsent(topic, k -> new YYMq(topic));
    }

    // 创建producer
    public YYProducer createProducer() {
        return new YYProducer(this);
    }

    // 创建consumer
    public YYConsumer createConsumer(String topic) {
        YYConsumer yyConsumer = new YYConsumer(this);
        yyConsumer.subscribe(topic);
        return yyConsumer;
    }


}
