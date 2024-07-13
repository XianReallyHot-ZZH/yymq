package cn.youyou.yymq.client;

import cn.kimmking.utils.HttpUtils;
import cn.youyou.yymq.common.Message;
import cn.youyou.yymq.common.Result;
import cn.youyou.yymq.common.Stat;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.awt.event.WindowFocusListener;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * MQ client broker
 * 职责：
 *      1.负责和server端的交互
 *      2.client创建consumer和producer的起点
 */
@Slf4j
public class YYBroker {

    // 单例
    @Getter
    public static YYBroker defaultBroker = new YYBroker();

    // 先写死
    public static String bokerUrl = "http://localhost:8765/yymq";

    public static ScheduledExecutorService executor;

    static {
        init();
    }

    // key: topic, value: 订阅了该topic的消费者（监听器）
    @Getter
    private MultiValueMap<String, YYConsumer<?>> consumers = new LinkedMultiValueMap<>();

    public static void init() {
        executor = Executors.newScheduledThreadPool(1);
        // 模拟监听server端的数据变化，模拟由server推送数据的方式，本地被动接受数据
        executor.scheduleAtFixedRate(() -> {
            getDefaultBroker().getConsumers().forEach((topic, consumers) -> {
                consumers.forEach(consumer -> {
                    // 拉取消息
                    Message<?> message = consumer.poll(topic);
                    if (message == null) {
                        return;
                    }
                    try {
                        // 回调
                        consumer.getListener().onMessage(message);
                        // ack确认
                        consumer.ack(topic, message);
                    } catch (Exception e) {
                        // TODO: handle exception
                    }
                });
            });
        }, 100, 100, TimeUnit.MILLISECONDS);
    }

    /**
     * 创建生产者
     *
     * @param topic
     * @return
     */
    public YYProducer createProducer(String topic) {
        return new YYProducer(this);
    }

    /**
     * 创建消费者
     *
     * @param topic
     * @return
     */
    public YYConsumer createConsumer(String topic) {
        YYConsumer consumer = new YYConsumer(this);
        consumer.sub(topic);
        return consumer;
    }


    public boolean send(String topic, Message message) {
        log.info(">>> [YYBroker] send topic:{}, message: {}", topic, message);
        Result<String> result = HttpUtils.httpPost(
                JSON.toJSONString(message),
                bokerUrl + "/send?t=" + topic,
                new TypeReference<Result<String>>(){});
        log.info(">>> [YYBroker] send result: {}", result);
        return result.getCode() == 1;
    }

    public void sub(String topic, String consumerId) {
        log.info(">>> [YYBroker] sub topic:{}, consumerId: {}", topic, consumerId);
        Result<String> result = HttpUtils.httpGet(bokerUrl + "/sub?t=" + topic + "&cid=" + consumerId,
                new TypeReference<Result<String>>() {});
        log.info(">>> [YYBroker] sub result: {}", result);
    }

    public void unsub(String topic, String consumerId) {
        log.info(">>> [YYBroker] unsub topic:{}, consumerId: {}", topic, consumerId);
        Result<String> result = HttpUtils.httpGet(bokerUrl + "/unsub?t=" + topic + "&cid=" + consumerId,
                new TypeReference<Result<String>>() {});
        log.info(">>> [YYBroker] unsub result: {}", result);
    }

    public <T> Message<T> poll(String topic, String consumerId) {
        log.info(">>> [YYBroker] poll topic:{}, consumerId: {}", topic, consumerId);
        Result<Message<String>> result = HttpUtils.httpGet(bokerUrl + "/poll?t=" + topic + "&cid=" + consumerId,
                new TypeReference<Result<Message<String>>>() {});
        log.info(">>> [YYBroker] poll result: {}", result);
        return (Message<T>) result.getData();
    }

    public boolean ack(String topic, String consumerId, int offset) {
        log.info(">>> [YYBroker] ack topic:{}, consumerId: {}, offset: {}", topic, consumerId, offset);
        Result<String> result = HttpUtils.httpGet(bokerUrl + "/ack?t=" + topic + "&cid=" + consumerId + "&offset=" + offset,
                new TypeReference<Result<String>>() {});
        log.info(">>> [YYBroker] ack result: {}", result);
        return result.getCode() == 1;
    }

    public void addConsumer(String topic, YYConsumer yyConsumer) {
        consumers.add(topic, yyConsumer);
    }

    public Stat stat(String topic, String consumerId) {
        log.info(">>> [YYBroker] stat topic:{}, consumerId: {}", topic, consumerId);
        Result<Stat> result = HttpUtils.httpGet(bokerUrl + "/stat?t=" + topic + "&cid=" + consumerId,
                new TypeReference<Result<Stat>>() {});
        log.info(">>> [YYBroker] stat result: {}", result);
        return result.getData();
    }
}
