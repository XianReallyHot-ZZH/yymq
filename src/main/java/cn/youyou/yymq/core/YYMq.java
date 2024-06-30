package cn.youyou.yymq.core;

import lombok.Data;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * 消息队列
 */
@Data
public class YYMq {

    //该消息队列的topic
    private String topic;

    // 消息队列载体
    private LinkedBlockingQueue<YYMessage> queue = new LinkedBlockingQueue<>();

    // 消息队列监听器集合
    private List<YYListener> listeners = new ArrayList<>();

    public YYMq(String topic) {
        this.topic = topic;
    }

    /**
     * MQ队列 供外部调用的方法：外部调用该方法给MQ发送消息
     * @param message
     * @return
     */
    public <T> boolean send(YYMessage<T> message) {
        boolean offered = queue.offer(message);
        // 接收到消息，回调监听器的消息接受监听方法
        listeners.forEach(listener -> listener.onMessage(message));
        return offered;
    }

    /**
     * MQ队列 供外部调用的方法：外部调用该方法可以从mq中获取消息
     * @param timeout
     * @return
     * @param <T>
     */
    @SneakyThrows
    public <T> YYMessage<T> poll(long timeout) {
        return queue.poll(timeout, MILLISECONDS);
    }

    /**
     * 给MQ添加监听器
     * @param listener
     * @param <T>
     */
    public <T> void addListener(YYListener<T> listener) {
        listeners.add(listener);
    }


}
