package cn.youyou.yymq.client;

import cn.youyou.yymq.common.Message;
import cn.youyou.yymq.common.Stat;
import lombok.Data;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 消费者，指定消费哪个topic
 */
@Data
public class YYConsumer<T> {

    private String consumerId;

    YYBroker broker;

    private YYListener listener;

    static AtomicInteger idGen = new AtomicInteger(0);

    public YYConsumer(YYBroker broker) {
        this.broker = broker;
        this.consumerId = "CID-" + idGen.getAndIncrement();
    }

    // 添加topic的订阅关系
    public void sub(String topic) {
        broker.sub(topic, consumerId);
    }

    // 解除topic的订阅关系
    public void unsub(String topic) {
        broker.unsub(topic, consumerId);
    }

    // 主动拉取消息
    public Message<T> poll(String topic) {
        return broker.poll(topic, consumerId);
    }

    // offset ack
    public boolean ack(String topic, int offset) {
        return broker.ack(topic, consumerId, offset);
    }

    // 消息消费成功后，主动ack
    public boolean ack(String topic, Message<?> message) {
        int offset = Integer.parseInt(message.getHeaders().get(Message.HEADER_KEY_OFFSET));
        return broker.ack(topic, consumerId, offset);
    }

    /**
     * 添加监听器，监听topic的变化，自动触发监听器的回调
     *
     * @param topic
     * @param listener
     */
    public void listen(String topic, YYListener<T> listener) {
        this.listener = listener;
        // 其实是把监听器挂载到broker，由broker来触发监听器的回调
        broker.addConsumer(topic, this);
    }

    /**
     * 获取topic队列状态
     *
     * @param topic
     * @return
     */
    public Stat stat(String topic) {
        return broker.stat(topic, consumerId);
    }

}
