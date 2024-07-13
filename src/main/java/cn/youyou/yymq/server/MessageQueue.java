package cn.youyou.yymq.server;

import cn.youyou.yymq.common.Message;
import cn.youyou.yymq.model.Subscription;
import cn.youyou.yymq.store.Indexer;
import cn.youyou.yymq.store.Store;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 消息队列
 * 职责：
 *  1、topic和队列的映射关系：一个topic对应一个消息队列
 *  2、消息队列（topic）和消费者的订阅关系：一个消息队列支持对应多个消费者
 *  3、消息队列的持久化：消息队列持久化到磁盘文件中
 *  4、队列的常见操作：比如接受添加消息，消费消息，确认消费
 */
@Slf4j
public class MessageQueue {

    // server端的MQ信息（所有消息队列）， key：topic，value：消息队列
    public static final Map<String, MessageQueue> queues = new HashMap<>();

    static {
        queues.put("yymq.default", new MessageQueue("yymq.default"));
    }

    // 消息队列的topic
    private String topic;

    // 当前消息队列（topic）的消费者订阅关系, key：consumerId，value：订阅关系描述
    private Map<String, Subscription> subscriptions = new HashMap<>();

    private Store store;

    public MessageQueue(String topic) {
        this.topic = topic;
        this.store = new Store(topic);
        this.store.init();
    }

    /**
     * 创建topic
     *
     * @param topic
     * @return
     */
    public static String createTopic(String topic) {
        if (queues.containsKey(topic)) {
            return "created";
        }
        MessageQueue queue = new MessageQueue(topic);
        queues.put(topic, queue);
        return topic;
    }

    /**
     * 消费者订阅
     *
     * @param topic
     * @param consumerId
     */
    public static void sub(String topic, String consumerId) {
        log.info(">>> [MessageQueue] sub topic:{} consumerId:{}", topic, consumerId);
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) {
            throw new RuntimeException("topic not found");
        }
        messageQueue.subscribe(new Subscription(topic, consumerId, -1));
    }

    public void subscribe(Subscription subscription) {
        subscriptions.putIfAbsent(subscription.getConsumerId(), subscription);
    }

    /**
     * 消费者取消订阅
     *
     * @param topic
     * @param consumerId
     */
    public static void unsub(String topic, String consumerId) {
        log.info(">>> [MessageQueue] unsub topic:{} consumerId:{}", topic, consumerId);
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) {
            throw new RuntimeException("topic not found");
        }
        messageQueue.unsubscribe(consumerId);
    }

    public void unsubscribe(String consumerId) {
        subscriptions.remove(consumerId);
    }

    /**
     * 接受生产者发送过来的信息
     *
     * @param topic
     * @param message
     * @return
     */
    public static int send(String topic, Message<String> message) {
        log.info(">>> [MessageQueue] send topic:{} message:{}", topic, message);
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) {
            throw new RuntimeException("topic not found");
        }
        // 当前文件的写入位点，将作为消息的位点
        int position = messageQueue.store.position();
        message.getHeaders().put(Message.HEADER_KEY_OFFSET, position);
        // 完成消息写入
        messageQueue.store.write(message);
        return position;
    }

    /**
     * 消费者消费消息
     *
     * @param topic
     * @param consumerId
     * @return
     */
    public static Message<String> poll(String topic, String consumerId) {
        log.info(">>> [MessageQueue] poll topic:{} consumerId:{}", topic, consumerId);
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) {
            throw new RuntimeException("topic not found");
        }
        if (!messageQueue.subscriptions.containsKey(consumerId)) {
            throw new RuntimeException("subscriptions not found for consumerId:" + consumerId + " and topic:" + topic);
        }
        // 开始为指定的consumerId进行信息的拉取，拉取当前消费者的ack位点的下一个未ack位点
        int offset = messageQueue.subscriptions.get(consumerId).getOffset();
        int nextOffset = offset > -1 ? offset + Indexer.getEntry(topic, offset).getLength() : 0;
        Message<String> message = messageQueue.store.read(nextOffset);
        log.info(">>> [MessageQueue] poll topic:{}, consumerId:{}, offset:{}, message:{}", topic, consumerId, nextOffset, message);
        return message;
    }

    /**
     * 消费者确认消费
     *
     * @param topic
     * @param consumerId
     * @param offset
     * @return
     */
    public static int ack(String topic, String consumerId, int offset) {
        log.info(">>> [MessageQueue] ack topic:{} consumerId:{} offset:{}", topic, consumerId, offset);
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) {
            throw new RuntimeException("topic not found");
        }
        if (!messageQueue.subscriptions.containsKey(consumerId)) {
            throw new RuntimeException("subscriptions not found for consumerId:" + consumerId + " and topic:" + topic);
        }

        // 处理指定的consumerId的ack位点
        Subscription subscription = messageQueue.subscriptions.get(consumerId);
        // 这次要ack的位点大于之前确认好的位点
        if (offset > subscription.getOffset() && offset < Store.LEN) {
            subscription.setOffset(offset);
            log.info(">>> [MessageQueue] complete ack >>> topic:{} consumerId:{} offset:{}", topic, consumerId, offset);
            return offset;
        }
        return -1;
    }

    /**
     * 消费者批量拉取信息
     *
     * @param topic
     * @param consumerId
     * @param size
     * @return
     */
    public static List<Message<?>> poll(String topic, String consumerId, int size) {
        log.info(">>> [MessageQueue] poll topic:{} consumerId:{} size:{}", topic, consumerId, size);
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) {
            throw new RuntimeException("topic not found");
        }
        if (!messageQueue.subscriptions.containsKey(consumerId)) {
            throw new RuntimeException("subscriptions not found for consumerId:" + consumerId + " and topic:" + topic);
        }

        int offset = messageQueue.subscriptions.get(consumerId).getOffset();
        int nextOffset = offset > -1 ? offset + Indexer.getEntry(topic, offset).getLength() : 0;
        List<Message<?>> result = new ArrayList<>();
        Message<String> message = messageQueue.store.read(nextOffset);
        while (message != null) {
            result.add(message);
            if (result.size() >= size) {
                break;
            }
            message = messageQueue.store.read(nextOffset);
        }
        log.info(">>> [MessageQueue] poll topic:{}, consumerId:{}, size:{}, last message:{}", topic, consumerId, size, message);
        return result;
    }


}
