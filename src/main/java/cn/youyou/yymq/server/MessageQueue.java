package cn.youyou.yymq.server;

import cn.youyou.yymq.common.Message;
import cn.youyou.yymq.model.Subscription;
import cn.youyou.yymq.store.Store;

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
public class MessageQueue {

    // server端的MQ信息（所有消息队列）， key：topic，value：消息队列
    public static final Map<String, MessageQueue> queues = new HashMap<>();

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
     * 消费者订阅
     *
     * @param topic
     * @param consumerId
     */
    public static void sub(String topic, String consumerId) {

    }

    /**
     * 消费者取消订阅
     *
     * @param topic
     * @param consumerId
     */
    public static void unsub(String topic, String consumerId) {

    }

    /**
     * 接受生产者发送过来的信息
     *
     * @param topic
     * @param message
     * @return
     */
    public static int send(String topic, Message<String> message) {

    }

    /**
     * 消费者消费消息
     *
     * @param topic
     * @param consumerId
     * @return
     */
    public static Message<String> poll(String topic, String consumerId) {

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

    }

}
