package cn.youyou.yymq.core;

/**
 * 生产者
 */
public class YYProducer {

    YYBroker broker;

    public YYProducer(YYBroker broker) {
        this.broker = broker;
    }

    /**
     * 给指定的topic发送消息
     *
     * @param topic
     * @param message
     * @return
     */
    public boolean send(String topic, YYMessage message) {
        YYMq mq = broker.find(topic);
        if (mq == null) {
            throw new RuntimeException("topic not found");
        }
        return mq.send(message);
    }

}
