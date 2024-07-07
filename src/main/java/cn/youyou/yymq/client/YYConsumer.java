package cn.youyou.yymq.client;

import cn.youyou.yymq.common.Message;
import cn.youyou.yymq.core.YYMq;

/**
 * 消费者，指定消费哪个topic
 */
public class YYConsumer {

    YYBroker broker;

    String topic;

    public YYConsumer(YYBroker broker) {
        this.broker = broker;
    }

    public void subscribe(String topic) {
        this.topic = topic;
        YYMq mq = broker.find(topic);
        if(mq == null) throw new RuntimeException("topic not found");
    }

    public Message poll(long timeout) {
        YYMq mq = broker.find(topic);
        if(mq == null) throw new RuntimeException("topic not found");
        return mq.poll(timeout);
    }

    public void listen(YYListener listener) {
        YYMq mq = broker.find(topic);
        if(mq == null) throw new RuntimeException("topic not found");
        mq.addListener(listener);
    }


}
