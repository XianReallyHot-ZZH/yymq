package cn.youyou.yymq.demo;

import cn.youyou.yymq.client.YYBroker;
import cn.youyou.yymq.client.YYConsumer;
import cn.youyou.yymq.client.YYListener;
import cn.youyou.yymq.client.YYProducer;
import cn.youyou.yymq.common.Message;
import cn.youyou.yymq.common.Stat;
import com.alibaba.fastjson.JSON;
import lombok.SneakyThrows;

public class YYMqDemo {
    @SneakyThrows
    public static void main(String[] args) {

        long ids = 0;

        String topic = "yymq.default";
        YYBroker broker = YYBroker.getDefaultBroker();

        YYProducer producer = broker.createProducer(topic);
        YYConsumer consumer = broker.createConsumer(topic);

        // 模拟消息发送
        for (int i = 0; i < 10; i++) {
            Order order = new Order(ids, "item" + ids, 100 * ids);
            producer.send(topic, new Message<>((long) ids++, JSON.toJSONString(order), null));
        }

        // 模拟消息消费（主动拉取）
        for (int i = 0; i < 10; i++) {
            Message<String> message = (Message<String>) consumer.poll(topic);
            System.out.println(message); // 做业务处理。。。。
            consumer.ack(topic, message);
        }


        while (true) {
            char c = (char) System.in.read();
            if (c == 'q' || c == 'e') {
                // consumer1.unsub(topic);s
                System.out.println(" [exit] : " + c );
                break;
            }
            if (c == 'p') {
                Order order = new Order(ids, "item" + ids, 100 * ids);
                producer.send(topic, new Message<>(ids ++, JSON.toJSONString(order), null));
                System.out.println("produce ok => " + order);
            }
            if (c == 'c') {
                Message<String> message = (Message<String>) consumer.poll(topic);
                System.out.println("consume ok => " + message);
                consumer.ack(topic, message);
            }
            if (c == 's') {
                Stat stat = consumer.stat(topic);
                System.out.println(stat);
            }

            boolean hasL = false;
            if (c == 'l') {
                if (!hasL) {
                    consumer.listen(topic, new YYListener<String>() {
                        @Override
                        public void onMessage(Message<String> message) {
                            System.out.println(" [onMessage] 接受server推送: " + message);
                        }
                    });
                    hasL = true;
                } else {
                    System.out.println("has set listen");
                }
            }

            if (c == 'b') {
                for (int i = 0; i < 10; i++) {
                    Order order = new Order(ids, "item" + ids, 100 * ids);
                    producer.send(topic, new Message<>((long) ids ++, JSON.toJSONString(order), null));
                }
                System.out.println("batch produce 10 orders...");
            }
        }

        System.exit(1);

    }
}
