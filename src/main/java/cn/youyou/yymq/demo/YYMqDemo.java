package cn.youyou.yymq.demo;

import cn.youyou.yymq.client.YYBroker;
import cn.youyou.yymq.client.YYConsumer;
import cn.youyou.yymq.common.Message;
import cn.youyou.yymq.client.YYProducer;
import lombok.SneakyThrows;

public class YYMqDemo {
    @SneakyThrows
    public static void main(String[] args) {

        long ids = 0;

        String topic = "yy.order";
        YYBroker broker = new YYBroker();
        broker.createTopic(topic);

        YYProducer producer = broker.createProducer();
        YYConsumer consumer = broker.createConsumer(topic);
        consumer.subscribe(topic);
        consumer.listen(message -> {
            System.out.println(" onMessage => " + message);
        });


        for (int i = 0; i < 10; i++) {
            Order order = new Order(ids, "item" + ids, 100 * ids);
            producer.send(topic, new Message<>((long) ids ++, order, null));
        }

        for (int i = 0; i < 10; i++) {
            Message<Order> message = (Message<Order>) consumer.poll(1000);
            System.out.println(message);
        }

        while (true) {
            char c = (char) System.in.read();
            if (c == 'q' || c == 'e') {
                break;
            }
            if (c == 'p') {
                Order order = new Order(ids, "item" + ids, 100 * ids);
                producer.send(topic, new Message<>(ids ++, order, null));
                System.out.println("send ok => " + order);
            }
            if (c == 'c') {
                Message<Order> message = (Message<Order>) consumer.poll(1000);
                System.out.println("poll ok => " + message);
            }
            if (c == 'a') {
                for (int i = 0; i < 10; i++) {
                    Order order = new Order(ids, "item" + ids, 100 * ids);
                    producer.send(topic, new Message<>((long) ids ++, order, null));
                }
                System.out.println("send 10 orders...");
            }
        }

    }
}
