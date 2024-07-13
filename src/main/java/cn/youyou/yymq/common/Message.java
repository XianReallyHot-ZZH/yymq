package cn.youyou.yymq.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MQ queue 中消息的抽象（泛型）
 *
 * @param <T>
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Message<T> {

    // 消息偏移量key, 偏移量代表了消息在队列中的"位置", 在ack消息的时候其实是通过在subscription中维护相应的offset实现的
    public static final String HEADER_KEY_OFFSET = "X-offset";

    // 一款简单的id生成器
    static AtomicLong idGen = new AtomicLong(0);

    // 唯一id
    private Long id;
    // 消息体
    private T body;
    // 系统类属性信息，框架自身的机制依赖的一些信息，比如offset，mq的版本信息啥的
    private Map<String, String> headers = new HashMap<>();
    // 业务属性信息
//    private Map<String, Object> properties;

    /**
     * Message<String> creator
     *
     * @param body
     * @param headers
     * @return
     */
    public static Message<String> create(String body, Map<String, String> headers) {
        return new Message<>(idGen.incrementAndGet(), body, headers);
    }

}
