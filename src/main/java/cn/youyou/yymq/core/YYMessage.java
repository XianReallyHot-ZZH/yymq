package cn.youyou.yymq.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * MQ中消息的抽象（泛型）
 *
 * @param <T>
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class YYMessage<T> {

    // 唯一id
    private Long id;
    // 消息体
    private T body;
    // 系统类属性信息，比如mq的版本信息啥的
    private Map<String, Object> headers;
    // 业务属性信息，比如用于mq框架自身要使用到的信息
//    private Map<String, Object> properties;

}
