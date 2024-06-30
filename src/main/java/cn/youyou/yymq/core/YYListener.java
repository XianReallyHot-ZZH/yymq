package cn.youyou.yymq.core;

/**
 * MQ的监听器接口
 */
public interface YYListener<T> {

    /**
     * 监听接受到的消息
     * 入参为mq接受到的信息
     * @param message
     */
    void onMessage(YYMessage<T> message);

}
