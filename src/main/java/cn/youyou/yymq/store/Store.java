package cn.youyou.yymq.store;

import cn.youyou.yymq.common.Message;

import java.nio.MappedByteBuffer;

/**
 * 消息队列持久化
 * 职责：
 *  将对应消息队列（topic）的消息进行磁盘持久化
 *  提供常见的持久化操作：写、读、恢复等操作
 *
 */
public class Store {

    // 对应持久化单文件的长度，默认为10KB
    public static final int LEN = 1024 * 10;

    // 对应的消息队列
    private String topic;

    // 文件内存映射buffer，用这个工具来完成持久化操作
    private MappedByteBuffer mappedByteBuffer;

    public Store(String topic) {
        this.topic = topic;
    }

    /**
     * 初始化
     */
    public void init() {

    }

    /**
     * 将消息持久化，写入磁盘文件，返回写入磁盘文件当时的position
     *
     * @param message
     * @return
     */
    public int write(Message<String> message) {

    }

    /**
     * 从指定位置读取一个message对象
     *
     * @param position
     * @return
     */
    public Message<String> read(int position) {


    }

}
