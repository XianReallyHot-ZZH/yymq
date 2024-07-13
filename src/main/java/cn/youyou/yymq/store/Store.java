package cn.youyou.yymq.store;

import cn.youyou.yymq.common.Message;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;


/**
 * 消息队列持久化
 * 职责：
 *  将对应消息队列（topic）的消息进行磁盘持久化
 *  提供常见的持久化操作：写、读、恢复等操作
 *
 */
@Slf4j
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
    @SneakyThrows
    public void init() {
        // 创建topic对应的持久化文件
        File file = new File(topic + ".dat");
        if (!file.exists()) {
            file.createNewFile();
        }

        // 创建内存映射buffer
        Path path = Paths.get(file.getAbsolutePath());
        FileChannel fileChannel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, LEN); // 设置初始position和容量

        // 持久化数据恢复
        recovery();
    }

    /**
     * 持久化数据恢复
     * 1、恢复内存映射文件的最新写入位点
     * 2、恢复持久化文本检索器的检索信息
     * todo 3、如果总数据 > 10M，使用多个数据文件的list来管理持久化数据
     *
     * 恢复逻辑：
     * 读前10位，转成int=len，看是不是大于0，往后翻len的长度，就是下一条记录，
     * 重复上一步，一直到0为止，找到数据结尾
     */
    private void recovery() {
        ByteBuffer readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();
        byte[] header = new byte[MessageProtocol.MSG_LEN_META_LEN];
        readOnlyBuffer.get(header);
        int pos = 0;
        while (header[9] > 0) {
            String trim = new String(header, StandardCharsets.UTF_8).trim();
            log.info(">>> [Store] recovering, header: {}", trim);
            int len = Integer.parseInt(trim) + MessageProtocol.MSG_LEN_META_LEN;
            Indexer.addEntry(topic, pos, len);
            pos += len;
            log.info(">>> [Store] recovering, next position: {}", pos);
            readOnlyBuffer.position(pos);
            readOnlyBuffer.get(header);
        }
        readOnlyBuffer = null;
        log.info(">>> [Store] recovery done, init position: {}", pos);
        mappedByteBuffer.position(pos);
    }

    /**
     * 返回当前写入磁盘文件时的position
     *
     * @return
     */
    public int position() {
        return mappedByteBuffer.position();
    }

    /**
     * 将消息持久化，写入磁盘文件，返回写入磁盘文件当时的position
     *
     * @param message
     * @return
     */
    public int write(Message<String> message) {
        int position = position();
        String msg = MessageProtocol.encode(message);
        log.info(">>> [Store] write msg: {}", msg);
        int length = msg.getBytes(StandardCharsets.UTF_8).length;
        Indexer.addEntry(topic, position, length);
        mappedByteBuffer.put(Charset.forName("UTF-8").encode(msg));
        return position;
    }

    /**
     * 从指定位置读取一个message对象
     *
     * @param position
     * @return
     */
    public Message<String> read(int position) {
        ByteBuffer readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();
        Indexer.Entry entry = Indexer.getEntry(topic, position);
        readOnlyBuffer.position(entry.getPosition());
        byte[] bytes = new byte[entry.getLength()];
        readOnlyBuffer.get(bytes, 0, entry.getLength());
        return MessageProtocol.decode(bytes);
    }

    public int total() {
        return Indexer.getEntries(topic).size();
    }


    /**
     * 编码协议定义如下：
     * | 消息长度 | 消息内容 |
     * | 固定10字节 | 变长的n字节 |
     */
    private static class MessageProtocol {

        private static final int MSG_LEN_META_LEN = 10;
        /**
         * 将Message编码成特定格式的字符串
         *
         * @param message
         * @return
         */
        public static String encode(Message<String> message) {
            String msg = JSON.toJSONString(message);
            // 1000_1000_10
            int len = msg.getBytes(StandardCharsets.UTF_8).length;
            String msgLenInfo = String.format("%010d", len);
            return msgLenInfo + msg;
        }

        /**
         * 从符合编码协议的文本中解析出消息内容Message
         *
         * @param bytes
         * @return
         */
        public static Message<String> decode(byte[] bytes) {
            String json = new String(bytes, StandardCharsets.UTF_8);
            return decode(json);
        }

        public static Message<String> decode(String msg) {
            log.info(">>> [Store] read msg: {}", msg);
            if (msg == null || msg.length() < 1) {
                return null;
            }
            String json = msg.substring(MSG_LEN_META_LEN);
            return JSON.parseObject(json, new TypeReference<Message<String>>() {});
        }


    }

}
