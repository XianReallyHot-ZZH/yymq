package cn.youyou.yymq.store;

import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.HashMap;
import java.util.Map;

/**
 * 持久化文件检索器
 * 职责：
 *  记录各个队列的message持久化检索信息，用于从持久化文件读取消息时指导store怎么读取消息，即每个message都对应了一份检索信息
 */
public class Indexer {

    // server端所有的文件检索信息，key：topic，value：消息对应的检索信息
    static final MultiValueMap<String, Entry> indexes = new LinkedMultiValueMap<>();

    // key: topic，value：[key: position, value: Entry], 方便内部实现的时候快速定位到消息对应的检索信息
    static final Map<String, Map<Integer, Entry>> indexMaps = new HashMap<>();


    /**
     * 添加消息对应的检索信息
     *
     * @param topic
     * @param position
     * @param len
     */
    public static void addEntry(String topic, int position, int len) {

    }

    /**
     * 获取消息对应的检索信息
     *
     * @param topic
     * @param position
     * @return
     */
    public static Entry getEntry(String topic, int position) {

    }




    private static class Entry {
        // 消息对应的持久化文件偏移量
        private int position;
        // 消息在持久化文件中的长度
        private int length;
    }

}
