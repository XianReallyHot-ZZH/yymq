package cn.youyou.yymq.store;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 持久化文本检索器
 * 职责：
 *  记录各个队列的message持久化检索信息，用于从持久化文件读取消息时指导store怎么读取消息，即每个message都对应了一份检索信息
 *  具体表现为：用于指导在文件的什么position读多长size的字节内容
 */
@Slf4j
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
        log.info(">>> [Indexer] add entry, topic: {}, position: {}, len: {}", topic, position, len);
        Entry entry = new Entry(position, len);
        indexes.add(topic, entry);
        indexMaps.computeIfAbsent(topic, k -> new HashMap<>()).put(position, new Entry(position, len));
    }

    /**
     * 获取topic对应的所有消息的检索信息
     *
     * @param topic
     * @return
     */
    public static List<Entry> getEntries(String topic) {
        return indexes.get(topic);
    }

    /**
     * 获取topic对应offset(position)的消息的检索信息
     *
     * @param topic
     * @param offset    等价于position
     * @return
     */
    public static Entry getEntry(String topic, int offset) {
        Map<Integer, Entry> map = indexMaps.get(topic);
        return map == null ? null : map.get(offset);
    }


    /**
     * 持久化文件检索信息
     */
    @AllArgsConstructor
    @Data
    public static class Entry {
        // 消息对应的持久化文件偏移量
        private int position;
        // 消息在持久化文件中的长度
        private int length;
    }

}
