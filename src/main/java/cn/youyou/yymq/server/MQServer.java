package cn.youyou.yymq.server;

import cn.youyou.yymq.common.Message;
import cn.youyou.yymq.common.Result;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * server controller
 */
@RestController
@RequestMapping("/yymq")
public class MQServer {

    /**
     * 创建topic对应的server接口
     *
     * @param topic
     * @return
     */
    public Result<String> createTopic(@RequestParam("t") String topic) {
        return Result.ok(MessageQueue.createTopic(topic));
    }

    /**
     * consumer进行topic订阅对应的server接口
     *
     * @param topic
     * @param consumerId
     * @return
     */
    @RequestMapping("/sub")
    public Result<String> sub(@RequestParam("t") String topic,
                              @RequestParam("cid") String consumerId) {
        MessageQueue.sub(topic, consumerId);
        return Result.ok();
    }

    /**
     * consumer取消topic订阅对应的server接口
     *
     * @param topic
     * @param consumerId
     * @return
     */
    @RequestMapping("/unsub")
    public Result<String> unsub(@RequestParam("t") String topic,
                                @RequestParam("cid") String consumerId) {
        MessageQueue.unsub(topic, consumerId);
        return Result.ok();
    }

    /**
     * producer发送消息对应的server接口
     *
     * @param topic
     * @param message
     * @return
     */
    @RequestMapping("/send")
    public Result<String> send(@RequestParam("t") String topic,
                               @RequestBody Message<String> message) {
        return Result.ok("" + MessageQueue.send(topic, message));
    }

    /**
     * consumer拉取消息对应的server接口
     *
     * @param topic
     * @param consumerId
     * @return
     */
    @RequestMapping("/poll")
    public Result<Message<?>> poll(@RequestParam("t") String topic,
                                   @RequestParam("cid") String consumerId) {
        return Result.msg(MessageQueue.poll(topic, consumerId));
    }

    /**
     * consumer批量拉取消息对应的server接口
     *
     * @param topic
     * @param consumerId
     * @param size
     * @return
     */
    @RequestMapping("/poll-batch")
    public Result<List<Message<?>>> batch(@RequestParam("t") String topic,
                                          @RequestParam("cid") String consumerId,
                                          @RequestParam(name = "size", required = false, defaultValue = "1000") int size) {
        return Result.msg(MessageQueue.poll(topic, consumerId, size));
    }

    /**
     * consumer确认消息对应的server接口
     *
     * @param topic
     * @param consumerId
     * @param offset
     * @return
     */
    @RequestMapping("/ack")
    public Result<String> ack(@RequestParam("t") String topic,
                              @RequestParam("cid") String consumerId,
                              @RequestParam("offset") Integer offset) {
        return Result.ok("" + MessageQueue.ack(topic, consumerId, offset));
    }

}
