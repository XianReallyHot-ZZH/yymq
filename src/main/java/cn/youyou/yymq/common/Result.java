package cn.youyou.yymq.common;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * client和server端通信，两端之间交互的结果
 */
@Data
@AllArgsConstructor
public class Result<T> {

    private int code;
    private T data;

    public static Result<String> ok() {
        return new Result<>(1, "OK");
    }

    public static Result<String> ok(String msg) {
        return new Result<>(1, msg);
    }

    public static Result<Message<?>> msg(Message<?> msg) {
        return new Result<>(1, msg);
    }

    public static Result<Message<?>> msg(String msg) {
        return new Result<>(1, Message.create(msg, null));
    }

    public static Result<List<Message<?>>> msg(List<Message<?>> msgs) {
        return new Result<>(1, msgs);
    }


}
