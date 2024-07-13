package cn.youyou.yymq.common;

import cn.youyou.yymq.model.Subscription;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@Data
@NoArgsConstructor
public class Stat {
    private Subscription subscription;
    //    private int remaining;
    private int total;      // 队列里多少数据量
    private int position;   // 队列（文件）当前最新位点
}
