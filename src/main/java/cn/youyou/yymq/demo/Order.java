package cn.youyou.yymq.demo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Description for this class.
 *
 * @Author : kimmking(kimmking@apache.org)
 * @create 2024/6/25 下午9:10
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Order {
    private long id;
    private String item;
    private double price;
}
