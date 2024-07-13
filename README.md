# yymq
[yymq](https://github.com/XianReallyHot-ZZH/yymq)是一个支持持久化的消息队列中间件，宕机重启具备数据恢复的能力，除此之外，实现了部分mq的常见基本功能，目前整个中间件在设计上是单机模式，未来可能会扩展成分布式集群模式。

## 说明
* 当前的队列与topic关联关系规定为：一个topic只能关联一个队列，一个队列只能关联一个topic
* 当前的消息订阅遵循如下定义：一个消费者只能订阅一个topic，一个topic能被多个消费者订阅，server端会为每个消费者维护各自的消费位点(offset)
* 当前的持久化与队列的存储设计为：一个队列(topic)对应一份持久化文件，文件命名为{topic_name}-{n}.dat, 其中n为文件编号，支持多文件续写（这个后续支持）


## 快速开始
1. 启动SpringBoot应用，启动类YymqApplication
2. YYMqDemo里提供了一个demo，模拟发送消息，拉取消息，监听消息，ack


## 功能汇总
1. 客户端基本功能
    * 创建topic(√)
    * 创建producer(√)
    * 创建consumer(√)
    * 发送消息(√)
    * 拉取消息(√)
    * 监听消息（被动接受消息）(√)
    * 消息ack(√)
2. server基本功能
    * 创建topic(√)
    * 响应发送消息(√)
    * 响应拉取消息(√)
    * 响应ack+消费位点offset更新(√)
    * 订阅关系维护(topic,consumer,offset)(√)
    * 消息持久化(√)
        * 单文件写入(√)
        * 多文件续写(todo)
    * 订阅关系持久化(todo)
    * 持久化恢复(doing)
        * 文件写入位点恢复(√)
        * 多文件续写恢复(todo)
        * 订阅关系恢复(todo)
    处理并发问题(todo)
3. 高级功能(todo)
    * 考虑批量操作, 包括读写, 可以打包和压缩
    * 考虑实现消息过期,消息重试,消息定时投递等策略
    * 考虑消息清理策略, 包括定时清理,按容量清理、LRU 等
    * 考虑 Spring mvc 替换成 netty 的TCP 协议, rsocket/websocket
    * 数据分区、副本、消费组
    * 分布式集群模式
4. 对接各种技术，构建生态环境(todo)
    * 拆解项目结构，拆解成：client，common，server等模块
    * 考虑封装 JMS 1.1 接口规范
    * 考虑实现 STOMP 消息规范
    * 考虑实现消息事务机制与事务管理器
    * 对接Spring
    * 对接 Camel 或Spring Integration
    * 优化内存和磁盘使用


