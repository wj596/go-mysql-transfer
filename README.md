[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

# 项目简介

go-mysql-transfer是一款MySQL数据实时增量同步工具。能够实时解析MySQL二进制日志binlog，并生成指定格式的消息，同步到接收端。

# 功能特性

- 不依赖其它组件，一键部署

- 集成多种接收端，如：Redis、MongoDB、Elasticsearch、RabbitMQ、Kafka、RocketMQ，不需要再编写客户端，开箱即用

- 内置丰富的数据解析、消息生成规则；支持Lua脚本扩展，以处理更复杂的数据逻辑

- 集成prometheus客户端，支持监控告警

- 支持高可用集群部署，可选Zookeeper或Ectd

- 支持数据同步失败重试

- 支持全量数据初始化同步  

# 实现原理

1、go-mysql-transfer将自己伪装为MySQL Slave向Master发送dump协议，获取binlog

2、go-mysql-transfer根据的规则，或者lua脚本解析数据，生成消息

3、将生成的消息批量发送给接收端


# 与同类工具比较

| 特色       | Canal    | mysql_stream | go-mysql-transfer                                            |
| ---------- | -------- | ------------ | ------------------------------------------------------------ |
| 开发语言   | Java     | Python       | Golang                                                       |
| HA         | 支持     | 支持         | 支持                                                         |
| 接收端     | 编码定制 | Kafka等      | Redis、MongoDB、Elasticsearch、RabbitMQ、Kafka、RocketMQ、<br />后续支持更多 |
| 数据初始化 | 不支持   | 支持         | 支持                                                         |
| 数据格式   | 编码定制 | json（固定） | 规则 (固定)<br />Lua脚本 (定制)     

# 安装包

**二进制安装包**

直接下载编译好的安装包:  [点击下载](https://github.com/wj596/go-mysql-transfer/releases)

**源码编译**

1、依赖Golang 1.14 及以上版本

2、设置' GO111MODULE=on '

3、拉取源码 ‘ go get -d github.com/wj596/go-mysql-transfer’

3、进入目录，执行 ‘ go build ’ 编译

# 全量数据初始化

go-mysql-transfer -stock

# 运行

**开启MySQL的binlog**

```
#Linux在my.cnf文件
#Windows在my.ini文件
log-bin=mysql-bin # 开启 binlog
binlog-format=ROW # 选择 ROW 模式
server_id=1 # 配置 MySQL replaction 需要定义，不要和 go-mysql-transfer 的 slave_id 重复
```

**命令行运行**

1、修改app.yml

2、Windows直接运行 go-mysql-transfer.exe

3、Linux执行 nohup go-mysql-transfer &



**docker运行**

1、拉取源码 ‘ go get -d github.com/wj596/go-mysql-transfer’

2、修改配置文件 ‘ app.yml ’ 中相关配置

3、构建镜像 ‘ docker image build -t go-mysql-transfer -f Dockerfile . ’

4、运行 ‘ docker run -d --name go-mysql-transfer -p 9595:9595  go-mysql-transfer:latest ’

# 使用说明

[go-mysql-transfer实现详解](https://www.jianshu.com/p/dce9160d298c?_blank)

[go-mysql-transfer增量同步数据到Redis操作说明](https://www.jianshu.com/p/c533659a1d83?_blank)

[go-mysql-transfer增量同步数据到MongoDB操作说明](https://www.jianshu.com/p/51124c9371f9?_blank)

[go-mysql-transfer增量同步数据到Elasticsearch操作说明](https://www.jianshu.com/p/5a9b6c4f318c?_blank)

[go-mysql-transfer增量同步数据到RocketMQ操作说明](https://www.jianshu.com/p/18bb121bbf63?_blank)

[go-mysql-transfer增量同步数据到Kafka操作说明](https://www.jianshu.com/p/aec8e4c28c06?_blank)

[go-mysql-transfer增量同步数据到RabbitMQ操作说明](https://www.jianshu.com/p/ba5f1d3c75f2?_blank)

# 技术栈

* [go-mysql](github.com/siddontang/go-mysql)

* [go-mysql-elasticsearch](https://github.com/siddontang/go-mysql-elasticsearch)

* [go-redis](https://github.com/go-redis/redis)

* [rocketmq-client-go](https://github.com/apache/rocketmq-client-go)

感谢以上优秀的开源框架


# 更新日志

**V1.0.0 Bate**1

* 初始化提交Bate1版本
