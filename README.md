[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

# 项目简介

go-mysql-transfer是一个能实时读取MySQL二进制日志binlog，并生成指定格式消息，发送给redis、mongodb、elasticsearch、rabbitmq、kafka、rocketmq、NSQ、HTTP接口的应用程序，实现数据实时增量同步。

go-mysql-transfer基于规则或者动态脚本完成数据解析和消息生成逻辑，无需用户编码，简洁高效、稳定可靠。

# 实现原理

1、go-mysql-transfer将自己伪装为MySQL Slave向Master发送dump协议，获取binlog

2、go-mysql-transfer更加配置的规则，或者lua脚本生成消息

3、将生成的消息批量发送给接收端

# 功能特性

- 不依赖其它组件，一键部署

- 集成多种接收端，如：redis、mongodb、elasticsearch、rabbitmq、kafka、rocketmq、NSQ、HTTP接口等

- 基于规则或者动态脚本进行数据解析和消息生成，方便扩展

- 集成prometheus客户端，支持监控告警

- 支持高可用集群，可选Zookeeper或Ectd

- 支持失败重试

- 支持全量数据初始化

# 与同类工具比较

| 特色       | Canal      | mysql_stream | go-mysql-transfer                                            |
| ---------- | ---------- | ------------ | ------------------------------------------------------------ |
| 开发语言   | Java       | Python       | Golang                                                       |
| HA         | 支持       | 支持         | 支持                                                         |
| 数据落地   | 定制(开发) | Kafka等      | redis、mongodb、elasticsearch、rabbitmq、<br />kafka、rocketmq、NSQ、HTTP接口等 |
| 数据初始化 | 不支持     | 支持         | 支持                                                         |
| 数据格式   | 定制(开发) | json（固定） | 规则<br />lua脚本     

# 安装包

**二进制安装包**

直接下载安装包:  [点击下载](https://github.com/wj596/go-mysql-transfer/releases)

**源码编译**

1、依赖Golang 1.14 及以上版本

2、设置' GO111MODULE=on '

3、拉取源码 ‘ go get -d github.com/wj596/go-mysql-transfer’

3、进入目录，执行 ‘ go build ’ 编译

# 运行

1、修改app.yml

2、Windows直接运行 go-mysql-transfer.exe

3、Linux执行 nohup go-mysql-transfer &


全量数据初始化执行 ：go-mysql-transfer -stock


# 使用说明

[快速开始](https://github.com/wj596/gojob/wiki/faststart?_blank)

[单机部署](https://github.com/wj596/gojob/wiki/standalone?_blank)

[集群部署](https://github.com/wj596/gojob/wiki/cluster?_blank)

[作业配置](https://github.com/wj596/gojob/wiki/deploy?_blank)

[二次开发](https://github.com/wj596/gojob/wiki/develop?_blank)


# 技术栈

*protocol and replication [go-mysql](github.com/siddontang/go-mysql)

# 更新日志

**V1.0.0 Bate**

* 初始化提交Bate版本

