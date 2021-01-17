[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

# 简介

go-mysql-transfer是一款MySQL数据库实时增量同步工具。

能够监听MySQL二进制日志(Binlog)的变动，将变更内容形成指定格式的消息，实时发送到接收端。从而在数据库和接收端之间形成一个高性能、低延迟的增量数据同步更新管道。

# 特性  

1、简单，不依赖其它组件，一键部署
2、集成多种接收端，如：Redis、MongoDB、Elasticsearch、RocketMQ、Kafka、RabbitMQ、HTTP API等，无需编写客户端，开箱即用
3、内置丰富的数据解析、消息生成规则、模板语法
4、支持Lua脚本扩展，可处理复杂逻辑
5、集成Prometheus客户端，支持监控告警
6、集成Web Admin监控页面
7、支持高可用集群部署
8、数据同步失败重试
9、支持全量数据初始化


# 原理

1、将自己伪装为MySQL的Slave监听binlog，获取binlog的变更数据

2、根据规则或者lua脚本解析数据，生成指定格式的消息

3、将生成的消息批量发送给接收端

# 与同类工具比较

<table>
    <thead>
        <tr>
            <th width="20%">特色</th>
            <th width="20%">Canal</th>
            <th width="20%">mysql_stream</th>
             <th width="40%">go-mysql-transfer</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>开发语言</td>
            <td>Java</td>
             <td>Python</td>
             <td>Golang</td>
        </tr>
        <tr>
            <td>高可用</td>
            <td>支持</td>
             <td>支持</td>
             <td>支持</td>
        </tr>
        <tr>
            <td>接收端</td>
            <td>编码定制</td>
             <td>Kafka等(MQ)</td>
             <td>Redis、MongoDB、Elasticsearch、RabbitMQ、Kafka、RocketMQ、HTTP API  <br>后续支持更多</td>
        </tr>
        <tr>
            <td>全量数据初始化</td>
            <td>不支持</td>
             <td>支持</td>
             <td>支持</td>
        </tr>
        <tr>
            <td>数据格式</td>
            <td>编码定制</td>
             <td>Json（固定格式）</td>
             <td>Json（规则配置)<br>模板语法<br>Lua脚本</td>
        </tr>
    </tbody>
</table>

# 安装包

**二进制安装包**

直接下载安装包:  [点击下载](https://github.com/wj596/go-mysql-transfer/releases)

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

# 使用说明

1、[go-mysql-transfer实现详解](https://www.jianshu.com/p/dce9160d298c?_blank)

2、[同步到Redis操作说明](https://www.jianshu.com/p/c533659a1d83?_blank)

3、[同步到MongoDB操作说明](https://www.jianshu.com/p/51124c9371f9?_blank)

4、[同步到Elasticsearch操作说明](https://www.jianshu.com/p/5a9b6c4f318c?_blank)

5、[同步到RocketMQ操作说明](https://www.jianshu.com/p/18bb121bbf63?_blank) 

6、[同步到Kafka操作说明](https://www.jianshu.com/p/aec8e4c28c06?_blank)

7、[同步到RabbitMQ操作说明](https://www.jianshu.com/p/ba5f1d3c75f2?_blank)

8、[让go-mysql-transfer具备无尽的扩展能力（^_^）Lua脚本使用说明](https://www.jianshu.com/p/c4b0147d65a7?_blank)

# 感谢

* [go-mysql](github.com/siddontang/go-mysql)

* [go-mysql-elasticsearch](https://github.com/siddontang/go-mysql-elasticsearch)

* [go-redis](https://github.com/go-redis/redis)

* [rocketmq-client-go](https://github.com/apache/rocketmq-client-go)




# 更新日志

**v1.0.0 bate**

* 9.17  初始化提交bate版本

**v1.0.1 release**

* 9.22  release

**v1.0.2 release**

* 添加dbOps（数据库操作）、httpOps（http操作）两个Lua模块
* 修复enum类型字段出现的乱码问题
* redis接收端增加*Sorted*  Set数据类型支持
* 修复了近来反馈的bug

