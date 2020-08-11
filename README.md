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

# 安装包

**二进制安装包**

直接下载对应操作系统的安装包, [点击下载](https://github.com/wj596/gojob/releases)

**源码编译**

1、依赖Golang 1.12 及以上版本

2、设置' GO111MODULE=on '

3、拉取源码 ‘ go get -d github.com/wj596/gojob ’

3、进入目录，执行 ‘ go build ’  编译

**docker镜像**

1、拉取源码 ‘ go get -d github.com/wj596/gojob ’

2、修改配置文件 ‘ application.yml ’ 中相关配置

3、构建镜像 ‘ docker image build -t gojob -f Dockerfile . ’

4、运行 ‘ docker run -d --name gojob -p 8071:8071 gojob:latest ’

# 使用说明

[快速开始](https://github.com/wj596/gojob/wiki/faststart?_blank)

[单机部署](https://github.com/wj596/gojob/wiki/standalone?_blank)

[集群部署](https://github.com/wj596/gojob/wiki/cluster?_blank)

[作业配置](https://github.com/wj596/gojob/wiki/deploy?_blank)

[二次开发](https://github.com/wj596/gojob/wiki/develop?_blank)

# 逻辑架构图

**单机模式**

<img src="https://github.com/wj596/shares/blob/master/gojob/gojob1.png" width="60%">

单机模式下主要运行逻辑：

1、通过WEB界面定义作业元数据，并保存到本地存储引擎

3、任务调度器从本地持久化存储获取作业调度信息

4、按照执行节点选择策略或数据分片策略，选取执行节点，发送HTTP调用请求到执行节点

5、调度日志异步多写到各个MySQL节点

**集群模式**

<img src="https://github.com/wj596/shares/blob/master/gojob/gojob2.png" width="60%">  

集群模式下主要运行逻辑：

1、Raft选举算法选取主节点

2、在WEB界面定义作业元数据，通过Raft强数据一致性协议将数据复制到其他节点

3、各节点将作业元数据保存到本地存储引擎 

6、主节点从本地持久化存储获取调度信息

7、主节点按照执行节点选择策略或数据分片策略，选取执行节点，发送HTTP调用请求到执行节点

8、主节点将调度日志异步多写到各个MySQL节点

9、如果集群内有任一节点宕机，将重新触发主节点选举



**Raft共识机制**

基于日志的状态机复制：

<img src="https://github.com/wj596/shares/blob/master/gojob/gojob3-1.png">  

节点状态转换：

<img src="https://github.com/wj596/shares/blob/master/gojob/gojob3-2.png">  

Raft强数据一致性主要逻辑：

1、选举安全：在一个特定的任期内，最多只能选出一名领导人。

2、Leader Append-Only：领导者只能在其日志中添加新条目（它既不能覆盖也不能删除条目）。

3、日志匹配：如果两个日志包含具有相同索引和术语的条目，则日志在通过给定索引的所有条目中都是相同的。

4、领导者完整性：如果在给定的术语中提交了日志条目，那么从该术语开始，它将出现在领导者的日志中。

5、状态机安全性：如果服务器已将特定日志条目应用于其状态机，则其他服务器不会对同一日志应用不同的命令。



# UI预览

单机模式下的"运行分析" ：

<img src="https://github.com/wj596/shares/blob/master/gojob/gojob4.png"> 

集群模式下的"运行分析" ：

<img src="https://github.com/wj596/shares/blob/master/gojob/gojob5.png"> 

"集群管理"：

<img src="https://github.com/wj596/shares/blob/master/gojob/gojob6.png">

"任务管理"：

<img src="https://github.com/wj596/shares/blob/master/gojob/gojob7.png">

<img src="https://github.com/wj596/shares/blob/master/gojob/gojob8.png">

"调度日志"：

<img src="https://github.com/wj596/shares/blob/master/gojob/gojob9.png">


# 技术栈

* Web框架  [gin](https://github.com/gin-gonic/gin)
* ORM  [Xorm](https://github.com/go-xorm/xorm)
* 静态资源打包  [statik](https://github.com/rakyll/statik)
* UI框架  [vue-element-admin](https://github.com/PanJiaChen/vue-element-admin)

感谢以上优秀的开源框架

# 更新日志

**V1.0.0 Bate**

* 初始化提交Bate版本

