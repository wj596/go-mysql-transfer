[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

# 项目简介

go-mysql-transfer是一个能实时读取MySQL二进制日志binlog，并生成指定格式消息，发送给redis、mongodb、elasticsearch、rabbitmq、kafka、rocketmq、NSQ、HTTP接口的应用程序，实现数据实时增量同步。

go-mysql-transfer基于规则或者动态脚本完成数据解析和消息生成逻辑，无需用户编码，简洁高效、稳定可靠。

# 实现原理

1、go-mysql-transfer将自己伪装为MySQL Slave向Master发送dump协议，获取binlog

2、go-mysql-transfer更加配置的规则，或者lua脚本生成消息

3、将生成的消息批量发送给接收端

![](https://upload-images.jianshu.io/upload_images/2897315-3b0fee8246d16250.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


# 功能特性

- 极少依赖： 只依赖MySQL 数据库，分布式环境下使用内建的分布式协调机制，不需要安装第三方分布式协调服务，如Zookeeper、Etcd等；更少的依赖意味着后续需要更少的部署和运维成本。

- 易部署：原生Native程序，无需安装运行时环境，如JDK、.net framework等；支持单机和集群部署两种部署模式。

- 任务重试：支持自定义任务重试次数、重试时间间隔。当任务执行失败时，会按照固定的间隔时间进行重试。

- 任务超时：支持自定义任务超时时间，当任务超时，会强制结束执行。

- 失败转移：当任务在一个执行节点上执行失败，会转移到下一个可用执行节点上继续执行。如任务在节点A上执行失败，会转移到节点B上继续执行，如果失败会转移到节点C上继续执行，直到执行成功。

- misfire补偿机制：由于调度服务器宕机、资源耗尽等原因致使任务错过激活时间，称之为哑火(misfire)。比如每天23点整生成日结报表，但是恰巧在23点前服务器宕机、此任务就错失了一次调度。如果我们设置了misfireThreshold为30分钟，如果服务器在23点30分之前恢复，调度器会进行一次执行，以补偿在23点整哑火的调度。

- 负载均衡：如果集群节点为集群部署，调度服务器可以使用轮询、随机、加权轮询、加权随机等路由策略，为任务选择合适的执行节点。既可以保证执行节点高可用、我单点隐患，也可以将压力分散到不同的执行节点。

- 任务分片：将大任务拆解为多个小任务均匀的散落在多个节点上并行执行，以协作的方式完成任务。比如订单核对业务，我们有天津、上海、重庆、河北、山西、辽宁、吉林、江苏、浙江、安徽十个省市的账单，如果数据量比较大，单机处理这些订单的核对业务显然不现实。

  gojob可以将任务分为3片：执行节点1负责-->天津、上海、重庆、河北；执行节点2负责-->山西、辽宁、吉林;  执行节点13负责-->江苏、浙江、安徽。这样可以用3台机器来合力完成这个任务。如果你的机器足够，可以将任务分成更多片，用更多的机器来协同处理。

- 弹性扩缩容：调度器会感知执行节点的增加和删除、上线和下线，并将执行节点的变化情况应用到下一次的负载均衡算法和任务分片算法中。支持动态的执行节点动态横向扩展，弹性伸缩整个系统的处理能力。

- 调度唯一性：调度节点集群使用Raft算法进行主节点选举，一个集群中只存在一个主节点。任务在一个执行周期内，只会被主节点调用一次，保证调度的一致性。

- 调度节点高可用：集群内通过Raft共识算法和数据快照将作业元数据实时进行同步，调度节点收到同步的数据后存在自己内建BoltDB存储引擎中；作业元数据具有强一致性和多副本存储的特性；任务可在任意调度节点被调度，调度节点之间可以无缝衔接，任何一个节点宕机另一个节点可以在毫秒计的时间内接替，保证调度节点无单点隐患。

- 数据库节点高可用：由于作业元数据保存在节点自己的存储引擎中，MySQL数据库只用来保存调度日志。日志数据的特性使其可容忍短时间内不一致甚至丢失(虽然极少发生但理论上可容忍)，因此将日志数据异步写入多库，无需对数据库做集群或者同步设置。极端情况下，数据库节点全部宕机都不会影响调度业务的正常运行，保证数据库节点无单点隐患。

- 任务依赖：任务可以设置多个子任务，触发时机。如：任务执行结束触发子任务、任务执行成功触发子任务、任务执行失败触发子任。

- 告警：支持邮件告警。任务调度失败会发送告警邮件到指定的邮箱，每个任务可配置多个告警邮箱。调度节点出现故障、数据库节点出现故障也会发送告警邮箱。
- 数字签名：支持HMAC( 哈希消息认证码 )数字签名，调度节点和执行节点之间可以通过数字签名来确认身份。

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

