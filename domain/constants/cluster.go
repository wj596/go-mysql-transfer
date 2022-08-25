/*
 * Copyright 2021-2022 the original author(https://github.com/wj596)
 *
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package constants

const (
	HeartbeatInterval       = 1 //主从心跳间隔，单位秒
	HeartbeatTimeout        = 5 //心跳超时时间，单位秒； 超过这个时间没有心跳，说明从节点离线
	HeartbeatFailureMaximum = 3 //心跳失败最大次数； 超过这个数据量，从接口可以判断自己产生网络分区
)

const (
	SyncEventTypeSource   = 1
	SyncEventTypeEndpoint = 2
	SyncEventTypePipeline = 3
)
