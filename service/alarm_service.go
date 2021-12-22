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

package service

import (
	"strings"

	"github.com/go-gomail/gomail"

	"go-mysql-transfer/config"
	"go-mysql-transfer/domain/bo"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/domain/po"
	"go-mysql-transfer/util/dateutils"
	"go-mysql-transfer/util/dingtalk"
	"go-mysql-transfer/util/log"
	"go-mysql-transfer/util/stringutils"
)

type AlarmService struct {
}

func (s *AlarmService) faultAlarm(pipe *po.PipelineInfo, cause string) {
	go s.doAlarm(pipe, "客户端故障", cause)
}

func (s *AlarmService) failAlarm(pipe *po.PipelineInfo, cause string) {
	go s.doAlarm(pipe, "启动失败", cause)
}

func (s *AlarmService) panicAlarm(pipe *po.PipelineInfo, cause string) {
	go s.doAlarm(pipe, "管道停止", cause)
}

func (s *AlarmService) doAlarm(pipe *po.PipelineInfo, title, cause string) {
	if strings.Contains(pipe.AlarmItemList, constants.PipelineAlarmItemException) {
		if s.isMailEnable(pipe) {
			from := config.GetIns().GetSmtpConfig().User
			tos := strings.Split(pipe.GetAlarmMailList(), ",")
			subject := "[go-mysql-transfer]告警"
			body := "<h4>" + title + "：</h4>"
			body += "<ul>"
			body += "<li>管道：" + pipe.Name + "</li>"
			body += "<li>时间：" + dateutils.NowFormatted() + "</li>"
			body += "<li>原因：" + cause + "</li>"
			body += "</ul>"
			for _, to := range tos {
				s.doMail(from, to, subject, body)
			}
		}
		if s.isDingEnable(pipe) {
			subject := "告警"
			body := "## " + title
			body += "\n - 管道：" + pipe.Name
			body += "\n - 时间：" + dateutils.NowFormatted()
			body += "\n - 原因：" + cause
			s.doDingTalk(pipe.AlarmWebhook, pipe.AlarmWebhookSecretKey, subject, body)
		}
	}
}

func (s *AlarmService) batchReport(pipe *po.PipelineInfo, runtime *bo.PipelineRuntime) {
	if strings.Contains(pipe.AlarmItemList, constants.PipelineAlarmItemBatch) {
		if s.isMailEnable(pipe) {
			from := config.GetIns().GetSmtpConfig().User
			tos := strings.Split(pipe.GetAlarmMailList(), ",")
			subject := "[go-mysql-transfer]全量同步报告"
			body := "<h4>全量同步结果：</h4>"
			body += "<ul>"
			body += "<li>管道：" + pipe.Name + "</li>"
			body += "<li>开始时间：" + runtime.BatchStartTime.Load() + "</li>"
			body += "<li>结束时间：" + runtime.BatchEndTime.Load() + "</li>"

			body += "<li>数据总量：</li>"
			for k, v := range runtime.BatchTotalCounters {
				body += "<li>&ensp;&ensp;" + k + "：" + stringutils.ToString(v.Load()) + "</li>"
			}
			body += "<li>同步条数：</li>"
			for k, v := range runtime.BatchInsertCounters {
				body += "<li>&ensp;&ensp;" + k + "：" + stringutils.ToString(v.Load()) + "</li>"
			}

			body += "<li>消息：" + runtime.LatestMessage.Load() + "</li>"
			body += "</ul>"
			for _, to := range tos {
				go s.doMail(from, to, subject, body)
			}
		}
		if s.isDingEnable(pipe) {
			subject := "全量同步报告"
			body := "## 全量同步结果"
			body += "\n - 管道：" + pipe.Name
			body += "\n - 开始时间：" + runtime.BatchStartTime.Load()
			body += "\n - 结束时间：" + runtime.BatchEndTime.Load()
			body += "\n - 数据总量："
			for k, v := range runtime.BatchTotalCounters {
				body += "\n - &ensp;&ensp;" + k + "：" + stringutils.ToString(v.Load()) + "</li>"
			}
			body += "\n - 同步条数："
			for k, v := range runtime.BatchInsertCounters {
				body += "\n - &ensp;&ensp;" + k + "：" + stringutils.ToString(v.Load()) + "</li>"
			}
			body += "\n - 消息：" + runtime.LatestMessage.Load()
			go s.doDingTalk(pipe.AlarmWebhook, pipe.AlarmWebhookSecretKey, subject, body)
		}
	}
}

func (s *AlarmService) streamReport(pipe *po.PipelineInfo, runtime *bo.PipelineRuntime) {
	if strings.Contains(pipe.AlarmItemList, constants.PipelineAlarmItemStream) {
		if s.isMailEnable(pipe) {
			from := config.GetIns().GetSmtpConfig().User
			tos := strings.Split(pipe.GetAlarmMailList(), ",")
			subject := "[go-mysql-transfer]同步日报"
			body := "<ul>"
			body += "<li>管道：" + pipe.Name + "</li>"
			body += "<li>报告时间： " + dateutils.NowFormatted() + "</li>"
			body += "<li>响应Insert事件数量：" + stringutils.ToString(runtime.InsertCounter.Load()) + "</li>"
			body += "<li>响应Update事件数量：" + stringutils.ToString(runtime.UpdateCounter.Load()) + "</li>"
			body += "<li>响应Delete事件数量：" + stringutils.ToString(runtime.DeleteCounter.Load()) + "</li>"
			body += "<li>Position：" + runtime.PositionName.Load() + "  " + stringutils.ToString(runtime.PositionIndex.Load()) + "</li>"
			body += "</ul>"
			for _, to := range tos {
				go s.doMail(from, to, subject, body)
			}
		}
		if s.isDingEnable(pipe) {
			subject := "同步日报"
			body := "## 同步日报"
			body += "\n - 管道：" + pipe.Name
			body += "\n - 报告时间：" + dateutils.NowFormatted()
			body += "\n - 响应Insert事件数量：" + stringutils.ToString(runtime.InsertCounter.Load())
			body += "\n - 响应Update事件数量：" + stringutils.ToString(runtime.UpdateCounter.Load())
			body += "\n - 响应Delete事件数量：" + stringutils.ToString(runtime.DeleteCounter.Load())
			body += "\n - Position：" + runtime.PositionName.Load() + "  " + stringutils.ToString(runtime.PositionIndex.Load())
			go s.doDingTalk(pipe.AlarmWebhook, pipe.AlarmWebhookSecretKey, subject, body)
		}
	}
}

func (s *AlarmService) isMailEnable(pipeline *po.PipelineInfo) bool {
	if pipeline.AlarmMailList != "" && config.GetIns().IsSmtpUsed() {
		return true
	}
	return false
}

func (s *AlarmService) isDingEnable(pipeline *po.PipelineInfo) bool {
	if pipeline.GetAlarmWebhook() != "" {
		return true
	}
	return false
}

func (s *AlarmService) doMail(from, to, subject, body string) {
	msg := gomail.NewMessage()
	msg.SetHeader("From", from)
	msg.SetHeader("To", to)
	msg.SetHeader("Subject", subject) // 设置邮件主题
	msg.SetBody("text/html", body)    // 设置邮件正文
	smt := config.GetIns().GetSmtpConfig()
	dialer := gomail.NewDialer(smt.Host, smt.Port, smt.User, smt.Password)
	err := dialer.DialAndSend(msg)
	if err != nil {
		log.Errorf("发送邮件失败[%s]", err.Error())
	}
}

func (s *AlarmService) doDingTalk(webhook, secretKey, subject, body string) {
	message := &dingtalk.Message{
		Content: &dingtalk.MarkdownContent{
			Title: subject,
			Text:  body,
		},
	}
	err := dingtalk.Send(webhook, secretKey, message)
	if err != nil {
		log.Error(err.Error())
	}
}
