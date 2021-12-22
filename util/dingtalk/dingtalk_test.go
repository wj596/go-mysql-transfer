package dingtalk

import (
	"fmt"
	"go-mysql-transfer/util/dateutils"
	"testing"
)

func TestSend(t *testing.T) {
	///webhook := "https://oapi.dingtalk.com/robot/send?access_token=365e43cf29670d7901704cff50bab2cecaae66f9ecd28e4900b56f6942ea9cda"
	///secretKey := "SEC2f5c7e625b1b404d24439f0e8f4f03f0f459679b3732d22f7c038ae7b407f6cc"

	var subject string
	var body string
	subject = "异常告警"
	body += "## 异常告警"
	body += "\n - 管道：" + "测试管道"
	body += "\n - 时间：" + dateutils.NowFormatted()
	body += "\n - 状态：" + "客户端异常"
	body += "\n - 原因：" + "dial tcp 127.0.0.1:6379: connectex: No connection could be made because the target machine actively refused it."

	message := &Message{
		Content: &MarkdownContent{
			Title: subject,
			Text:  body,
		},
	}
	data, _ := message.GetBody()
	fmt.Println(string(data))

	//err := Send(webhook, secretKey, message)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//respData :="{\"errcode\":0,\"errmsg\":\"ok\"}"
	//var result Response
	//err := json.Unmarshal([]byte(respData), &result)
	//if err != nil {
	//	fmt.Println(err)
	//}
	//fmt.Println(result.Errcode)

}
