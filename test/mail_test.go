package test

import (
	"github.com/go-gomail/gomail"
	"go-mysql-transfer/util/dateutils"
	"testing"
)

func TestSendMail(t *testing.T) {

	var subject string
	var body string
	subject = "[go-mysql-transfer]异常告警"
	body += "<h4>异常告警：</h4>"
	body += "<ul>"
	body += "<li>管道：测试管道</li>"
	body += "<li>时间：" + dateutils.NowFormatted() + "</li>"
	body += "<li>状态：客户端异常</li>"
	body += "<li>原因：dial tcp 127.0.0.1:6379: connectex: No connection could be made because the target machine actively refused it.</li>"
	body += "</ul>"

	msg := gomail.NewMessage()
	msg.SetHeader("From", "wj596@126.com")
	msg.SetHeader("To", "918952016@qq.com")
	msg.SetHeader("Subject", subject) // 设置邮件主题
	msg.SetBody("text/html", body)    // 设置邮件正文

	dialer := gomail.NewDialer("smtp.126.com", 25, "wj596@126.com", "PUPHJPJTKNQEEDTD")
	err := dialer.DialAndSend(msg)
	if err != nil {
		t.Fatal(err)
	}
}
