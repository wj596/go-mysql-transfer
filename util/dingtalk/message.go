package dingtalk

import "encoding/json"

type Message struct {
	At      *At
	Content Content
}

type Result struct {
	Errcode int64  `json:"errcode"`
	Errmsg  string `json:"errmsg"`
}

type At struct {
	AtMobiles []string `json:"atMobiles,omitempty"` //被@人的手机号
	AtUserIds []string `json:"atUserIds,omitempty"` //被@人的用户userid
	IsAtAll   bool     `json:"isAtAll,omitempty"`   //是否@所有人
}

type Content interface {
	GetType() string
}

type TextContent struct {
	content string `json:"content,omitempty"` //消息内容
}

type MarkdownContent struct {
	Title string `json:"title,omitempty"` //首屏会话透出的展示内容
	Text  string `json:"text,omitempty"`  //markdown格式的消息
}

type LinkContent struct {
	Title      string `json:"title,omitempty"`      //消息标题
	Text       string `json:"text,omitempty"`       //消息内容 如果太长只会部分展示
	MessageUrl string `json:"messageUrl,omitempty"` //点击消息跳转的URL
	PicUrl     string `json:"picUrl,omitempty"`     //图片URL
}

func (s *Message) GetBody() ([]byte, error) {
	msg := make(map[string]interface{}, 3)
	contentType := s.Content.GetType()
	msg["msgtype"] = contentType
	msg[contentType] = s.Content
	if s.At != nil {
		msg["at"] = s.At
	}
	return json.Marshal(msg)
}

func NewMessage() *Message {
	return new(Message)
}

func NewMarkdownContent() *MarkdownContent {
	return new(MarkdownContent)
}

func NewAt() *At {
	return new(At)
}

func (s *Message) SetContent(content Content) {
	s.Content = content
}

func (s *Message) SetAt(at *At) {
	s.At = at
}

func (s *TextContent) GetType() string {
	return "text"
}

func (s *MarkdownContent) GetType() string {
	return "markdown"
}

func (s *LinkContent) GetType() string {
	return "link"
}
