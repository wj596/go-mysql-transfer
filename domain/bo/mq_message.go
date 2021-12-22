package bo

type MessageBody struct {
	Topic     string      `json:"-"`
	Action    string      `json:"action"`
	Timestamp int64       `json:"timestamp"`
	Raw       interface{} `json:"raw,omitempty"`
	Date      interface{} `json:"date"`
}
