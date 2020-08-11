package logutil

type ZkLoggerAgent struct {
}

func NewZkLoggerAgent() *ZkLoggerAgent {
	return &ZkLoggerAgent{}
}

func (s *ZkLoggerAgent) Printf(template string, args ...interface{}) {
	Infof(template, args...)
}
