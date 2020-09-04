package logutil

type ElsLoggerAgent struct {
}

func NewElsLoggerAgent() *ElsLoggerAgent {
	return &ElsLoggerAgent{}
}

func (s *ElsLoggerAgent) Printf(format string, v ...interface{}) {
	Infof(format, v)
}
