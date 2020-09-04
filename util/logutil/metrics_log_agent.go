package logutil

type MetricsLoggerAgent struct {
}

func NewMetricsLoggerAgent() *MetricsLoggerAgent {
	return &MetricsLoggerAgent{}
}

// Info logs to INFO log. Arguments are handled in the manner of fmt.Print.
func (s *MetricsLoggerAgent) Printf(format string, v ...interface{}) {
	Infof(format, v)
}
