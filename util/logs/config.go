package logs

const (
	_logFileName     = "system.log"
	_logLevelInfo    = "info"
	_logLevelWarn    = "warn"
	_logLevelError   = "error"
	_logMaxSize      = 500
	_logMaxAge       = 30
	_logEncodingJson = "json"
)

// logger 配置
type Config struct {
	Level    string `yaml:"level"`     //日志级别 debug|info|warn|error
	Store    string `yaml:"store"`     //日志目录
	FileName string `yaml:"file_name"` //日志文件名称
	MaxSize  int    `yaml:"max_size"`  //日志文件最大M字节
	MaxAge   int    `yaml:"max_age"`   //日志文件最大存活的天数
	Compress bool   `yaml:"compress"`  //是否启用压缩
	Encoding string `yaml:"encoding"`  //日志编码 console|json
}
