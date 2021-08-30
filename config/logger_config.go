package config

const (
	_logFileName     = "system.log"
	_logMaxSize      = 500
	_logMaxAge       = 30
	_logEncodingJson = "json"
)

// LoggerConfig 日志配置
type LoggerConfig struct {
	Level    string `yaml:"level"`     //日志级别 debug|info|warn|error
	Store    string `yaml:"store"`     //日志目录
	FileName string `yaml:"file_name"` //日志文件名称
	MaxSize  int    `yaml:"max_size"`  //日志文件最大M字节
	MaxAge   int    `yaml:"max_age"`   //日志文件最大存活的天数
	Compress bool   `yaml:"compress"`  //是否启用压缩
	Encoding string `yaml:"encoding"`  //日志编码 console|json
}

func (c *LoggerConfig) GetLevel() string {
	return c.Level
}

func (c *LoggerConfig) GetStore() string {
	return c.Store
}

func (c *LoggerConfig) GetFileName() string {
	if c.FileName == "" {
		c.FileName = _logFileName
	}
	return c.FileName
}

func (c *LoggerConfig) GetMaxSize() int {
	if c.MaxSize <= 0 {
		c.MaxSize = _logMaxSize
	}
	return c.MaxSize
}

func (c *LoggerConfig) GetMaxAge() int {
	if c.MaxAge <= 0 {
		c.MaxAge = _logMaxAge
	}
	return c.MaxAge
}

func (c *LoggerConfig) IsCompress() bool {
	return c.Compress
}

func (c *LoggerConfig) GetEncoding() string {
	return c.Encoding
}

func (c *LoggerConfig) IsJsonEncoding() bool {
	return c.Encoding == _logEncodingJson
}
