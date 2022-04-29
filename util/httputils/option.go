package httputils

type Option interface {
	apply(*HttpSetting)
}

type optionFunc func(*HttpSetting)

func (f optionFunc) apply(s *HttpSetting) {
	f(s)
}

func SignWithKey(key string) Option {
	return optionFunc(func(s *HttpSetting) {
		s.signKey = key
	})
}

func Expect(statusCode int) Option {
	return optionFunc(func(s *HttpSetting) {
		s.expectStatusCode = statusCode
	})
}
