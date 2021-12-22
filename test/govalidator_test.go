package test

import (
	"fmt"
	"github.com/asaskevich/govalidator"
	"testing"
)

func TestEmail(t *testing.T) {
	fmt.Println(govalidator.IsEmail("https://www.cnblogs.com/rongfengliang/p/13843835.html"))
	fmt.Println(govalidator.IsEmail("999@126.com"))
}

func TestIsURL(t *testing.T) {
	fmt.Println(govalidator.IsURL("https://www.cnblogs.com/rongfengliang/p/13843835.html"))
	fmt.Println(govalidator.IsRFC3339("999@126.com"))
	fmt.Println(govalidator.IsURL("aaaaaaaaaaaaaaa.cn"))
}

func TestIsHost(t *testing.T) {
	fmt.Println(govalidator.IsURL("aaaaaaaaaaaaaaa"))
	fmt.Println(govalidator.IsURL("192"))
	fmt.Println(govalidator.IsURL("http://localhost:3306"))
	fmt.Println(govalidator.IsURL("kubesphere.jqk8s.jqsoft.net"))
}
