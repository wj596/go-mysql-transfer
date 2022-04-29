package httputils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGet(t *testing.T) {
	a := assert.New(t)
	r, err := Get("http://httpbin.org/get")
	a.Nil(err)
	a.NotEmpty(r)
}

func TestGetForString(t *testing.T) {
	a := assert.New(t)
	r, err := GetForString("http://httpbin.org/get")
	a.Nil(err)
	a.NotEmpty(r)
}

func TestGetForRespond(t *testing.T) {
	a := assert.New(t)
	r, err := GetForRespond("http://httpbin.org/get")
	a.Nil(err)
	a.NotEmpty(r)
}

func TestGetForExpect(t *testing.T) {
	a := assert.New(t)
	r, err := GetForString("http://httpbin.org/bearer", Expect(200)) // response 401
	a.NotNil(err)
	a.Empty(r)
}

func TestGetChain(t *testing.T) {
	a := assert.New(t)
	r, err := R().SetExpect(200).Get("http://httpbin.org/get")
	fmt.Println(r)
	a.Nil(err)
	a.NotEmpty(r)
}

func TestPost(t *testing.T) {
	a := assert.New(t)
	body := P{
		"test": "test",
	}
	err := Post("http://httpbin.org/post", body)
	a.Nil(err)
}

func TestPostForString(t *testing.T) {
	a := assert.New(t)
	body := P{
		"test": "test",
	}
	r, err := PostForString("http://httpbin.org/post", body)
	a.Nil(err)
	a.NotEmpty(r)
}

func TestPostForChain(t *testing.T) {
	a := assert.New(t)
	r, err := R().SetJson(P{
		"test": "test",
	}).Post("http://httpbin.org/post")
	a.Nil(err)
	a.NotEmpty(r)
}

func TestPostFormForChain(t *testing.T) {
	a := assert.New(t)
	r, err := R().SetForm(P{
		"test": "test",
	}).
	AddHeader("SS","XX").
	Post("http://httpbin.org/post")
	a.Nil(err)
	a.NotEmpty(r)
	fmt.Println(r.ToIndentJson())
}
