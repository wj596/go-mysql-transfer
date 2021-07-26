package httpclient

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
)

func TestHttpClientGet(t *testing.T) {
	res, err := DefaultClient.GET("http://httpbin.org/get").Do()
	if err != nil {
		t.Error("get failed", err)
	}

	defer res.Body.Close()

	if res.StatusCode != 200 {
		t.Error("Status Code not 200")
	}
}

func TestHttpClientTimeout(t *testing.T) {
	res, err := DefaultClient.SetTimeout(10).GET("http://127.0.0.1:8801/zombie").Do()
	if err != nil {
		t.Error("get failed", err)
	}

	defer res.Body.Close()

	if res.StatusCode != 200 {
		t.Error("Status Code not 200")
	}
}

func TestHttpClientGetForString(t *testing.T) {
	str, err := DefaultClient.GET("http://httpbin.org/get").DoForString()
	if err != nil {
		t.Error("get failed", err)
	}

	println(str)
}

func TestHttpClientGetWithParameters(t *testing.T) {
	// 实际请求地址：http://httpbin.org/get?age=18&name=wf
	str, err := DefaultClient.GET("http://httpbin.org/get").AddParameters(H{
		"token": "12596358412",
	}).AddParameters(H{
		"name": "wf",
		"age":  18,
	}).DoForString()

	if err != nil {
		t.Error("get failed", err)
	}

	println(str)
}

func retryCondition(res *http.Response) bool {
	if res == nil { // Response为空时候重试
		return true
	}
	if res.StatusCode != http.StatusOK { // 状态不是200的时候重试
		return true
	}

	return true
}

func TestHttpClientRetry(t *testing.T) {
	NewClient().
		SetRetryCount(3).                      // 重试次数
		SetRetryInterval(5).                   // 重试间隔 （秒）
		AddRetryConditionFunc(retryCondition). // 重试条件
		GET("http://test.org/get").
		Do()

	// log like:
	//{"level":"error","msg":"Get \"http://test.org/get\": dial tcp: lookup test.org: no such host"}
	//{"level":"info","msg":"第1次重试： GET | http://test.org/get )"}
	//{"level":"error","msg":"Get \"http://test.org/get\": dial tcp: lookup test.org: no such host"}
	//{"level":"info","msg":"第2次重试： GET | http://test.org/get )"}
	//{"level":"error","msg":"Get \"http://test.org/get\": dial tcp: lookup test.org: no such host"}
	//{"level":"info","msg":"第3次重试： GET | http://test.org/get )"}
	//{"level":"error","msg":"Get \"http://test.org/get\": dial tcp: lookup test.org: no such host"}
}

func TestHttpClientPostForm(t *testing.T) {
	// Content-Type is : application/x-www-form-urlencoded
	res, err := DefaultClient.POST("http://httpbin.org/post").
		SetBodyAsForm(H{
			"name": "wf",
			"age":  18,
		}).Do()

	if err != nil {
		t.Error("post failed", err)
	}

	defer res.Body.Close()

	if res.StatusCode != 200 {
		t.Error("Status Code not 200")
	}

	data, err := ioutil.ReadAll(res.Body)
	if nil != err {
		t.Error("read failed", err)
	}

	fmt.Println(string(data))
}

type Person struct {
	Name string
	Age  int
}

func TestHttpClientPostJson(t *testing.T) {
	// Content-Type is : application/json
	res, err := DefaultClient.POST("http://httpbin.org/post").
		SetBodyAsJson(H{
			"name": "wf",
			"age":  18,
		}).Do()

	// 支持对象类型
	//person := &Person{
	//	Name: "wf",
	//	Age:  18,
	//}
	//res, err := DefaultClient.POST("http://httpbin.org/post").SetJson(person).Do()

	// 支持字符串类型
	//jsonText := "{\"Name\":\"wf\",\"Age\":18}"
	//res, err := DefaultClient.POST("http://httpbin.org/post").SetJson(jsonText).Do()

	if err != nil {
		t.Error("post failed", err)
	}

	defer res.Body.Close()

	if res.StatusCode != 200 {
		t.Error("Status Code not 200")
	}

	data, err := ioutil.ReadAll(res.Body)
	if nil != err {
		t.Error("read failed", err)
	}

	fmt.Println(string(data))
}

func TestHttpClientPostMultipart(t *testing.T) {
	// Content-Type is : multipart/form-data
	res, err := DefaultClient.POST("http://httpbin.org/post").
		SetBodyAsForm(H{
			"name":   "wf",
			"age":    18,
			"photo":  FormFile("D:\\4B.jpg"),
			"resume": FormFile("D:\\resume.rtf"),
		}).Do()

	if err != nil {
		t.Error("post failed", err)
	}

	defer res.Body.Close()

	if res.StatusCode != 200 {
		t.Error("Status Code not 200")
	}

	data, err := ioutil.ReadAll(res.Body)
	if nil != err {
		t.Error("read failed", err)
	}

	fmt.Println(string(data))
}

func TestHttpClientDelete(t *testing.T) {
	str, err := DefaultClient.DELETE("http://httpbin.org/delete").DoForString()
	if err != nil {
		t.Error("delete failed", err)
	}

	println(str)
}

func TestHttpClientPutForm(t *testing.T) {
	// Content-Type is : application/x-www-form-urlencoded
	res, err := DefaultClient.PUT("http://httpbin.org/put").
		SetBodyAsForm(H{
			"name": "wf",
			"age":  18,
		}).Do()

	if err != nil {
		t.Error("put failed", err)
	}

	defer res.Body.Close()

	if res.StatusCode != 200 {
		t.Error("Status Code not 200")
	}

	data, err := ioutil.ReadAll(res.Body)
	if nil != err {
		t.Error("read failed", err)
	}

	fmt.Println(string(data))
}

func TestHttpClientPutJson(t *testing.T) {
	// Content-Type is : application/json
	res, err := DefaultClient.PUT("http://httpbin.org/put").
		SetBodyAsJson(H{
			"name": "wf",
			"age":  18,
		}).Do()

	// 支持对象类型
	//person := &Person{
	//	Name: "wf",
	//	Age:  18,
	//}
	//res, err := DefaultClient.POST("http://httpbin.org/post").SetJson(person).Do()

	// 支持字符串类型
	//jsonText := "{\"Name\":\"wf\",\"Age\":18}"
	//res, err := DefaultClient.POST("http://httpbin.org/post").SetJson(jsonText).Do()

	if err != nil {
		t.Error("post failed", err)
	}

	defer res.Body.Close()

	if res.StatusCode != 200 {
		t.Error("Status Code not 200")
	}

	data, err := ioutil.ReadAll(res.Body)
	if nil != err {
		t.Error("read failed", err)
	}

	fmt.Println(string(data))
}

func TestResources(t *testing.T) {
	entity, err := DefaultClient.
		AddHeader("Authorization","adc8620e5164462e854f6f2e4e33ee53").
		GET("http://localhost:8090/portal/users/admin/resources").
		DoForEntity()

	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("RespondText:",entity.DataAsString())
}
