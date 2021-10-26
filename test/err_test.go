package test

import (
	"fmt"
	"github.com/juju/errors"
	"testing"
)

var nfErr = errors.New("not found")

func test(a int) error {
	if a==1{
		return nfErr
	}else{
		return errors.New("sssssss")
	}

}

func TestErr(t *testing.T) {
	err := test(2)
	if err!=nil{
		fmt.Println(err==nfErr)
	}
}
