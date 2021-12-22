package test

import (
	"fmt"
	"github.com/juju/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"testing"
)

var nfErr = errors.New("not found")

func test(a int) error {
	if a == 1 {
		return nfErr
	} else {
		return errors.New("sssssss")
	}

}

func ss(a int) int {
	switch a {
	case 1:
		return 1
	case 2:
		//if a==2{
		//	fmt.Println("sssssssss")
		//	return 99
		//}
		fmt.Println("dddddddddddddd")
		return 2
	}
	return 3
}



func TestErr(t *testing.T) {
	var model mongo.WriteModel

	fmt.Println(model)

	fmt.Println(ss(2))
}
