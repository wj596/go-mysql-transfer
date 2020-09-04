package dateutil

import (
	"fmt"
	"testing"
)

func TestConvertGoFormat(t *testing.T) {

	fmt.Println(ConvertGoFormat("yyyy-MM-dd HH:mm:ss"))

	fmt.Println(ConvertGoFormat("yyyy-MM-dd"))

}
