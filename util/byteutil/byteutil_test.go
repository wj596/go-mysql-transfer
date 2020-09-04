package byteutil

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"testing"
)

func TestGetBytes(t *testing.T) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(true)
	println(buf.Bytes())
	println(len(buf.Bytes()))
	println(string(buf.Bytes()))

	buf2 := bytes.NewBuffer([]byte{})
	binary.Write(buf2, binary.BigEndian, true)
	println(buf2.Bytes())
	println(len(buf2.Bytes()))
}

func TestCopyBytes(t *testing.T) {
	buf2 := bytes.NewBuffer([]byte{})
	binary.Write(buf2, binary.BigEndian, true)
	a := buf2.Bytes()
	fmt.Println(a)
	b := a
	fmt.Println(b)
	a = nil
	fmt.Println(a)
	fmt.Println(b)
}
