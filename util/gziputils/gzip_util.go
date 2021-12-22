package gziputils

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
)

func Zip(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)

	if _, err := zw.Write(data); err != nil {
		zw.Close() // close quiet
		return nil, err
	}

	if err := zw.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func UnZip(data []byte) ([]byte, error) {
	input := bytes.NewBuffer(data)
	reader, err := gzip.NewReader(input)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var output []byte
	output, err = ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	return output, nil
}
