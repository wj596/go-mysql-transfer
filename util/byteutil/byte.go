/*
 * Copyright 2020-2021 the original author(https://github.com/wj596)
 *
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */
package byteutil

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
)

func StrToBytes(u string) []byte {
	return []byte(u)
}

func BytesToStr(u []byte) string {
	return string(u)
}

func Uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}

func Int64ToBytes(u int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(u))
	return buf
}

func BytesToUint64(b []byte) uint64 {
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

func BytesToInt64(b []byte) int64 {
	if b == nil {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}

func Uint8ToBytes(u uint8) ([]byte, error) {
	bytesBuffer := bytes.NewBuffer([]byte{})
	err := binary.Write(bytesBuffer, binary.BigEndian, &u)
	if err != nil {
		return nil, err
	}
	return bytesBuffer.Bytes(), nil
}

func BytesToUint8(b []byte) (uint8, error) {
	bytesBuffer := bytes.NewBuffer(b)
	var tmp uint8
	err := binary.Read(bytesBuffer, binary.BigEndian, &tmp)
	if err != nil {
		return 0, err
	}
	return tmp, nil
}

func BytesToUint32(b []byte) uint32 {
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}

func Uint32ToBytes(u uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, u)
	return buf
}

func JsonBytes(v interface{}) []byte {
	bytes, err := json.Marshal(v)
	if nil != err {
		return nil
	}
	return bytes
}
