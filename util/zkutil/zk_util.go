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
package zkutil

import (
	"github.com/samuel/go-zookeeper/zk"
)

func CreateDirIfNecessary(dir string, conn *zk.Conn) error {
	exist, _, err := conn.Exists(dir)
	if err != nil {
		return err
	}
	if !exist {
		_, err := conn.Create(dir, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	}

	return nil
}

func DeleteDir(dir string, conn *zk.Conn) error {
	_, stat, err := conn.Get(dir)
	if err != nil {
		return err
	}

	return conn.Delete(dir, stat.Version)
}
