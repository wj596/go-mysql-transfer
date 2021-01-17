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
package etcds

import (
	"context"
	"time"

	"github.com/juju/errors"
	"go.etcd.io/etcd/clientv3"
)

const _etcdOpsTimeout = 1 * time.Second

type Node struct {
	Key      string
	Value    []byte
	Revision int64
}

func CreateIfNecessary(key, val string, ops clientv3.KV, opts ...clientv3.OpOption) error {
	ctx, cancel := context.WithTimeout(context.Background(), _etcdOpsTimeout)
	defer cancel()

	_, err := ops.Txn(ctx).If(
		clientv3.Compare(clientv3.ModRevision(key), "=", 0),
	).Then(
		clientv3.OpPut(key, val, opts...),
	).Commit()

	if err != nil {
		return errors.Trace(err)
	}

	//if !resp.Succeeded {
	//	return errors.AlreadyExistsf("key %s in etcd", key)
	//}

	return nil
}

func Get(key string, ops clientv3.KV) ([]byte, int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), _etcdOpsTimeout)
	defer cancel()

	resp, err := ops.Get(ctx, key)
	if err != nil {
		return nil, -1, errors.Trace(err)
	}

	if len(resp.Kvs) == 0 {
		return nil, -1, errors.NotFoundf("key %s in etcd", key)
	}

	return resp.Kvs[0].Value, resp.Header.Revision, nil
}

func HasChildren(key string, ops clientv3.KV) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), _etcdOpsTimeout)
	defer cancel()

	resp, err := ops.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return false, errors.Trace(err)
	}

	length := len(key)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if len(key) > length {
			return true, nil
		}
	}

	return false, nil
}

func List(key string, ops clientv3.KV) (map[string]*Node, error) {
	ctx, cancel := context.WithTimeout(context.Background(), _etcdOpsTimeout)
	defer cancel()

	ret := make(map[string]*Node)

	resp, err := ops.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return ret, errors.Trace(err)
	}

	length := len(key)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if len(key) <= length {
			continue
		}
		node := &Node{
			Key:      key,
			Value:    kv.Value,
			Revision: kv.Version,
		}
		ret[key] = node
	}

	return ret, nil
}

func Save(key, val string, ops clientv3.KV, opts ...clientv3.OpOption) error {
	ctx, cancel := context.WithTimeout(context.Background(), _etcdOpsTimeout)
	defer cancel()

	resp, err := ops.Txn(ctx).If(
		clientv3.Compare(clientv3.ModRevision(key), ">", 0),
	).Then(
		clientv3.OpPut(key, val, opts...),
	).Commit()

	if err != nil {
		return errors.Trace(err)
	}

	if !resp.Succeeded {
		return errors.NotFoundf("key %s in etcd", key)
	}

	return nil
}

// UpdateOrCreate updates a key/value, if the key does not exist then create, or update
func UpdateOrCreate(key, val string, ops clientv3.KV, opts ...clientv3.OpOption) error {
	ctx, cancel := context.WithTimeout(context.Background(), _etcdOpsTimeout)
	defer cancel()

	_, err := ops.Do(ctx, clientv3.OpPut(key, val, opts...))
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func Delete(key string, ops clientv3.KV, opts ...clientv3.OpOption) error {
	ctx, cancel := context.WithTimeout(context.Background(), _etcdOpsTimeout)
	defer cancel()

	_, err := ops.Delete(ctx, key, opts...)

	if err != nil {
		return errors.Trace(err)
	}

	return nil
}
