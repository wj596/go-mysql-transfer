package bolt

import (
	"fmt"
	"path/filepath"

	"github.com/juju/errors"
	"go.etcd.io/bbolt"

	"go-mysql-transfer/config"
	"go-mysql-transfer/util/fileutils"
)

const (
	_storePath = "db"
	_fileMode  = 0600
	_fileName  = "data.db"
)

var (
	_conn           *bbolt.DB
	_sourceBucket   = []byte("source_bucket")
	_endpointBucket = []byte("endpoint_bucket")
	_pipelineBucket = []byte("pipeline_bucket")
	_ruleBucket     = []byte("rule_bucket")
)

func Initialize(config *config.AppConfig) error {
	storePath := filepath.Join(config.GetDataDir(), _storePath)
	if err := fileutils.MkdirIfNecessary(storePath); err != nil {
		return errors.New(fmt.Sprintf("create metadataFilePath : %s", err.Error()))
	}

	filePath := filepath.Join(storePath, _fileName)
	conn, err := bbolt.Open(filePath, _fileMode, bbolt.DefaultOptions)
	if err != nil {
		return errors.New(fmt.Sprintf("open boltdb: %s", err.Error()))
	}

	err = conn.Update(func(tx *bbolt.Tx) error {
		tx.CreateBucketIfNotExists(_sourceBucket)
		return nil
	})
	if err != nil {
		return errors.New(fmt.Sprintf("create bucket: %s", err.Error()))
	}

	err = conn.Update(func(tx *bbolt.Tx) error {
		tx.CreateBucketIfNotExists(_endpointBucket)
		return nil
	})
	if err != nil {
		return errors.New(fmt.Sprintf("create bucket: %s", err.Error()))
	}

	err = conn.Update(func(tx *bbolt.Tx) error {
		tx.CreateBucketIfNotExists(_pipelineBucket)
		return nil
	})
	if err != nil {
		return errors.New(fmt.Sprintf("create bucket: %s", err.Error()))
	}

	err = conn.Update(func(tx *bbolt.Tx) error {
		tx.CreateBucketIfNotExists(_ruleBucket)
		return nil
	})
	if err != nil {
		return errors.New(fmt.Sprintf("create bucket: %s", err.Error()))
	}

	_conn = conn
	return nil

}

func Close() {

	if _conn != nil {
		_conn.Close()
	}

}
