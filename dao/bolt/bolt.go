package bolt

import (
	"encoding/binary"
	"fmt"
	"path/filepath"

	"github.com/juju/errors"
	"go.etcd.io/bbolt"

	"go-mysql-transfer/config"
	"go-mysql-transfer/util/fileutils"
)

const (
	_storePath        = "db"
	_fileMode         = 0600
	_metadataFileName = "metadata.db"
	_commitFileName   = "commit.db"
)

var (
	_mdb *bbolt.DB
	_cdb *bbolt.DB

	_sourceBucket   = []byte("source")
	_endpointBucket = []byte("endpoint")
	_pipelineBucket = []byte("pipeline")
	_ruleBucket     = []byte("rule")

	_commitBucket = []byte("commit")
)

func Initialize(config *config.AppConfig) error {
	storePath := filepath.Join(config.GetDataDir(), _storePath)
	if err := fileutils.MkdirIfNecessary(storePath); err != nil {
		return errors.New(fmt.Sprintf("create metadataFilePath : %s", err.Error()))
	}

	var err error

	metadataFilePath := filepath.Join(storePath, _metadataFileName)
	_mdb, err = bbolt.Open(metadataFilePath, _fileMode, bbolt.DefaultOptions)
	if err != nil {
		return errors.New(fmt.Sprintf("open boltdb: %s", err.Error()))
	}

	recordFilePath := filepath.Join(storePath, _commitFileName)
	_cdb, err = bbolt.Open(recordFilePath, _fileMode, bbolt.DefaultOptions)
	if err != nil {
		return errors.New(fmt.Sprintf("open boltdb: %s", err.Error()))
	}

	err = _mdb.Update(func(tx *bbolt.Tx) error {
		tx.CreateBucketIfNotExists(_sourceBucket)
		return nil
	})
	if err != nil {
		return errors.New(fmt.Sprintf("create bucket: %s", err.Error()))
	}

	err = _mdb.Update(func(tx *bbolt.Tx) error {
		tx.CreateBucketIfNotExists(_endpointBucket)
		return nil
	})
	if err != nil {
		return errors.New(fmt.Sprintf("create bucket: %s", err.Error()))
	}

	err = _mdb.Update(func(tx *bbolt.Tx) error {
		tx.CreateBucketIfNotExists(_pipelineBucket)
		return nil
	})
	if err != nil {
		return errors.New(fmt.Sprintf("create bucket: %s", err.Error()))
	}

	err = _mdb.Update(func(tx *bbolt.Tx) error {
		tx.CreateBucketIfNotExists(_ruleBucket)
		return nil
	})
	if err != nil {
		return errors.New(fmt.Sprintf("create bucket: %s", err.Error()))
	}

	err = _cdb.Update(func(tx *bbolt.Tx) error {
		tx.CreateBucketIfNotExists(_commitBucket)
		return nil
	})
	if err != nil {
		return errors.New(fmt.Sprintf("create bucket: %s", err.Error()))
	}

	return nil
}

func Close() {
	if _mdb != nil {
		_mdb.Close()
	}

	if _cdb != nil {
		_cdb.Close()
	}
}

func marshalId(id uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, id)
	return buf
}
