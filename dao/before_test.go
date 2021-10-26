package dao

import (
	"testing"

	"github.com/juju/errors"

	"go-mysql-transfer/config"
	"go-mysql-transfer/util/snowflake"
)

func before(t *testing.T) {
	configFile := "D:\\newtransfers\\application.yml"
	if err := config.Initialize(configFile); err != nil {
		println(errors.ErrorStack(err))
		return
	}

	err := Initialize(config.GetIns())
	if err != nil {
		t.Fatal(err.Error())
	}

	snowflake.Initialize(1)
}
