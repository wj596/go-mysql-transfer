package test

import (
	"fmt"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"go-mysql-transfer/util/sysutils"
	"testing"
)

type testHandler struct {
}

func (h *testHandler) OnRotate(e *replication.RotateEvent) error {
	fmt.Println("OnRotate", string(e.NextLogName), uint32(e.Position))
	return nil
}

func (h *testHandler) OnTableChanged(schema, table string) error {
	fmt.Println("OnTableChanged", schema, table)
	return nil
}

func (h *testHandler) OnDDL(nextPos mysql.Position, _ *replication.QueryEvent) error {
	fmt.Println("OnDDL", nextPos.Name, nextPos.Pos)
	return nil
}

func (h *testHandler) OnXID(nextPos mysql.Position) error {
	fmt.Println("OnXID", nextPos.Name, nextPos.Pos)
	return nil
}

func (h *testHandler) OnRow(e *canal.RowsEvent) error {
	curr, _ := c.GetMasterPos()
	fmt.Println("OnRow", curr.Name, curr.Pos, e.Table.Name, e.Action, e.Rows)
	return nil
}

func (h *testHandler) OnGTID(gtid mysql.GTIDSet) error {
	fmt.Println("OnGTID")
	return nil
}

func (h *testHandler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	fmt.Println("OnPosSynced")
	return nil
}

func (h *testHandler) String() string {
	return "testHandler"
}

var c *canal.Canal

func TestCanal(t *testing.T) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = "127.0.0.1:3306"
	cfg.User = "root"
	cfg.Password = "123456"
	cfg.Charset = "utf8"
	cfg.Flavor = "mysql"
	cfg.ServerID = 1001
	cfg.Dump.ExecutionPath = ""
	cfg.Dump.DiscardErr = false
	cfg.Dump.SkipMasterData = false
	cfg.IncludeTableRegex = append(cfg.IncludeTableRegex, "eseap"+"\\."+"t_user")

	var err error
	c, err = canal.NewCanal(cfg)
	if err != nil {
		fmt.Println(err)
		return
	}

	//c.AddDumpDatabases("eseap")
	//c.AddDumpTables("eseap", "t_types")
	c.SetEventHandler(new(testHandler))
	c.RunFrom(mysql.Position{
		Name: "",
		Pos:  0,
	})
	// c.Close()

	sysutils.WaitCloseSignals()

}

func TestCanal2(t *testing.T) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = "192.168.44.113:3306"
	cfg.User = "root"
	cfg.Password = "root"
	cfg.Charset = "utf8"
	cfg.Flavor = "mysql"
	cfg.ServerID = 1001
	cfg.Dump.ExecutionPath = ""
	cfg.Dump.DiscardErr = false
	cfg.Dump.SkipMasterData = false
	//cfg.IncludeTableRegex = append(cfg.IncludeTableRegex, "eseap"+"\\."+"t_types")

	var err error
	c, err = canal.NewCanal(cfg)
	if err != nil {
		fmt.Println(err)
		return
	}

	res, _ := c.Execute(fmt.Sprintf("select count(1) from %s", "eseap.t_user"))

	totalRow, _ := res.GetInt(0, 0)
	fmt.Println(totalRow)

}
