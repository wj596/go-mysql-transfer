package bolt

import (
	"fmt"
	"go-mysql-transfer/util/stringutils"
	"testing"
)

func TestTransformRuleDaoImpl_SelectList(t *testing.T) {
	before(t)
	dao := new(TransformRuleDaoImpl)
	ls,err := dao.SelectList(0,0)
	if err != nil {
		t.Fatal(err.Error())
	}
	//for _, v := range ls {
	//	_conn.Update(func(tx *bbolt.Tx) error {
	//		bt := tx.Bucket(_ruleBucket)
	//		return bt.Delete(byteutil.Uint64ToBytes(v.Id))
	//	})
	//}
	fmt.Println(stringutils.ToJsonIndent(ls))
}
