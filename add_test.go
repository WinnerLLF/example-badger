package main

import "testing"

func TestDBLink(t *testing.T) {
	Cdb.Dbpath = "./badger"
	db := DBLink()
	defer db.Close()

	t.Log(db)
}

func TestSingle_SetUpdate(t *testing.T) {
	key := "a"
	value := "a1"

	prtKey := &key
	prtValue := &value

	Cdb.Dbpath = "./badger"
	Cdb.Key = prtKey
	Cdb.Value = prtValue

	Single_SetUpdate()
	t.Log("新增数据成功!")
}

func TestSingle_EntryUpdate(t *testing.T) {
	key := "b"
	value := "b1"

	prtKey := &key
	prtValue := &value

	Cdb.Dbpath = "./badger"
	Cdb.Key = prtKey
	Cdb.Value = prtValue

	Single_EntryUpdate()
	t.Log("新增数据成功!")
}

func TestBatch_Data(t *testing.T) {
	var maplist = make(map[string]string, 0)
	vstring := [5]string{"f", "g", "c", "d", "e"}
	for key, value := range vstring {
		maplist[value] = vstring[key] + "1"
	}
	Cdb.Listmap = maplist
	Cdb.Dbpath = "./badger"

	Batch_Data()

	t.Log("新增数据和查询数据成功!")
}
