package main

import "testing"

func TestDelete_NewTranData(t *testing.T) {
	deleteKey := "a"
	prtKey := &deleteKey

	Cdb.Delete = prtKey
	Cdb.Dbpath = "./badger"
	Delete_NewTranData()
	t.Log("删除数据成功!")
}

func TestBatch_PrefixDelete(t *testing.T) {
	deleteKey := "key"
	prtKey := &deleteKey

	Cdb.Delete = prtKey
	Cdb.Dbpath = "./badger"

	Batch_PrefixDelete()
	t.Log("批量删除前缀数据成功!")
}
