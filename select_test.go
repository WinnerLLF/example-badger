package main

import "testing"

func TestSingle_View_Data(t *testing.T) {
	readValue := "b"
	ptrRead := &readValue

	Cdb.Read = ptrRead
	Cdb.Dbpath = "./badger"

	svd := Single_View_Data()

	t.Log(svd)
}

func TestViewkes_Data(t *testing.T) {
	Cdb.Dbpath = "./badger"

	vData := Viewkes_Data()
	for key, value := range vData {
		t.Log("数据键值对：" + key + "-" + value)
	}
}

func TestPrefix_ViewData(t *testing.T) {
	readKey := "key"
	prtRead := &readKey

	Cdb.Dbpath = "./badger"
	Cdb.Read = prtRead
	pfvData := Prefix_ViewData()
	for key, value := range pfvData {
		t.Log("数据键值对：" + key + "-" + value)
	}
}
