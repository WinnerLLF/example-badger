package main

import (
	"flag"
	"fmt"
	DB "github.com/dgraph-io/badger"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	Cdb    ContentDB
	DbBase *DB.DB

	dataview     string
	dataviewlist map[string]string
	pathurl      = "./badger"
	maplist      = make(map[string]string)
	keystr       = flag.String("k", "test", "Test the key")
	valuestr     = flag.String("v", "test value", "Test the value")
	readdata     = flag.String("r", "test", "Read the data")
	check        = flag.String("c", "set", "Select the method to execute")
	deletdata    = flag.String("d", "delete", "Delete the data")
)

const (
	dbmsg = "数据库链接失败"
	cxmsg = "查询数据失败"
	xzmsg = "新增或者更新数据失败"
	tjmsg = "提交事务失败"
	clmsg = "处理查询的数据信息失败"
	dcmsg = "删除失败"
	xtmsg = "系统错误"
	gbmsh = "关闭数据库失败"
)

type ContentDB struct {
	Key     *string           // key键
	Value   *string           // value键值
	Read    *string           // 读取key
	Listmap map[string]string // 批量操作
	Dbpath  string            // DB路径
	Check   *string           // 选择操作对象
	Delete  *string           // 删除操作键值/或者模糊删除
}

func init() {
	vstring := [5]string{"a", "b", "c", "d", "e"}
	for key, value := range vstring {
		maplist[value] = vstring[key] + "1"
	}
}
func Management() {
	Cdb.Dbpath = pathurl
	Cdb.Key = keystr
	Cdb.Value = valuestr
	Cdb.Listmap = maplist
	Cdb.Read = readdata
	Cdb.Delete = deletdata
	Cdb.Check = check
}

func main() {
	flag.Parse()

	Management()
	// TODO badgerDB实例
	switch *check {
	case "set":
		Single_SetUpdate()
	case "entry":
		Single_EntryUpdate()
	case "entrylist":
		Batch_Data()
	case "view":
		dataview = Single_View_Data()
	case "viewlist":
		dataviewlist = Viewkes_Data()
	case "prefix":
		dataviewlist = Prefix_ViewData()
	case "delete":
		Delete_NewTranData()
	case "deletelist":
		Batch_Delete()
	case "deleteprefix":
		Batch_PrefixDelete()
	case "gc":
		CollatingDocuments()
	case "stream":
		//StreamData()
	default:
	}

	// TODO 处理查询返回结果
	fmt.Println(dataview)
	for key, value := range dataviewlist {
		fmt.Printf("key=%s, value=%s\n", key, value)
	}
}

//TODO DataBase Manage
func DBLink() *DB.DB {
	db, err := DB.Open(DB.DefaultOptions(Cdb.Dbpath))
	CheckError(dbmsg, err)

	return db
}

//TODO Add or Update DB
func Single_SetUpdate() {
	db := DBLink()
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()
	CheckError(xzmsg, txn.Set([]byte(*Cdb.Key), []byte(*Cdb.Value)))
	CheckError(tjmsg, txn.Commit())
}
func Single_EntryUpdate() {
	db := DBLink()
	defer db.Close()

	CheckError(xzmsg, db.Update(func(txn *DB.Txn) error {
		e := DB.NewEntry([]byte(*Cdb.Key), []byte(*Cdb.Value))
		err := txn.SetEntry(e)
		CheckError(xzmsg, err)
		return err
	}))
}
func Batch_Data() {
	db := DBLink()
	defer db.Close()

	var vallist [][]byte
	var wg sync.WaitGroup
	N := 10
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key%d", i))
			CheckError(xzmsg, db.Update(func(tx *DB.Txn) error {
				return tx.SetEntry(DB.NewEntry(key, key))
			}))
			CheckError(xtmsg, db.View(func(tx *DB.Txn) error {
				item, err := tx.Get(key)
				CheckError(cxmsg, err)
				val, err := item.ValueCopy(nil)
				CheckError(cxmsg, err)
				vallist = append(vallist, val)
				return nil
			}))
		}(i)
	}
	wg.Wait()
}

//TODO Delete DB
func Delete_NewTranData() {
	db := DBLink()
	defer db.Close()

	del := *Cdb.Delete
	if strings.Contains(del, ",") {
		dellist := strings.Split(del, ",")
		for _, value := range dellist {
			txn := db.NewTransaction(true)
			defer txn.Discard()
			CheckError(dcmsg, txn.Delete([]byte(value)))
			CheckError(dcmsg, txn.Commit())
		}
	} else {
		txn := db.NewTransaction(true)
		defer txn.Discard()
		CheckError(dcmsg, txn.Delete([]byte(del)))
		CheckError(dcmsg, txn.Commit())
	}
}
func Batch_Delete() {
	db := DBLink()
	defer db.Close()

	del := *Cdb.Delete
	err := db.Update(func(txn *DB.Txn) error {
		if strings.Contains(del, ",") {
			dellist := strings.Split(del, ",")
			for _, value := range dellist {
				CheckError(dcmsg, txn.Delete([]byte(value)))
			}
		}
		return nil
	})
	CheckError(dcmsg, err)
}
func Batch_PrefixDelete() {
	db := DBLink()
	defer db.Close()

	iterOpt := DB.DefaultIteratorOptions
	iterOpt.PrefetchValues = false
	txn := db.NewTransaction(false)
	idxIt := txn.NewIterator(iterOpt)
	defer idxIt.Close()

	count := 0
	txn2 := db.NewTransaction(true)
	prefix := []byte(*Cdb.Delete)
	for idxIt.Seek(prefix); idxIt.ValidForPrefix(prefix); idxIt.Next() {
		key := idxIt.Item().Key()
		count++
		newKey := make([]byte, len(key))
		copy(newKey, key)
		CheckError(dcmsg, txn2.Delete(newKey))
	}
	fmt.Println("删除数据:" + strconv.Itoa(count) + "条")
	CheckError(dcmsg, txn2.Commit())

}

//TODO Select DB
func Single_View_Data() string {
	db := DBLink()
	defer db.Close()

	var valdata string
	derr := db.View(func(txn *DB.Txn) error {
		item, terr := txn.Get([]byte(*Cdb.Read))
		CheckError(cxmsg, terr)
		var valCopy []byte
		err := item.Value(func(val []byte) error {
			valCopy = append([]byte{}, val...)
			return nil
		})
		CheckError(clmsg, err)
		valCopy, err = item.ValueCopy(nil)
		valdata = string(valCopy)
		return nil
	})
	CheckError(cxmsg, derr)

	return valdata
}
func Viewkes_Data() map[string]string {
	db := DBLink()
	defer db.Close()

	mapvalue := make(map[string]string)
	err := db.View(func(txn *DB.Txn) error {
		opts := DB.DefaultIteratorOptions
		opts.PrefetchSize = 10
		opts.PrefetchValues = true // false | true
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			var err error
			item := it.Item()
			k := item.Key()
			if !opts.PrefetchValues {
				mapvalue[string(k)] = ""
			} else {
				var keyc, valuec string
				err = item.Value(func(v []byte) error {
					keyc = string(k)
					valuec = string(v)
					return nil
				})
				mapvalue[keyc] = valuec
			}
			if err != nil {
				return err
			}
		}
		return nil
	})

	CheckError(cxmsg, err)

	return mapvalue
}
func Prefix_ViewData() map[string]string {
	db := DBLink()
	defer db.Close()

	mapvalue := make(map[string]string)
	derr := db.View(func(txn *DB.Txn) error {
		it := txn.NewIterator(DB.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(*Cdb.Read)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			var keyc, valuec string
			err := item.Value(func(v []byte) error {
				keyc = string(k)
				valuec = string(v)
				return nil
			})
			mapvalue[keyc] = valuec
			if err != nil {
				return err
			}
		}
		return nil
	})
	CheckError(cxmsg, derr)

	return mapvalue
}

//TODO Gc Manage
func CollatingDocuments() {
	db := DBLink()
	defer db.Close()

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
	again:
		err := db.RunValueLogGC(0.5)
		if err == nil {
			goto again
		}
	}
}

//TODO Stream Manage
func StreamData() {

}

//TODO Error处理
func CheckError(msg string, err error) {
	if err != nil {
		fmt.Println(msg + err.Error())
	}
}
