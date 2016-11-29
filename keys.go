package main

import (
	"github.com/sybrexsys/RapidKV/database"
	"github.com/sybrexsys/RapidKV/datamodel"
)

func keysCommand(db *database.Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	return db.GetKeys(key)
}

func typeCommand(db *database.Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	val, isval := db.GetValue(key)
	if !isval {
		return datamodel.CreateSimpleString("none")
	}
	switch val.(type) {
	case datamodel.DataString:
		return datamodel.CreateSimpleString("string")
	case datamodel.DataArray:
		return datamodel.CreateSimpleString("list")
	case datamodel.DataDictionary:
		return datamodel.CreateSimpleString("hash")
	default:
		return datamodel.CreateSimpleString("none")
	}
}

func delCommand(db *database.Database, command datamodel.DataArray) datamodel.CustomDataType {
	deleted := 0
	cnt := command.Count()
	for i := 1; i < cnt; i++ {
		key, err := getKey(command, i)
		if err != nil {
			return datamodel.CreateError("ERR Unknown parameter")
		}
		if db.Del(key) {
			deleted++
		}
	}
	return datamodel.CreateInt(deleted)
}

func existsCommand(db *database.Database, command datamodel.DataArray) datamodel.CustomDataType {
	exists := 0
	cnt := command.Count()
	for i := 1; i < cnt; i++ {
		key, err := getKey(command, i)
		if err != nil {
			return datamodel.CreateError("ERR Unknown parameter")
		}
		if _, ok := db.GetValue(key); ok {
			exists++
		}
	}
	return datamodel.CreateInt(exists)
}

func renameCommand(db *database.Database, command datamodel.DataArray) datamodel.CustomDataType {
	oldkey, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	newkey, err := getKey(command, 2)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	_, proc := db.Rename(oldkey, newkey, true)
	if !proc {
		return datamodel.CreateError("ERR no such key")
	}
	return datamodel.CreateSimpleString("OK")
}

func renamenxCommand(db *database.Database, command datamodel.DataArray) datamodel.CustomDataType {
	oldkey, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	newkey, err := getKey(command, 2)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	res, proc := db.Rename(oldkey, newkey, false)
	if !proc {
		return datamodel.CreateError("ERR no such key")
	}
	return datamodel.CreateInt(res)
}

func persistCommand(db *database.Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	if db.SkipTTL(key) {
		return datamodel.CreateInt(1)
	}
	return datamodel.CreateInt(0)
}

func ttlCommand(db *database.Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	ttl, exists := db.GetTTL(key)
	if !exists {
		return datamodel.CreateInt(-2)
	}
	if ttl == -1 {
		return datamodel.CreateInt(-1)
	}
	return datamodel.CreateInt(ttl / 1000)
}

func pttlCommand(db *database.Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	ttl, exists := db.GetTTL(key)
	if !exists {
		return datamodel.CreateInt(-2)
	}
	if ttl == -1 {
		return datamodel.CreateInt(-1)
	}
	return datamodel.CreateInt(ttl)
}

func moveCommand(db *database.Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}

	database, ok := command.Get(2).(datamodel.DataInt)
	if !ok {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	todb := getDataBase(database.Get())
	if db.Move(key, todb) {
		return datamodel.CreateInt(1)
	}
	return datamodel.CreateInt(0)
}

func expireCommand(db *database.Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	ttld, ok := command.Get(2).(datamodel.DataInt)
	if !ok {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	ttl := ttld.Get()
	if ttl < 0 {
		res := db.Del(key)
		if res {
			return datamodel.CreateInt(1)
		}
		return datamodel.CreateInt(0)
	}
	res := db.SetTTL(key, ttl*1000)
	if res {
		return datamodel.CreateInt(1)
	}
	return datamodel.CreateInt(0)
}

func pexpireCommand(db *database.Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	ttld, ok := command.Get(2).(datamodel.DataInt)
	if !ok {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	ttl := ttld.Get()
	if ttl < 0 {
		res := db.Del(key)
		if res {
			return datamodel.CreateInt(1)
		}
		return datamodel.CreateInt(0)
	}
	res := db.SetTTL(key, ttl)
	if res {
		return datamodel.CreateInt(1)
	}
	return datamodel.CreateInt(0)
}

func expireatCommand(db *database.Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	ttld, ok := command.Get(2).(datamodel.DataInt)
	if !ok {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	ttl := ttld.Get()
	if ttl < 0 {
		res := db.Del(key)
		if res {
			return datamodel.CreateInt(1)
		}
		return datamodel.CreateInt(0)
	}
	res := db.SetTTL(key, -ttl*1000)
	if res {
		return datamodel.CreateInt(1)
	}
	return datamodel.CreateInt(0)
}

func pexpireatCommand(db *database.Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	ttld, ok := command.Get(2).(datamodel.DataInt)
	if !ok {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	ttl := ttld.Get()
	if ttl < 0 {
		res := db.Del(key)
		if res {
			return datamodel.CreateInt(1)
		}
		return datamodel.CreateInt(0)
	}
	res := db.SetTTL(key, ttl)
	if res {
		return datamodel.CreateInt(1)
	}
	return datamodel.CreateInt(0)
}
