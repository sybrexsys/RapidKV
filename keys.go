package main

import (
	"github.com/sybrexsys/RapidKV/datamodel"
)

func keysCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	return db.GetKeys(key)
}

func typeCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
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

func delCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	deleted := 0
	if db.Del(key) {
		deleted++
	}
	cnt := command.Count()
	for i := 0; i < cnt; i++ {
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

func existsCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	exists := 0
	if _, ok := db.GetValue(key); ok {
		exists++
	}
	cnt := command.Count()
	for i := 0; i < cnt; i++ {
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

func renameCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	newkey, err := getKey(command, 0)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	_, proc := db.Rename(key, newkey, true)
	if !proc {
		return datamodel.CreateError("ERR no such key")
	}
	return datamodel.CreateSimpleString("OK")
}

func renamenxCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	newkey, err := getKey(command, 0)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	res, proc := db.Rename(key, newkey, false)
	if !proc {
		return datamodel.CreateError("ERR no such key")
	}
	return datamodel.CreateInt(res)
}

func persistCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	if db.SkipTTL(key) {
		return datamodel.CreateInt(1)
	}
	return datamodel.CreateInt(0)
}

func ttlCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	ttl, exists := db.GetTTL(key)
	if !exists {
		return datamodel.CreateInt(-2)
	}
	if ttl == -1 {
		return datamodel.CreateInt(-1)
	}
	return datamodel.CreateInt(ttl / 1000)
}

func pttlCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	ttl, exists := db.GetTTL(key)
	if !exists {
		return datamodel.CreateInt(-2)
	}
	if ttl == -1 {
		return datamodel.CreateInt(-1)
	}
	return datamodel.CreateInt(ttl)
}

func moveCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	database, err := getInt(command, 0)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	todb := getDataBase(database)
	if db.Move(key, todb) {
		return datamodel.CreateInt(1)
	}
	return datamodel.CreateInt(0)
}

func expireCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	ttl, err := getInt(command, 0)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
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

func pexpireCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	ttl, err := getInt(command, 0)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
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

func expireatCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	ttl, err := getInt(command, 0)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
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

func pexpireatCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	ttl, err := getInt(command, 0)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
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
