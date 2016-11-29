package main

import (
	"strconv"

	"github.com/sybrexsys/RapidKV/database"
	"github.com/sybrexsys/RapidKV/datamodel"
)

type commandError string

func (conerr commandError) Error() string { return string(conerr) }

type databasefunc func(*database.Database, datamodel.DataArray) datamodel.CustomDataType

var commandList = map[string]databasefunc{
	//keys
	"del":       delCommand,
	"exists":    existsCommand,
	"expire":    expireCommand,
	"expireat":  expireatCommand,
	"keys":      keysCommand,
	"move":      moveCommand,
	"persist":   persistCommand,
	"pexpire":   pexpireCommand,
	"pexpireat": pexpireatCommand,
	"pttl":      pttlCommand,
	"rename":    renameCommand,
	"renamenx":  renamenxCommand,
	"ttl":       ttlCommand,
	"type":      typeCommand,

	//strings
	"append":      appendCommand,
	"bitcount":    nil,
	"bitfield":    nil,
	"bitop":       nil,
	"bitops":      nil,
	"decr":        nil,
	"decrby":      nil,
	"get":         getCommand,
	"getbit":      nil,
	"getrange":    nil,
	"getset":      nil,
	"incr":        nil,
	"incrby":      nil,
	"incrbyfloat": nil,
	"mget":        nil,
	"mset":        nil,
	"msetnx":      nil,
	"psetex":      nil,
	"set":         setCommand,
	"setbit":      nil,
	"setex":       nil,
	"setnx":       nil,
	"setrange":    nil,
	"strlen":      nil,

	//hashes
	"hdel":         nil,
	"hexists":      nil,
	"hget":         nil,
	"hgetall":      nil,
	"hincrby":      nil,
	"hincrbyfloat": nil,
	"hkeys":        nil,
	"hlen":         nil,
	"hmget":        nil,
	"hmset":        nil,
	"hset":         nil,
	"hsetnx":       nil,
	"hstrlen":      nil,
	"hvals":        nil,
	"hscan":        nil,

	//sets
	"sadd":        nil,
	"scard":       nil,
	"sdiff":       nil,
	"sdifstore":   nil,
	"sinter":      nil,
	"sinterstore": nil,
	"sismember":   nil,
	"smembers":    nil,
	"smove":       nil,
	"spop":        nil,
	"srandmember": nil,
	"srem":        nil,
	"sunion":      nil,
	"sunionstore": nil,
	"sscan":       nil,
}

func getKey(command datamodel.DataArray, idx int) (string, error) {
	a := command.Get(idx)
	switch value := a.(type) {
	case datamodel.DataString:
		return value.Get(), nil
	case datamodel.DataInt:
		return strconv.Itoa(value.Get()), nil
	default:
		return "", commandError("Unknown parameter")
	}

}
