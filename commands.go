package main

import (
	"strconv"

	"github.com/sybrexsys/RapidKV/datamodel"
)

type commandError string

func (conerr commandError) Error() string { return string(conerr) }

type databasefunc func(*Database, string, datamodel.DataArray) datamodel.CustomDataType

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
	"append": appendCommand,
	"decr":   decrCommand,
	"decrby": decrbyCommand,
	"get":    getCommand,
	"getset": getsetCommand,
	"incr":   incrCommand,
	"incrby": incrbyCommand,
	"mget":   mgetCommand,
	"mset":   msetCommand,
	"msetnx": msetnxCommand,
	"psetex": psetexCommand,
	"set":    setCommand,
	"setex":  setexCommand,
	"setnx":  setnxCommand,
	"strlen": strlenCommand,

	//hashes
	"hdel":    hdelCommand,
	"hexists": hexistsCommand,
	"hget":    hgetCommand,
	"hgetall": hgetallCommand,
	"hincrby": hincrbyCommand,
	"hkeys":   hkeysCommand,
	"hlen":    hlenCommand,
	"hmget":   hmgetCommand,
	"hmset":   hmsetCommand,
	"hset":    hsetCommand,
	"hsetnx":  hsetnxCommand,
	"hstrlen": hstrlenCommand,
	"hvals":   hvalsCommand,

	//lists

	"lindex":  lindexCommand,
	"linsert": linsertCommand,
	"llen":    llenCommand,
	"lpop":    lpopCommand,
	"lpush":   lpushCommand,
	"lpushx":  lpushxCommand,
	"lrange":  lrangeCommand,
	"lrem":    nil,
	"lset":    lsetCommand,
	"ltrim":   ltrimCommand,
	"rpop":    rpopCommand,
	"rpush":   rpushCommand,
	"rpushx":  rpushxCommand,

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

func getInt(command datamodel.DataArray, idx int) (int, error) {
	a := command.Get(idx)
	switch value := a.(type) {
	case datamodel.DataString:
		return strconv.Atoi(value.Get())
	case datamodel.DataInt:
		return value.Get(), nil
	default:
		return -1, commandError("Unknown parameter")
	}
}
