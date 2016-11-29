package main

import (
	"strings"

	"github.com/sybrexsys/RapidKV/datamodel"
	"strconv"
)

func getCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	val, isval := db.GetValue(key)
	if !isval {
		return datamodel.CreateNull()
	}
	_, okstr := val.(datamodel.DataString)
	if !okstr {
		return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return val.Copy()
}

func mgetCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	cnt := command.Count()
	arr := datamodel.CreateArray(cnt - 1)
	for i := 1; i < cnt; i++ {
		key, err := getKey(command, i)
		if err != nil {
			arr.Add(datamodel.CreateNull())
			continue
		}
		val, isval := db.GetValue(key)
		if !isval {
			arr.Add(datamodel.CreateNull())
			continue
		}
		_, okstr := val.(datamodel.DataString)
		if !okstr {
			arr.Add(datamodel.CreateNull())
			continue
		}
		arr.Add(val.Copy())
	}
	return arr
}

func setCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	value, err := getKey(command, 2)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	if command.Count() == 3 {
		db.SetValue(key, datamodel.CreateString(value), SetAny, 0)
		return datamodel.CreateSimpleString("OK")
	}
	var (
		setIfExist    int
		setIfNotExist int
		timeoutEx     int
		timeoutPx     int
	)

	for i := 3; i < command.Count(); i++ {
		option, err := getKey(command, i)
		if err != nil {
			return datamodel.CreateError("ERR Unknown parameter")
		}
		option = strings.ToLower(option)
		if option == "xx" {
			setIfExist = SetIfExists
		} else if option == "nx" {
			setIfNotExist = SetIfNotExists
		} else if option == "ex" {
			if i+1 == command.Count() {
				return datamodel.CreateError("ERR Syntax error")
			}
			val := command.Get(i + 1)
			z, ok := val.(datamodel.DataInt)
			if !ok {
				return datamodel.CreateError("ERR Syntax error")
			}
			timeoutEx = z.Get() * 1000
			if timeoutEx <= 0 {
				return datamodel.CreateError("ERR Syntax error")
			}
			i++
		} else if option == "px" {
			if i+1 == command.Count() {
				return datamodel.CreateError("ERR Syntax error")
			}
			val := command.Get(i + 1)
			z, ok := val.(datamodel.DataInt)
			if !ok {
				return datamodel.CreateError("ERR Syntax error")
			}
			timeoutPx = z.Get()
			if timeoutPx <= 0 {
				return datamodel.CreateError("ERR Syntax error")
			}
			i++
		} else {
			return datamodel.CreateError("ERR Unknown parameter")
		}
	}
	if setIfExist > 0 && setIfNotExist > 0 {
		return datamodel.CreateError("ERR Syntax error")
	}
	if timeoutEx > 0 && timeoutPx > 0 {
		return datamodel.CreateError("ERR Syntax error")
	}
	_, res := db.SetValue(key, datamodel.CreateString(value), setIfExist+setIfNotExist, timeoutEx+timeoutPx)
	if !res {
		return datamodel.CreateNull()
	}
	return datamodel.CreateSimpleString("OK")
}

func msetCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	db.RUnlock()
	db.Lock()
	defer func() {
		db.Unlock()
		db.RLock()
	}()
	if command.Count()&1 != 1 {
		return datamodel.CreateError("ERR Wrong count of the parameters")
	}
	for i := 1; i < command.Count(); i += 2 {
		key, err := getKey(command, i)
		if err != nil {
			return datamodel.CreateError("ERR Unknown parameter")
		}
		value, err := getKey(command, i+1)
		if err != nil {
			return datamodel.CreateError("ERR Unknown parameter")
		}
		db.SetValue(key, datamodel.CreateString(value), SetAny, 0)
	}
	return datamodel.CreateSimpleString("OK")
}

func msetnxCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	db.RUnlock()
	db.Lock()
	defer func() {
		db.Unlock()
		db.RLock()
	}()
	all := 1
	if command.Count()&1 != 1 {
		return datamodel.CreateError("ERR Wrong count of the parameters")
	}
	for i := 1; i < command.Count(); i += 2 {
		key, err := getKey(command, i)
		if err != nil {
			return datamodel.CreateError("ERR Unknown parameter")
		}
		value, err := getKey(command, i+1)
		if err != nil {
			return datamodel.CreateError("ERR Unknown parameter")
		}
		_, setted := db.SetValue(key, datamodel.CreateString(value), SetIfNotExists, 0)
		if !setted {
			all = 0
		}
	}
	return datamodel.CreateInt(all)
}

func appendCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	value, err := getKey(command, 2)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	return db.ProcessValue(key, true, func(elem *Element) (datamodel.CustomDataType, bool) {
		if elem.Value == nil {
			elem.Value = datamodel.CreateString(value)
			return datamodel.CreateInt(len(value)), true
		}
		val, ok := elem.Value.(datamodel.DataString)
		if !ok {
			return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value"), false
		}
		str := val.Get() + value
		val.Set(str)
		return datamodel.CreateInt(len(str)), true
	})
}

func setexCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	timeout := command.Get(2)
	z, ok := timeout.(datamodel.DataInt)
	if !ok {
		return datamodel.CreateError("ERR Syntax error")
	}
	timeoutEx := z.Get() * 1000
	if timeoutEx <= 0 {
		return datamodel.CreateError("ERR Syntax error")
	}
	value, err := getKey(command, 3)
	if err != nil {
		return datamodel.CreateError("ERR Syntax error")
	}
	db.SetValue(key, datamodel.CreateString(value), SetAny, timeoutEx)
	return datamodel.CreateSimpleString("OK")
}

func psetexCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	timeout := command.Get(2)
	z, ok := timeout.(datamodel.DataInt)
	if !ok {
		return datamodel.CreateError("ERR Syntax error")
	}
	timeoutEx := z.Get()
	if timeoutEx <= 0 {
		return datamodel.CreateError("ERR Syntax error")
	}
	value, err := getKey(command, 3)
	if err != nil {
		return datamodel.CreateError("ERR Syntax error")
	}
	db.SetValue(key, datamodel.CreateString(value), SetAny, timeoutEx)
	return datamodel.CreateSimpleString("OK")
}

func setnxCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	value, err := getKey(command, 2)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	_, setted := db.SetValue(key, datamodel.CreateString(value), SetIfNotExists, 0)
	if !setted {
		return datamodel.CreateInt(0)
	}
	return datamodel.CreateInt(1)
}

func getsetCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	value, err := getKey(command, 2)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	return db.ProcessValue(key, true, func(elem *Element) (datamodel.CustomDataType, bool) {
		if elem.Value == nil {
			elem.Value = datamodel.CreateString(value)
			return datamodel.CreateNull(), true
		}
		_, ok := elem.Value.(datamodel.DataString)
		if !ok {
			return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value"), false
		}
		result := elem.Value
		elem.Value = datamodel.CreateString(value)
		return result, true
	})
}

func ariphmeticCommand(db *Database, key string, value int) datamodel.CustomDataType {
	return db.ProcessValue(key, true, func(elem *Element) (datamodel.CustomDataType, bool) {
		start := 0
		if elem.Value == nil {
			elem.Value = datamodel.CreateString(strconv.Itoa(value))
			return datamodel.CreateInt(value), true
		}
		val, ok := elem.Value.(datamodel.DataString)
		if !ok {
			return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value"), false
		}
		var err error
		start, err = strconv.Atoi(val.Get())
		if err != nil {
			return datamodel.CreateError(" ERR value is not an integer or out of range"), false
		}
		start += value
		val.Set(strconv.Itoa(start))
		return datamodel.CreateInt(start), true
	})
}

func incrCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	return ariphmeticCommand(db, key, 1)
}

func decrCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	return ariphmeticCommand(db, key, -1)
}

func incrbyCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	val := command.Get(2)
	c, isInt := val.(datamodel.DataInt)
	if !isInt {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	return ariphmeticCommand(db, key, c.Get())
}

func decrbyCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	val := command.Get(2)
	c, isInt := val.(datamodel.DataInt)
	if !isInt {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	return ariphmeticCommand(db, key, -c.Get())
}

func strlenCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	val, isval := db.GetValue(key)
	if !isval {
		return datamodel.CreateInt(0)
	}
	str, okstr := val.(datamodel.DataString)
	if !okstr {
		return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return datamodel.CreateInt(len(str.Get()))
}
