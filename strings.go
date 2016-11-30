package main

import (
	"strings"

	"strconv"

	"github.com/sybrexsys/RapidKV/datamodel"
)

func getCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	return db.GetValueAndProcess(key, func(val datamodel.CustomDataType, isval bool) datamodel.CustomDataType {
		if !isval {
			return datamodel.CreateNull()
		}
		_, okstr := val.(datamodel.DataString)
		if !okstr {
			return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return val.Copy()
	})
}

func mgetCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	cnt := command.Count()
	arr := datamodel.CreateArray(cnt + 1)
	for i := -1; i < cnt; i++ {
		if i >= 0 {
			var err error
			key, err = getKey(command, i)
			if err != nil {
				arr.Add(datamodel.CreateNull())
				continue
			}
		}
		val, isval := db.GetValueCopy(key)
		if !isval {
			arr.Add(datamodel.CreateNull())
			continue
		}
		_, okstr := val.(datamodel.DataString)
		if !okstr {
			arr.Add(datamodel.CreateNull())
			continue
		}
		arr.Add(val)
	}
	return arr
}

func setCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {

	value, err := getKey(command, 0)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	if command.Count() == 1 {
		db.SetValue(key, datamodel.CreateString(value), SetAny, 0)
		return datamodel.CreateSimpleString("OK")
	}
	var (
		setIfExist    int
		setIfNotExist int
		timeoutEx     int
		timeoutPx     int
	)

	for i := 1; i < command.Count(); i++ {
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
			z, err := getInt(command, i+1)
			if err != nil {
				return datamodel.CreateError("ERR Syntax error")
			}
			timeoutEx = z * 1000
			if timeoutEx <= 0 {
				return datamodel.CreateError("ERR Syntax error")
			}
			i++
		} else if option == "px" {
			if i+1 == command.Count() {
				return datamodel.CreateError("ERR Syntax error")
			}
			z, err := getInt(command, i+1)
			if err != nil {
				return datamodel.CreateError("ERR Syntax error")
			}
			timeoutPx = z
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

func msetCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	db.RUnlock()
	db.Lock()
	defer func() {
		db.Unlock()
		db.RLock()
	}()
	if command.Count()&1 != 1 {
		return datamodel.CreateError("ERR Wrong count of the parameters")
	}
	for i := -1; i < command.Count(); i += 2 {
		if i > 0 {
			var err error
			key, err = getKey(command, i)
			if err != nil {
				return datamodel.CreateError("ERR Unknown parameter")
			}
		}
		value, err := getKey(command, i+1)
		if err != nil {
			return datamodel.CreateError("ERR Unknown parameter")
		}
		db.SetValue(key, datamodel.CreateString(value), SetAny, 0)
	}
	return datamodel.CreateSimpleString("OK")
}

func msetnxCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
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
	for i := -1; i < command.Count(); i += 2 {
		if i > 0 {
			var err error
			key, err = getKey(command, i)
			if err != nil {
				return datamodel.CreateError("ERR Unknown parameter")
			}
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

func appendCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	value, err := getKey(command, 0)
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

func setexCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	timeout, err := getInt(command, 0)
	if err != nil {
		return datamodel.CreateError("ERR Syntax error")
	}
	timeoutEx := timeout * 1000
	if timeoutEx <= 0 {
		return datamodel.CreateError("ERR Syntax error")
	}
	value, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Syntax error")
	}
	db.SetValue(key, datamodel.CreateString(value), SetAny, timeoutEx)
	return datamodel.CreateSimpleString("OK")
}

func psetexCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	timeoutEx, err := getInt(command, 0)
	if err != nil {
		return datamodel.CreateError("ERR Syntax error")
	}
	if timeoutEx <= 0 {
		return datamodel.CreateError("ERR Syntax error")
	}
	value, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Syntax error")
	}
	db.SetValue(key, datamodel.CreateString(value), SetAny, timeoutEx)
	return datamodel.CreateSimpleString("OK")
}

func setnxCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	value, err := getKey(command, 0)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	_, setted := db.SetValue(key, datamodel.CreateString(value), SetIfNotExists, 0)
	if !setted {
		return datamodel.CreateInt(0)
	}
	return datamodel.CreateInt(1)
}

func getsetCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	value, err := getKey(command, 0)
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

func incrCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	return ariphmeticCommand(db, key, 1)
}

func decrCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	return ariphmeticCommand(db, key, -1)
}

func incrbyCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	val, err := getInt(command, 0)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	return ariphmeticCommand(db, key, val)
}

func decrbyCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	val, err := getInt(command, 0)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	return ariphmeticCommand(db, key, -val)
}

func strlenCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	return db.GetValueAndProcess(key, func(val datamodel.CustomDataType, isval bool) datamodel.CustomDataType {
		if !isval {
			return datamodel.CreateInt(0)
		}
		str, okstr := val.(datamodel.DataString)
		if !okstr {
			return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return datamodel.CreateInt(len(str.Get()))
	})
}
