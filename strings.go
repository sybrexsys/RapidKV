package main

import (
	"strings"

	"github.com/sybrexsys/RapidKV/database"
	"github.com/sybrexsys/RapidKV/datamodel"
)

func getCommand(db *database.Database, command datamodel.DataArray) datamodel.CustomDataType {
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
		return datamodel.CreateError("ERR Not string was found")
	}
	return val
}

func setCommand(db *database.Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	value, err := getKey(command, 2)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	if command.Count() == 3 {
		db.SetValue(key, datamodel.CreateString(value), database.SetAny, 0)
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
			setIfExist = database.SetIfExists
		}
		if option == "nx" {
			setIfExist = database.SetIfNotExists
		}
		if option == "ex" {
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
		}
		if option == "px" {
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

func appendCommand(db *database.Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	value, err := getKey(command, 2)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	return db.ProcessValue(key, true, func(elem *database.Element) (datamodel.CustomDataType, bool) {
		if elem.Value == nil {
			elem.Value = datamodel.CreateString(value)
			return datamodel.CreateInt(len(value)), true
		}
		val, ok := elem.Value.(datamodel.DataString)
		if !ok {
			return datamodel.CreateError("ERR Value is not string"), false
		}
		str := val.Get() + value
		val.Set(str)
		return datamodel.CreateInt(len(str)), true
	})
}

/*
func decrCommand(db *database.Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
}
*/
