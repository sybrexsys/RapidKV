package main

import (
	"github.com/sybrexsys/RapidKV/datamodel"
	"strings"
)

func lpushCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	return db.ProcessValue(key, true, func(elem *Element) (datamodel.CustomDataType, bool) {
		var arr datamodel.DataArray
		if elem.Value == nil {
			arr = datamodel.CreateArray(10)
			elem.Value = arr
		} else {
			var ok bool
			arr, ok = elem.Value.(datamodel.DataArray)
			if !ok {
				return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value"), false
			}
		}

		for i := 2; i < command.Count(); i++ {
			newval, err := getKey(command, i)
			if err != nil {
				return datamodel.CreateError("ERR Unknown parameter"), false
			}
			arr.Insert(0, datamodel.CreateString(newval))
		}
		result := datamodel.CreateInt(arr.Count())
		return result, true
	})
}

func rpushCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	return db.ProcessValue(key, true, func(elem *Element) (datamodel.CustomDataType, bool) {
		var arr datamodel.DataArray
		if elem.Value == nil {
			arr = datamodel.CreateArray(10)
			elem.Value = arr
		} else {
			var ok bool
			arr, ok = elem.Value.(datamodel.DataArray)
			if !ok {
				return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value"), false
			}
		}

		for i := 2; i < command.Count(); i++ {
			newval, err := getKey(command, i)
			if err != nil {
				return datamodel.CreateError("ERR Unknown parameter"), false
			}
			arr.Add(datamodel.CreateString(newval))
		}
		result := datamodel.CreateInt(arr.Count())
		return result, true
	})
}

func rpopCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)

	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}

	res := db.ProcessValue(key, true, func(elem *Element) (datamodel.CustomDataType, bool) {
		var arr datamodel.DataArray
		if elem.Value == nil {
			elem.Value = arr
			return datamodel.CreateNull(), false
		}

		arr, ok := elem.Value.(datamodel.DataArray)
		if !ok {
			return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value"), false
		}
		cnt := arr.Count()
		if cnt == 0 {
			return datamodel.CreateNull(), false
		}
		res := arr.Get(cnt - 1)
		arr.Remove(cnt - 1)
		return res, true
	})
	return res
}

func lpopCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	res := db.ProcessValue(key, true, func(elem *Element) (datamodel.CustomDataType, bool) {
		var arr datamodel.DataArray
		if elem.Value == nil {
			elem.Value = arr
			return datamodel.CreateNull(), false
		}

		arr, ok := elem.Value.(datamodel.DataArray)
		if !ok {
			return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value"), false
		}
		cnt := arr.Count()
		if cnt == 0 {
			return datamodel.CreateNull(), false
		}
		res := arr.Get(0)
		arr.Remove(0)
		return res, true
	})
	return res
}

func lpushxCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	return db.ProcessValue(key, true, func(elem *Element) (datamodel.CustomDataType, bool) {
		var arr datamodel.DataArray
		if elem.Value == nil {
			return datamodel.CreateInt(0), false
		}

		arr, ok := elem.Value.(datamodel.DataArray)
		if !ok {
			return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value"), false
		}

		newval, err := getKey(command, 2)
		if err != nil {
			return datamodel.CreateError("ERR Unknown parameter"), false
		}
		arr.Insert(0, datamodel.CreateString(newval))

		result := datamodel.CreateInt(arr.Count())
		return result, true
	})
}

func rpushxCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	return db.ProcessValue(key, true, func(elem *Element) (datamodel.CustomDataType, bool) {
		var arr datamodel.DataArray
		if elem.Value == nil {
			return datamodel.CreateInt(0), false
		}

		arr, ok := elem.Value.(datamodel.DataArray)
		if !ok {
			return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value"), false
		}

		newval, err := getKey(command, 2)
		if err != nil {
			return datamodel.CreateError("ERR Unknown parameter"), false
		}
		arr.Add(datamodel.CreateString(newval))

		result := datamodel.CreateInt(arr.Count())
		return result, true
	})
}

func llenCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	val, isval := db.GetValue(key)
	if !isval {
		return datamodel.CreateInt(0)
	}
	arr, okstr := val.(datamodel.DataArray)
	if !okstr {
		return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return datamodel.CreateInt(arr.Count())
}

func lindexCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	val := command.Get(2)
	c, isInt := val.(datamodel.DataInt)
	if !isInt {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	val, isval := db.GetValue(key)
	if !isval {
		return datamodel.CreateNull()
	}
	arr, okstr := val.(datamodel.DataArray)
	if !okstr {
		return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	idx := c.Get()
	if idx < 0 {
		idx = arr.Count() + idx
	}
	return arr.Get(idx).Copy()
}

func linsertCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	loc, err := getKey(command, 2)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	var isbefore bool
	loc = strings.ToLower(loc)
	if loc == "before" {
		isbefore = true
	} else if loc == "after" {
		isbefore = false
	} else {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	search, err := getKey(command, 3)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	value, err := getKey(command, 4)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	return db.ProcessValue(key, true, func(elem *Element) (datamodel.CustomDataType, bool) {
		var arr datamodel.DataArray
		if elem.Value == nil {
			return datamodel.CreateInt(-1), false
		}

		arr, ok := elem.Value.(datamodel.DataArray)
		if !ok {
			return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value"), false
		}

		cnt := arr.Count()
		idx := -1
		for i := 0; i < cnt; i++ {
			item, ok := arr.Get(i).(datamodel.DataString)
			if !ok {
				continue
			}
			if item.Get() == search {
				idx = i
				break
			}
		}
		if idx == -1 {
			return datamodel.CreateInt(0), false
		}
		if !isbefore {
			idx++
		}
		arr.Insert(idx, datamodel.CreateString(value))
		result := datamodel.CreateInt(arr.Count())
		return result, true
	})
}

func ltrimCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	return db.ProcessValue(key, true, func(elem *Element) (datamodel.CustomDataType, bool) {
		var arr datamodel.DataArray
		if elem.Value == nil {
			return datamodel.CreateSimpleString("OK"), false
		}

		arr, ok := elem.Value.(datamodel.DataArray)
		if !ok {
			return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value"), false
		}
		val := command.Get(2)
		c, isInt := val.(datamodel.DataInt)
		if !isInt {
			return datamodel.CreateError("ERR Unknown parameter"), false
		}
		fromval := c.Get()
		val = command.Get(2)
		c, isInt = val.(datamodel.DataInt)
		if !isInt {
			return datamodel.CreateError("ERR Unknown parameter"), false
		}
		toval := c.Get()
		cnt := arr.Count()
		if fromval < 0 {
			fromval = cnt + fromval
		}
		if toval < 0 {
			toval = cnt + toval
		}

		if fromval < 0 {
			fromval = 0
		}
		if toval < 0 {
			toval = 0
		}
		if fromval >= cnt || fromval > toval {
			elem.Value = datamodel.CreateArray(10)
			return datamodel.CreateSimpleString("OK"), false
		}
		steps := toval - fromval + 1
		for i := 0; i < steps; i++ {
			arr.Remove(fromval)
		}
		return datamodel.CreateSimpleString("OK"), false
	})
}

func lsetCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	val := command.Get(2)
	c, isInt := val.(datamodel.DataInt)
	if !isInt {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	value, err := getKey(command, 3)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}

	return db.ProcessValue(key, true, func(elem *Element) (datamodel.CustomDataType, bool) {
		var arr datamodel.DataArray
		if elem.Value == nil {
			return datamodel.CreateError("ERR No such key"), false
		}

		arr, ok := elem.Value.(datamodel.DataArray)
		if !ok {
			return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value"), false
		}

		cnt := arr.Count()
		idx := c.Get()
		if idx < 0 {
			idx = cnt - idx
		}
		if idx < 0 || idx >= cnt {
			return datamodel.CreateError("ERR Out of range"), false
		}
		arr.Remove(idx)
		arr.Insert(idx, datamodel.CreateString(value))
		result := datamodel.CreateSimpleString("OK")
		return result, true
	})
}

func lrangeCommand(db *Database, command datamodel.DataArray) datamodel.CustomDataType {
	key, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	val := command.Get(2)
	c, isInt := val.(datamodel.DataInt)
	if !isInt {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	fromval := c.Get()

	val = command.Get(3)
	c, isInt = val.(datamodel.DataInt)
	if !isInt {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	toval := c.Get()

	val, isval := db.GetValue(key)
	if !isval {
		return datamodel.CreateArray(0)
	}

	arr, okstr := val.(datamodel.DataArray)
	if !okstr {
		return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	cnt := arr.Count()

	if fromval < 0 {
		fromval = cnt + fromval
	}
	if toval < 0 {
		toval = cnt + toval
	}

	if fromval < 0 {
		fromval = 0
	}
	if toval < 0 {
		toval = 0
	}

	if toval >= cnt {
		toval = cnt - 1
	}

	if fromval > toval {
		return datamodel.CreateArray(0)
	}

	res := datamodel.CreateArray(toval - fromval + 1)
	for i := fromval; i <= toval; i++ {
		res.Add(arr.Get(i).Copy())
	}
	return res
}
