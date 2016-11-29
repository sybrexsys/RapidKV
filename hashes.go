package main

import "github.com/sybrexsys/RapidKV/datamodel"
import "strconv"

func hdelCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	hkey, err := getKey(command, 0)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	return db.ProcessValue(key, true, func(elem *Element) (datamodel.CustomDataType, bool) {
		if elem.Value == nil {
			return datamodel.CreateInt(0), false
		}

		dict, ok := elem.Value.(datamodel.DataDictionary)
		if !ok {
			return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value"), false
		}
		processed := 0
		idx := 1
		for {
			cur := dict.Value(hkey)
			_, ok = cur.(datamodel.DataNull)
			if !ok {
				dict.Add(hkey, datamodel.CreateNull())
				processed++
			}
			if idx == command.Count() {
				break
			}
			hkey, err = getKey(command, idx)
			if err != nil {
				return datamodel.CreateError("ERR Unknown parameter"), false
			}
			idx++
		}
		result := datamodel.CreateInt(processed)
		return result, true
	})
}

func hsetCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	hkey, err := getKey(command, 0)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	val, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	return db.ProcessValue(key, true, func(elem *Element) (datamodel.CustomDataType, bool) {
		var dict datamodel.DataDictionary
		if elem.Value == nil {
			dict = datamodel.CreateDictionary(10)
			elem.Value = dict
		} else {
			var ok bool
			dict, ok = elem.Value.(datamodel.DataDictionary)
			if !ok {
				return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value"), false
			}
		}
		res := 0
		cur := dict.Value(hkey)
		_, ok := cur.(datamodel.DataNull)
		if ok {
			res = 1
		}
		dict.Add(hkey, datamodel.CreateString(val))
		result := datamodel.CreateInt(res)
		return result, true
	})
}

func hexistsCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	hkey, err := getKey(command, 0)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	val, isval := db.GetValue(key)
	if !isval {
		return datamodel.CreateInt(0)
	}
	dict, okstr := val.(datamodel.DataDictionary)
	if !okstr {
		return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	cur := dict.Value(hkey)
	_, ok := cur.(datamodel.DataNull)
	if ok {
		return datamodel.CreateInt(0)
	}
	return datamodel.CreateInt(1)
}

func hgetCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	hkey, err := getKey(command, 0)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	val, isval := db.GetValue(key)
	if !isval {
		return datamodel.CreateNull()
	}
	dict, okstr := val.(datamodel.DataDictionary)
	if !okstr {
		return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	cur := dict.Value(hkey)
	return cur.Copy()
}

func hlenCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	val, isval := db.GetValue(key)
	if !isval {
		return datamodel.CreateInt(0)
	}
	dict, okstr := val.(datamodel.DataDictionary)
	if !okstr {
		return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return datamodel.CreateInt(dict.Count())
}

func hstrlenCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	hkey, err := getKey(command, 0)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	val, isval := db.GetValue(key)
	if !isval {
		return datamodel.CreateInt(0)
	}
	dict, okstr := val.(datamodel.DataDictionary)
	if !okstr {
		return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	cur := dict.Value(hkey)
	str, ok := cur.(datamodel.DataString)
	if !ok {
		return datamodel.CreateInt(0)
	}
	strstr := str.Get()
	return datamodel.CreateInt(len(strstr))
}

func hsetnxCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	hkey, err := getKey(command, 0)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	val, err := getKey(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	return db.ProcessValue(key, true, func(elem *Element) (datamodel.CustomDataType, bool) {
		var dict datamodel.DataDictionary
		if elem.Value == nil {
			dict = datamodel.CreateDictionary(10)
			elem.Value = dict
		} else {
			var ok bool
			dict, ok = elem.Value.(datamodel.DataDictionary)
			if !ok {
				return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value"), false
			}
		}
		cur := dict.Value(hkey)
		_, ok := cur.(datamodel.DataNull)
		if ok {
			dict.Add(hkey, datamodel.CreateString(val))
			return datamodel.CreateInt(1), true
		}
		return datamodel.CreateInt(0), true
	})
}

func hgetallCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	val, isval := db.GetValue(key)
	if !isval {
		return datamodel.CreateArray(0)
	}
	dict, okstr := val.(datamodel.DataDictionary)
	if !okstr {
		return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	arr := dict.Keys()
	outarr := datamodel.CreateArray(arr.Count() * 2)
	for i := 0; i < arr.Count(); i++ {
		hkey := arr.Get(i).(datamodel.DataString).Get()
		cur := dict.Value(hkey)
		str, ok := cur.(datamodel.DataString)
		if ok {
			outarr.Add(datamodel.CreateString(hkey))
			outarr.Add(str.Copy())
		}
	}
	return outarr
}

func hkeysCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	val, isval := db.GetValue(key)
	if !isval {
		return datamodel.CreateArray(0)
	}
	dict, okstr := val.(datamodel.DataDictionary)
	if !okstr {
		return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return dict.Keys()
}

func hvalsCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	val, isval := db.GetValue(key)
	if !isval {
		return datamodel.CreateArray(0)
	}
	dict, okstr := val.(datamodel.DataDictionary)
	if !okstr {
		return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	arr := dict.Keys()
	outarr := datamodel.CreateArray(arr.Count())
	for i := 0; i < arr.Count(); i++ {
		hkey := arr.Get(i).(datamodel.DataString).Get()
		cur := dict.Value(hkey)
		str, ok := cur.(datamodel.DataString)
		if ok {
			outarr.Add(str.Copy())
		}
	}
	return outarr
}

func hincrbyCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	hkey, err := getKey(command, 0)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	val, err := getInt(command, 1)
	if err != nil {
		return datamodel.CreateError("ERR Unknown parameter")
	}
	return db.ProcessValue(key, true, func(elem *Element) (datamodel.CustomDataType, bool) {
		var dict datamodel.DataDictionary
		if elem.Value == nil {
			dict = datamodel.CreateDictionary(10)
			elem.Value = dict
		} else {
			var ok bool
			dict, ok = elem.Value.(datamodel.DataDictionary)
			if !ok {
				return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value"), false
			}
		}
		cur := dict.Value(hkey)
		_, ok := cur.(datamodel.DataNull)
		if ok {
			dict.Add(hkey, datamodel.CreateString(strconv.Itoa(val)))
			return datamodel.CreateInt(val), true
		}
		str, ok := cur.(datamodel.DataString)
		if ok {
			ival, err := strconv.Atoi(str.Get())
			if err != nil {
				return datamodel.CreateError("ERR value is not an integer or out of range"), false
			}
			dict.Add(hkey, datamodel.CreateString(strconv.Itoa(val+ival)))
			return datamodel.CreateInt(val + ival), true
		}
		return datamodel.CreateInt(0), true
	})
}

func hmgetCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	val, isval := db.GetValue(key)
	if !isval {
		return datamodel.CreateArray(0)
	}
	dict, okstr := val.(datamodel.DataDictionary)
	if !okstr {
		return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	outarr := datamodel.CreateArray(command.Count())
	for i := 0; i < command.Count(); i++ {
		hkey, err := getKey(command, i)
		if err != nil {
			return datamodel.CreateError("ERR Unknown parameter")
		}
		cur := dict.Value(hkey)
		switch cur.(type) {
		case datamodel.DataString:
			outarr.Add(cur.Copy())
		case datamodel.DataNull:
			outarr.Add(cur.Copy())
		}
	}
	return outarr
}

func hmsetCommand(db *Database, key string, command datamodel.DataArray) datamodel.CustomDataType {
	return db.ProcessValue(key, true, func(elem *Element) (datamodel.CustomDataType, bool) {
		var dict datamodel.DataDictionary
		if elem.Value == nil {
			dict = datamodel.CreateDictionary(10)
			elem.Value = dict
		} else {
			var ok bool
			dict, ok = elem.Value.(datamodel.DataDictionary)
			if !ok {
				return datamodel.CreateError("WRONGTYPE Operation against a key holding the wrong kind of value"), false
			}
		}
		cnt := command.Count()
		if cnt&1 == 1 || cnt == 0 {
			return datamodel.CreateError("ERR Invalid syntax"), false
		}
		for i := 0; i < cnt; i += 2 {
			hkey, err := getKey(command, i)
			if err != nil {
				return datamodel.CreateError("ERR Unknown parameter"), false
			}
			val, err := getKey(command, i)
			if err != nil {
				return datamodel.CreateError("ERR Unknown parameter"), false
			}
			dict.Add(hkey, datamodel.CreateString(val))
		}
		return datamodel.CreateSimpleString("OK"), true
	})
}
