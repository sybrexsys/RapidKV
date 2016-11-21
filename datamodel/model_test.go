package datamodel

import "testing"

var specChars = [256]byte{
	1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
}

func typeObject(t *testing.T, obj CustomDataType) string {
	l := obj.getLength()
	m := make([]byte, l)
	_, err := obj.writeToBytes(m)
	if err != nil {
		t.Fatalf("Calculation returns error: %s", err.Error())
	}
	return string(m)
}

func getType(obj CustomDataType) string {
	switch obj.(type) {
	case DataDictionary:
		return "Dictionary"
	case DataNull:
		return "Null"
	case DataBool:
		return "Bool"
	case DataInt:
		return "Int"
	case DataReal:
		return "Real"
	case DataString:
		return "String"
	case DataArray:
		return "Array"
	}
	return "Base"
}

func checkResult(t *testing.T, obj CustomDataType, waitingResult string) {
	s := typeObject(t, obj)
	if s != waitingResult {
		t.Fatalf("\nobj:    %s\nvalue:  %s\nwait:   %s", getType(obj), s, waitingResult)
	}
}

func TestArrayWork(t *testing.T) {
	arr := CreateArray(10)
	checkResult(t, arr, "[]")
	arr.Add(CreateNull())
	checkResult(t, arr, "[null]")
	arr.Add(CreateNull())
	checkResult(t, arr, "[null, null]")
	arr.Remove(0)
	checkResult(t, arr, "[null]")
	arr.Add(CreateBool(true))
	checkResult(t, arr.Get(100), "null")
	checkResult(t, arr.Get(1), "true")
	arr.Add(CreateBool(false))
	checkResult(t, arr, "[null, true, false]")
	arr.Remove(1)
	checkResult(t, arr, "[null, false]")
	for i := 0; i < 100; i++ {
		arr.Add(CreateBool(true))
	}
	for i := 0; i < 100; i++ {
		arr.Remove(0)
	}
	arr.Adds(CreateBool(true), CreateBool(false))
	checkResult(t, arr, "[true, true, true, false]")
	arr.Insert(1, CreateBool(false))
	checkResult(t, arr, "[true, false, true, true, false]")
	idx := arr.Count()
	arr.Remove(idx)
	if idx != arr.Count() {
		t.Fatal("Must not change count of the elements")
	}

}

func TestDictionaryWork(t *testing.T) {
	dict := CreateDictionary(10)
	dict.Add("Test", CreateArray(10))
	checkResult(t, dict, "{\"Test\":[]}")
	dict.Add("Test1", CreateBool(true))
	dict.Add("Test", CreateNull())
	checkResult(t, dict, "{\"Test1\":true}")
	dict.Add("Test", CreateArray(0))
	z := dict.Count()
	if z != 2 {
		t.Fatalf("Dictionary element count %d, waited 2", z)
	}
	a := dict.Value("Test1")
	checkResult(t, a, "true")
	a.(DataBool).Set(false)

	a = dict.Value("Test")
	checkResult(t, a, "[]")

	a = dict.Value("Unknown")
	checkResult(t, a, "null")

	dict.Add("Test", CreateNull())
	checkResult(t, dict, "{\"Test1\":false}")
	arr := CreateArray(0)
	d1 := CreateDictionary(10)
	arr.Adds(CreateDictionary(10), d1)
	dict.Add("Test", arr)
	arr.Adds(
		CreateNull(),
		CreateBool(false),
		CreateBool(true),
		CreateInt(100),
		CreateInt(-100),
		CreateReal(3.14),
		CreateString("3.14000 \t \r \n \" \\ test Русский язык "),
	)
	var test CustomDataType
	test = arr
	l := test.getLength()
	for i := 0; i < l; i++ {
		m := make([]byte, i)
		if _, err := test.writeToBytes(m); err == nil {
			t.Fatalf("Function must return Error, %d %s", i, string(m))
		}
	}

}

func TestPrimitives(t *testing.T) {
	checkResult(t, CreateNull(), "null")
	checkResult(t, CreateBool(false), "false")
	checkResult(t, CreateInt(-100), "-100")
	checkResult(t, CreateReal(3.14), "3.14")
	checkResult(t, CreateString("3.1400 \t \r \n \" \\ test Русский язык "), "\"3.1400 \\t \\r \\n \\\" \\\\ test Русский язык \"")
	a := CreateBool(false)
	a.Set(!a.Get())
	checkResult(t, a, "true")
	b := CreateInt(100)
	b.Set(b.Get() + 1)
	checkResult(t, b, "101")
	c := CreateReal(3.14)
	c.Set(-c.Get())
	checkResult(t, c, "-3.14")
	d := CreateString("Test")
	d.Set(d.Get() + " passed")
	checkResult(t, d, "\"Test passed\"")
}

func TestParsing(t *testing.T) {
	offset := 0
	lex := new(lexeme)
	obj, err := ParseObj([]byte("[true, false, null]"), &offset, lex)
	if err != nil {
		t.Fatalf("%s", err.Error())
	}
	checkResult(t, obj, "[true, false, null]")
}
