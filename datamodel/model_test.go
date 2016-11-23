package datamodel

import (
	//"fmt"
	"testing"
	"time"
)

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
	dict.Add("Test1", arr)
	arr.Adds(
		CreateNull(),
		CreateBool(false),
		CreateBool(true),
		CreateInt(100),
		CreateInt(-100),
		CreateReal(3.14),
		CreateString("3.14000 \t \r \n \" \\ \b  \f / test Русский язык "),
	)
	dict.Add("Test2", CreateInt(100))
	var test CustomDataType
	test = dict
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
	checkResult(t, CreateString("\000 3.1400 \t \r \n \" \\ test Русский язык "), "\"\\u0000 3.1400 \\t \\r \\n \\\" \\\\ test Русский язык \"")
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

var mustFailLexeme = []string{
	"-",
	"-1.10s",
	"-.10",
	"-1.10\\",
	"1111111111111111111111111111111111111111111",
	"1k",
	"1.01e-1d",
	`"test\u00"`,
	`"test\u003"`,
	"",
	"/",
	"//",
	"invalid",
	`inva\tlid`,
	"/\rnull",
	"/*\rnull",
	" \r/**/",
	"\"\testsrtdsasdsasdsadas\\\\ \\r \\u10Fa",
	`"test`,
	`"test\u005 "`,
	`"test\ "`,
	`"test\s "`,
	`"test\u"`,
	`"test\u0"`,
	`"test \"`,
	"abc",
	`"\`,
	`"\z"`,
}

var mustPassLexeme = []string{
	"-1.10]",
	"1.10",
	"1234 ",
	"4455.5e-1",
	"// some error \r null",
	"\"\\testsrtdsasdsasdsadas\\\\ \\r \\u10Fa \"",
	`"\u005c \t a \r alses \f \b \n"
	`,
	`" rt" /* testcomment */`,
	"\rnull",
	"//test\rnull",
	"//test\rnull",
	"/**/null",
	"{}",
	"[]",
	//	"\"testsrt\"",

}

func TestParsingLexeme(t *testing.T) {
	for i := 0; i < len(mustFailLexeme); i++ {
		//		fmt.Printf("Step %d\n%s\n", i, mustFailLexeme[i])
		data := []byte(mustFailLexeme[i])
		off := 0
		lex := new(lexeme)
		err := getLexeme(data, &off, lex)
		if err == nil {
			t.Fatalf("Step %d\r Bellow lexeme must be parsed with error\r%s", i, mustFailLexeme[i])
		}
	}
	for i := 0; i < len(mustPassLexeme); i++ {
		//		fmt.Printf("Step %d\n%s\n", i, mustPassLexeme[i])
		data := []byte(mustPassLexeme[i])
		off := 0
		lex := new(lexeme)
		err := getLexeme(data, &off, lex)
		if err != nil {
			t.Fatalf("Step %d\r Bellow lexeme must be parsed without error\r%s\r Bellow error was received \r%s", i, mustPassLexeme[i], err.Error())
		}
	}
}

var mustFailObjects = []string{
	"{1:1}",
	"{1 :1}",
	"[abc",
	"[}",
	"[null null]",
	"[null [",
	"[null error",

	`{"1":}`,
	`{"11"{}}`,
	`{"11":100 1}`,
	`{"1:":error}`,
	`{"1";1}`,
	`{"1":100]`,
	"someerror",
}

var mustPassObjects = []string{
	`{"a1":[true,null," rt" /* testcomment */],
			"b2":false//,
			,
			//
			"c3":"testOther"}`,
	"{}",
	"[]",
}

func TestParsingObjects(t *testing.T) {
	for i := 0; i < len(mustFailObjects); i++ {
		//		fmt.Printf("Step %d\n%s\n", i, mustFailLexeme[i])
		data := []byte(mustFailObjects[i])
		off := 0
		_, err := LoadOneJSONObj(data, &off)
		if err == nil {
			t.Fatalf("Step %d\r Bellow lexeme must be parsed with error\r%s", i, mustFailLexeme[i])
		}
	}
	for i := 0; i < len(mustPassObjects); i++ {
		//		fmt.Printf("Step %d\n%s\n", i, mustPassLexeme[i])
		data := []byte(mustPassObjects[i])
		off := -1
		_, err := LoadOneJSONObj(data, &off)
		if err != nil {
			t.Fatalf("Step %d\r Bellow lexeme must be parsed without error\r%s\r Bellow error was received \r%s", i, mustPassLexeme[i], err.Error())
		}
	}

}

func TestParsing(t *testing.T) {

	offset := 0
	lex := new(lexeme)
	obj, err := parseObj([]byte("[true, false, null]"), &offset, lex)
	if err != nil {
		t.Fatalf("%s", err.Error())
	}
	checkResult(t, obj, "[true, false, null]")

	strongTest := `
		{"a1":[true,null," rt" /* testcomment */],
			"b2":false//,
			,
			//
			"c3":"testOther"}`

	offset = 0
	_, err = parseObj([]byte(strongTest), &offset, lex)
	if err != nil {
		t.Fatalf("%s", err.Error())
	}

	strongTest = `
		{"ccc1":[true,null," rt" /* testcomment */, 100.4 ,-5, 90, 4.01e+5, 0]
			}`

	offset = 0
	_, err = parseObj([]byte(strongTest), &offset, lex)
	if err != nil {
		t.Fatalf("%s", err.Error())
	}
	_, err = LoadJSONObj([]byte(strongTest))
	if err != nil {
		t.Fatalf("%s", err.Error())
	}
	strongTest = strongTest + `/*  */ [true]`
	_, err = LoadJSONObj([]byte(strongTest))
	if err == nil {
		t.Fatal("Error object must be found")
	}
	_, err = LoadJSONObj([]byte(longjson))
	if err != nil {
		t.Fatal("Error object must be found")
	}

}

var longjson = `
{
    "_id" : "50ab0f8bbcf1bfe2536dc3f9",
    "body" : "Amendment I\n<p>Congress shall make no law respecting an establishment of religion, or prohibiting the free exercise thereof; or abridging the freedom of speech, or of the press; or the right of the people peaceably to assemble, and to petition the Government for a redress of grievances.\n<p>\nAmendment II\n<p>\nA well regulated Militia, being necessary to the security of a free State, the right of the people to keep and bear Arms, shall not be infringed.\n<p>\nAmendment III\n<p>\nNo Soldier shall, in time of peace be quartered in any house, without the consent of the Owner, nor in time of war, but in a manner to be prescribed by law.\n<p>\nAmendment IV\n<p>\nThe right of the people to be secure in their persons, houses, papers, and effects, against unreasonable searches and seizures, shall not be violated, and no Warrants shall issue, but upon probable cause, supported by Oath or affirmation, and particularly describing the place to be searched, and the persons or things to be seized.\n<p>\nAmendment V\n<p>\nNo person shall be held to answer for a capital, or otherwise infamous crime, unless on a presentment or indictment of a Grand Jury, except in cases arising in the land or naval forces, or in the Militia, when in actual service in time of War or public danger; nor shall any person be subject for the same offence to be twice put in jeopardy of life or limb; nor shall be compelled in any criminal case to be a witness against himself, nor be deprived of life, liberty, or property, without due process of law; nor shall private property be taken for public use, without just compensation.\n<p>\n\nAmendment VI\n<p>\nIn all criminal prosecutions, the accused shall enjoy the right to a speedy and public trial, by an impartial jury of the State and district wherein the crime shall have been committed, which district shall have been previously ascertained by law, and to be informed of the nature and cause of the accusation; to be confronted with the witnesses against him; to have compulsory process for obtaining witnesses in his favor, and to have the Assistance of Counsel for his defence.\n<p>\nAmendment VII\n<p>\nIn Suits at common law, where the value in controversy shall exceed twenty dollars, the right of trial by jury shall be preserved, and no fact tried by a jury, shall be otherwise re-examined in any Court of the United States, than according to the rules of the common law.\n<p>\nAmendment VIII\n<p>\nExcessive bail shall not be required, nor excessive fines imposed, nor cruel and unusual punishments inflicted.\n<p>\nAmendment IX\n<p>\nThe enumeration in the Constitution, of certain rights, shall not be construed to deny or disparage others retained by the people.\n<p>\nAmendment X\n<p>\nThe powers not delegated to the United States by the Constitution, nor prohibited by it to the States, are reserved to the States respectively, or to the people.\"\n<p>\n",
    "permalink" : "aRjNnLZkJkTyspAIoRGe",
    "author" : "machine",
    "title" : "Bill of Rights",
    "tags" : [ 
        "watchmaker", 
        "santa", 
        "xylophone", 
        "math", 
        "handsaw", 
        "dream", 
        "undershirt", 
        "dolphin", 
        "tanker", 
        "action",
		1234355534,
		-122323,
		true,
		false,
		null,
		1454564.334343233
    ],
    "comments" : [ 
        {
            "body" : "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in ",
            "email" : "HvizfYVx@pKvLaagH.com",
            "author" : "Santiago Dollins"
        }, 
        {
            "body" : "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in ",
            "email" : "WpOUCpdD@hccdxJvT.com",
            "author" : "Jaclyn Morado"
        }, 
        {
            "body" : "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in ",
            "email" : "OgDzHfFN@cWsDtCtx.com",
            "author" : "Houston Valenti"
        }, 
        {
            "body" : "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in ",
            "email" : "tMNoFYPb@lymMpAyS.com",
            "author" : "Whitley Fears"
        } 
     ],
    "date" : "2012-11-20T05:05:15.231Z"
}

`

func BenchmarkParseJson(b *testing.B) {
	// run the Fib function b.N times
	t := time.Now()
	tot := []byte(longjson)
	for n := 0; n < b.N; n++ {
		_, err := LoadJSONObj(tot)
		if err != nil {
			b.Fatal("Error " + err.Error())
		}
	}
	b.Log("Total time ", b.N, "  processed bytes:", b.N*len(tot), " ", time.Since(t))
}

func TestLoadAndSave(t *testing.T) {
	tot := []byte(longjson)
	offset := 0
	obj, err := LoadOneJSONObj(tot, &offset)
	if err != nil {
		t.Fatal("Error " + err.Error())
	}
	l := obj.getLength()
	m := make([]byte, l)
	_, err = obj.writeToBytes(m)
	if err != nil {
		t.Fatal("Error " + err.Error())
	}
}

func BenchmarkSaveJsonOneAlloc(b *testing.B) {
	tot := []byte(longjson)
	t := time.Now()
	offset := 0
	obj, err := LoadOneJSONObj(tot, &offset)
	if err != nil {
		b.Fatal("Error " + err.Error())
	}
	l := obj.getLength()
	m := make([]byte, l)
	for n := 0; n < b.N; n++ {

		_, err = obj.writeToBytes(m)
		if err != nil {
			b.Fatal("Error " + err.Error())
		}
	}
	b.Log("Total processed bytes:", b.N*len(tot), " ", time.Since(t))
}

func BenchmarkSaveJsonEachAlloc(b *testing.B) {

	tot := []byte(longjson)
	t := time.Now()
	offset := 0
	obj, err := LoadOneJSONObj(tot, &offset)
	if err != nil {
		b.Fatal("Error " + err.Error())
	}
	for n := 0; n < b.N; n++ {
		l := obj.getLength()
		m := make([]byte, l)
		_, err = obj.writeToBytes(m)
		if err != nil {
			b.Fatal("Error " + err.Error())
		}
	}
	b.Log("Total processed bytes:", b.N*len(tot), " ", time.Since(t))
}
