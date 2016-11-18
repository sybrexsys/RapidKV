package datamodel

import (
	"testing"
)

func TestArrayWork(t *testing.T) {
	const arrres = "[null ,null]"
	arr := createArray(10)
	//for i := 0; i < 2; i++ {
	arr.Add(CreateNull())
	arr.Adds(CreateBool(true), CreateInt(100), CreateInt(-2000))
	addarr := createArray(10)
	addarr.Adds(CreateBool(true), CreateInt(100), CreateInt(-2000), CreateSring("test\n\\ 100 \000 \" 10012 русския языка"))
	arr.Add(addarr)

	//	}
	l := arr.getLength()
	m := make([]byte, l)
	_, err := arr.writeToBytes(m)
	if err != nil {
		t.Fatalf("Calculation returns error: %s", err.Error())
	}
	s := string(m)
	if s != arrres {
		t.Fatalf("Calculation returns invalid value: %s  Waiting result: %s ", s, arrres)
	}
}
