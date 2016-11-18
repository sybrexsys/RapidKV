package datamodel

import (
	"errors"
	"fmt"
	"strconv"
)

/*const (
    ModelType
)*/

type DataBase interface {
	getLength() int
	writeToBytes(b []byte) (int, error)
}

type DataNull interface {
	DataBase
	IsNull()
}

type DataBool interface {
	DataBase
	Get() bool
	Set(Value bool)
}

type DataInt interface {
	DataBase
	Get() int
	Set(Value int)
}

type DataReal interface {
	DataBase
	Get() float64
	Set(Value float64)
}

type DataString interface {
	DataBase
	Get() string
	Set(Value string)
}

type DataArray interface {
	DataBase
	Count() int
	Add(NewElement DataBase) int
	Insert(Index int, NewElement DataBase) int
	Remove(Index int)
	Get(Index int) DataBase
}

type DataDictionary interface {
	DataBase
	Count() int
	Add(Key DataString, Value DataBase)
	Delete(Key DataString)
	Value(Key DataString) DataBase
}

// Null section
type dataNull struct{}

var storageNull = dataNull{}

func CreateNull() *dataNull {
	return &storageNull
}

func (*dataNull) IsNull() {}

func (*dataNull) getLength() int { return 4 }

func (*dataNull) writeToBytes(b []byte) (int, error) {
	if len(b) < 4 {
		return -1, errors.New("don't enougth space for store")
	}
	b[0] = 'n'
	b[1] = 'u'
	b[2] = 'l'
	b[3] = 'l'
	return 4, nil
}

// Boolean section
type dataBool struct {
	val bool
}

func CreateBool(val bool) *dataBool {
	return &dataBool{val: val}
}

func (obj *dataBool) getLength() int {
	if obj.val {
		return 4
	}
	return 5
}

func (obj *dataBool) writeToBytes(b []byte) (int, error) {
	if obj.val {
		if len(b) < 4 {
			return -1, errors.New("don't enougth space for store")
		}
		b[0] = 't'
		b[1] = 'r'
		b[2] = 'u'
		b[3] = 'e'
		return 4, nil
	}

	if len(b) < 5 {
		return -1, errors.New("don't enougth space for store")
	}
	b[0] = 'f'
	b[1] = 'a'
	b[2] = 'l'
	b[3] = 's'
	b[4] = 'e'
	return 5, nil
}

// Int section

func getIntSize(x int) int {
	p := 10
	count := 1
	for x >= p {
		count++
		p *= 10
	}
	return count
}

type dataInt struct {
	val int
}

func CreateInt(val int) *dataInt {
	return &dataInt{val: val}
}

func (obj *dataInt) getLength() int {
	if obj.val >= 0 {
		return getIntSize(obj.val)
	}
	return getIntSize(-obj.val) + 1
}

func (obj *dataInt) writeToBytes(b []byte) (int, error) {
	str := strconv.Itoa(obj.val)
	if len(b) < len(str) {
		return -1, errors.New("don't enougth space for store")
	}
	return copy(b, []byte(str)), nil
}

// real section

func getRealSize(x float64) int {
	return len(fmt.Sprint(x))
}

type dataReal float64

func (obj *dataReal) getLength() int {
	return getRealSize(float64(*obj))
}

//  string section

type dataString struct {
	val string
}

func CreateSring(str string) *dataString {
	return &dataString{val: str}
}

func (obj *dataString) writeToBytes(b []byte) (int, error) {
	lenb := len(b)
	src := []byte(obj.val)
	if lenb < 2 {
		return -1, errors.New("don't enougth space for store")
	}
	b[0] = '"'
	offset := 1
	for _, ch := range src {
		switch ch {
		case 0:
			if lenb < offset+4 {
				return -1, errors.New("don't enougth space for store")
			}
			b[offset] = '\\'
			b[offset+1] = '0'
			b[offset+2] = '0'
			b[offset+3] = '0'
			offset += 4
		case 7:
			if lenb < offset+2 {
				return -1, errors.New("don't enougth space for store")
			}
			b[offset] = '\\'
			b[offset+1] = 't'
			offset += 2
		case 13:
			if lenb < offset+2 {
				return -1, errors.New("don't enougth space for store")
			}
			b[offset] = '\\'
			b[offset+1] = 'r'
			offset += 2
		case 10:
			if lenb < offset+2 {
				return -1, errors.New("don't enougth space for store")
			}
			b[offset] = '\\'
			b[offset+1] = 'n'
			offset += 2
		case '"':
			if lenb < offset+2 {
				return -1, errors.New("don't enougth space for store")
			}
			b[offset] = '\\'
			b[offset+1] = '"'
			offset += 2
		case '\\':
			if lenb < offset+2 {
				return -1, errors.New("don't enougth space for store")
			}
			b[offset] = '\\'
			b[offset+1] = '\\'
			offset += 2
		default:
			b[offset] = ch
			offset++
		}
	}
	if lenb < offset+1 {
		return -1, errors.New("don't enougth space for store")
	}
	b[offset] = '"'
	return offset + 1, nil
}

func (obj *dataString) getLength() int {
	cnt := 0
	d := []byte(obj.val)
	for _, c := range d {
		switch c {
		case 7, 10, 13, '"', '\\':
			cnt += 2
		case 0:
			cnt += 4
		default:
			cnt++
		}
	}
	return cnt + 2
}

func (obj *dataString) Get() string {
	return obj.val
}
func (obj *dataString) Set(Value string) {
	obj.val = Value
}

// array section
type dataArray struct {
	list []DataBase
	cnt  int
}

func createArray(initialSize int) *dataArray {
	return &dataArray{
		list: make([]DataBase, initialSize),
		cnt:  0,
	}
}

func (obj *dataArray) writeToBytes(b []byte) (int, error) {
	lenb := len(b)
	if lenb < 1 {
		return -1, errors.New("don't enougth space for store")
	}
	b[0] = '['
	offset := 1
	for i := 0; i < obj.cnt; i++ {
		wrt, err := obj.list[i].writeToBytes(b[offset:])
		if err != nil {
			return -1, err
		}
		offset += wrt
		if i < obj.cnt-1 {
			if lenb < offset+2 {
				return -1, errors.New("don't enougth space for store")
			}
			b[offset] = ','
			b[offset+1] = ' '
			offset += 2
		}
	}
	if lenb < offset+1 {
		return -1, errors.New("don't enougth space for store")
	}
	b[offset] = ']'
	return offset + 1, nil

}

func (obj *dataArray) getLength() int {
	cnt := 0
	for i := 0; i < obj.cnt; i++ {
		cnt += obj.list[i].getLength()
	}
	cnt += 2 + (obj.cnt-1)*2
	return cnt
}

func (obj *dataArray) Count() int {
	return obj.cnt
}

func (obj *dataArray) Add(NewElement DataBase) int {
	return obj.Insert(obj.cnt, NewElement)
}

func (obj *dataArray) Adds(NewElements ...DataBase) {
	for _, item := range NewElements {
		obj.Insert(obj.cnt, item)
	}
}

func (obj *dataArray) setCapacity(newCapacity int) {
	tmp := make([]DataBase, newCapacity)
	copy(tmp, obj.list)
	obj.list = tmp
}

func (obj *dataArray) grow() {
	var Delta int
	Cap := len(obj.list)
	if Cap > 64 {
		Delta = Cap / 4
	} else {
		if Cap > 8 {
			Delta = 16
		} else {
			Delta = 4
		}
	}
	obj.setCapacity(Cap + Delta)
}

func (obj *dataArray) Insert(Index int, NewElement DataBase) int {
	if obj.cnt == len(obj.list) {
		obj.grow()
	}
	cnt := obj.cnt
	if Index < 0 || Index >= cnt {
		obj.list[cnt] = NewElement
		obj.cnt++
		return cnt
	}
	copy(obj.list[Index+1:cnt+1], obj.list[Index:cnt+1])
	obj.list[Index] = NewElement
	return Index
}

func (obj *dataArray) Remove(Index int) {
	if Index < 0 || Index >= obj.cnt {
		return
	}

}

func (obj *dataArray) Get(Index int) DataBase {
	if Index < 0 || Index >= len(obj.list) {
		return &storageNull
	}
	return obj.list[Index]
}

// dictionary section

type dataDictionary struct {
	dict map[string]DataBase
}

/*
type DataDictionary interface {
	DataBase
	Count() int
	Add(Key DataString, Value DataBase)
	Delete(Key DataString)
	Value(Key DataString) DataBase
}*/
