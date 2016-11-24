package datamodel

import (
	"bytes"
	"errors"
	"strconv"
)

/*const (
    ModelType
)*/

type CustomDataType interface {
	getLength() int
	writeToBytes(b []byte) (int, error)
	Copy() CustomDataType
}

type DataNull interface {
	CustomDataType
	isNull()
}

type DataBool interface {
	CustomDataType
	Get() bool
	Set(Value bool)
}

type DataInt interface {
	CustomDataType
	Get() int
	Set(Value int)
}

type DataReal interface {
	CustomDataType
	Get() float64
	Set(Value float64)
}

type DataString interface {
	CustomDataType
	Get() string
	Set(Value string)
}

type DataArray interface {
	CustomDataType
	Count() int
	Add(NewElement ...CustomDataType) int
	Insert(Index int, NewElement CustomDataType) int
	Remove(Index int)
	Get(Index int) CustomDataType
}

type DataDictionary interface {
	CustomDataType
	Count() int
	Add(Key string, Value CustomDataType)
	Delete(Key string)
	Value(Key string) CustomDataType
}

func DataObjectToString(obj CustomDataType) string {
	l := obj.getLength()
	m := make([]byte, l)
	obj.writeToBytes(m)
	return string(m)
}

// Null section
type dataNull struct{}

var storageNull = dataNull{}

func CreateNull() DataNull {
	return &storageNull
}

func (obj *dataNull) Copy() CustomDataType {
	return obj
}

func (*dataNull) isNull() {}

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

func CreateBool(val bool) DataBool {
	return &dataBool{val: val}
}

func (obj *dataBool) Get() bool {
	return obj.val
}

func (obj *dataBool) Set(Value bool) {
	obj.val = Value
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

func (obj *dataBool) Copy() CustomDataType {
	return CreateBool(obj.val)
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

func CreateInt(val int) DataInt {
	return &dataInt{val: val}
}

func (obj *dataInt) Get() int {
	return obj.val
}

func (obj *dataInt) Set(Value int) {
	obj.val = Value
}

func (obj *dataInt) getLength() int {
	if obj.val >= 0 {
		return getIntSize(obj.val)
	}
	return getIntSize(-obj.val) + 1
}

func (obj *dataInt) writeToBytes(b []byte) (int, error) {
	var tot [20]byte
	i := 0
	var k int
	if obj.val < 0 {
		k = -obj.val
	} else {
		k = obj.val
	}
	for k >= 10 {
		q := k / 10
		tot[i] = byte(k - q*10 + '0')
		k = q
		i++
	}
	tot[i] = byte(k + '0')

	ln := i
	if obj.val < 0 {
		ln++
	}

	if len(b) < ln+1 {
		return -1, errors.New("don't enougth space for store")
	}
	j := 0

	if obj.val < 0 {
		b[0] = '-'
		j++
	}
	for i >= 0 {
		b[j] = tot[i]
		j++
		i--
	}
	return ln + 1, nil

	/*str := strconv.Itoa(obj.val)
	if len(b) < len(str) {
		return -1, errors.New("don't enougth space for store")
	}
	return copy(b, []byte(str)), nil*/
}

func (obj *dataInt) Copy() CustomDataType {
	return CreateInt(obj.val)
}

// real section

func getRealSize(x float64) int {
	return len(strconv.FormatFloat(x, 'f', -1, 64))
}

type dataReal struct {
	val float64
}

func CreateReal(val float64) DataReal {
	return &dataReal{val: val}
}

func (obj *dataReal) Get() float64 {
	return obj.val
}

func (obj *dataReal) Set(Value float64) {
	obj.val = Value
}

func (obj *dataReal) getLength() int {
	return getRealSize(obj.val)
}

func (obj *dataReal) writeToBytes(b []byte) (int, error) {
	str := strconv.FormatFloat(obj.val, 'f', -1, 64)
	if len(b) < len(str) {
		return -1, errors.New("don't enougth space for store")
	}
	return copy(b, []byte(str)), nil
}

func (obj *dataReal) Copy() CustomDataType {
	return CreateReal(obj.val)
}

//  string section

type dataString struct {
	val string
}

func CreateString(str string) DataString {
	return &dataString{val: str}
}

var hex = []byte("01234567890abcdef")

func writeToBytes(str string, b []byte) (int, error) {
	lenb := len(b)
	src := []byte(str)
	if lenb < 2 {
		return -1, errors.New("don't enougth space for store")
	}
	b[0] = '"'
	offset := 1
	for _, ch := range src {
		switch ch {
		case 9:
			if lenb < offset+2 {
				return -1, errors.New("don't enougth space for store")
			}
			b[offset] = '\\'
			b[offset+1] = 't'
			offset += 2
		case 8:
			if lenb < offset+2 {
				return -1, errors.New("don't enougth space for store")
			}
			b[offset] = '\\'
			b[offset+1] = 'b'
			offset += 2
		case 10:
			if lenb < offset+2 {
				return -1, errors.New("don't enougth space for store")
			}
			b[offset] = '\\'
			b[offset+1] = 'n'
			offset += 2
		case 12:
			if lenb < offset+2 {
				return -1, errors.New("don't enougth space for store")
			}
			b[offset] = '\\'
			b[offset+1] = 'f'
			offset += 2
		case 13:
			if lenb < offset+2 {
				return -1, errors.New("don't enougth space for store")
			}
			b[offset] = '\\'
			b[offset+1] = 'r'
			offset += 2
		case '/':
			if lenb < offset+2 {
				return -1, errors.New("don't enougth space for store")
			}
			b[offset] = '\\'
			b[offset+1] = '/'
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
			if ch < 0x1f {
				if lenb < offset+6 {
					return -1, errors.New("don't enougth space for store")
				}
				b[offset] = '\\'
				b[offset+1] = 'u'
				b[offset+2] = '0'
				b[offset+3] = '0'
				b[offset+4] = hex[ch>>4]
				b[offset+5] = hex[ch&0xf]
				offset += 6
			} else {
				if lenb < offset+1 {
					return -1, errors.New("don't enougth space for store")
				}
				b[offset] = ch
				offset++
			}

		}
	}
	if lenb < offset+1 {
		return -1, errors.New("don't enougth space for store")
	}
	b[offset] = '"'
	return offset + 1, nil
}

func (obj *dataString) writeToBytes(b []byte) (int, error) {
	return writeToBytes(obj.val, b)
}

func getLength(str string) int {
	cnt := 0
	d := []byte(str)
	for _, c := range d {
		switch c {
		case 8, 9, 10, 12, 13, '"', '\\', '/':
			cnt += 2
		default:
			if c < 0x1f {
				cnt += 6
			} else {
				cnt++
			}
		}
	}
	return cnt + 2
}

func (obj *dataString) getLength() int {
	return getLength(obj.val)
}

func (obj *dataString) Get() string {
	return obj.val
}
func (obj *dataString) Set(Value string) {
	obj.val = Value
}

func (obj *dataString) Copy() CustomDataType {
	return CreateString(obj.val)
}

// array section
type dataArray struct {
	list []CustomDataType
	cnt  int
}

func CreateArray(initialSize int) DataArray {
	return &dataArray{
		list: make([]CustomDataType, initialSize),
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
	cnt := 2
	for i := 0; i < obj.cnt; i++ {
		cnt += obj.list[i].getLength()
	}
	if obj.cnt > 1 {
		cnt += (obj.cnt - 1) * 2
	}
	return cnt
}

func (obj *dataArray) Count() int {
	return obj.cnt
}

func (obj *dataArray) Add(NewElements ...CustomDataType) int {
	i := 0
	for _, item := range NewElements {
		i = obj.Insert(obj.cnt, item)
	}
	return i
}

func (obj *dataArray) setCapacity(newCapacity int) {
	tmp := make([]CustomDataType, newCapacity)
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

func (obj *dataArray) Insert(Index int, NewElement CustomDataType) int {
	if obj.cnt == len(obj.list) {
		obj.grow()
	}
	cnt := obj.cnt
	if Index < 0 || Index >= cnt {
		obj.list[cnt] = NewElement
		obj.cnt++
		return cnt
	}
	copy(obj.list[Index+1:], obj.list[Index:])
	obj.list[Index] = NewElement
	obj.cnt++
	return Index
}

func (obj *dataArray) Remove(Index int) {
	if Index < 0 || Index >= obj.cnt {
		return
	}
	copy(obj.list[Index:], obj.list[Index+1:])
	obj.cnt--
}

func (obj *dataArray) Get(Index int) CustomDataType {
	if Index < 0 || Index >= len(obj.list) {
		return &storageNull
	}
	return obj.list[Index]
}

func (obj *dataArray) Copy() CustomDataType {
	l := len(obj.list)
	tmp := CreateArray(l)
	for i := 0; i < l; i++ {
		tmp.Add(obj.list[i].Copy())
	}
	return tmp
}

// dictionary section

type dataDictionary struct {
	dict map[string]CustomDataType
}

func CreateDictionary(initialSize int) DataDictionary {
	return &dataDictionary{
		dict: make(map[string]CustomDataType, initialSize),
	}
}

func (obj *dataDictionary) writeToBytes(b []byte) (int, error) {
	lenb := len(b)
	if lenb < 1 {
		return -1, errors.New("don't enougth space for store")
	}
	b[0] = '{'
	offset := 1
	i := 0
	maplen := len(obj.dict)
	for k, v := range obj.dict {
		off, err := writeToBytes(k, b[offset:])
		if err != nil {
			return 0, err
		}
		offset += off
		if lenb < offset+1 {
			return -1, errors.New("don't enougth space for store")
		}
		b[offset] = ':'
		offset++
		off, err = v.writeToBytes(b[offset:])
		if err != nil {
			return -1, err
		}
		offset += off
		if i < maplen-1 {
			if lenb < offset+2 {
				return -1, errors.New("don't enougth space for store")
			}
			b[offset] = ','
			b[offset+1] = ' '
			offset += 2
		}
		i++
	}
	if lenb < offset+1 {
		return -1, errors.New("don't enougth space for store")
	}
	b[offset] = '}'
	return offset + 1, nil

}

func (obj *dataDictionary) getLength() int {
	cnt := 2
	for k, v := range obj.dict {
		cnt += getLength(k) + 1 + v.getLength()
	}
	if len(obj.dict) > 1 {
		cnt += (len(obj.dict) - 1) * 2
	}
	return cnt
}

func (obj *dataDictionary) Count() int {
	return len(obj.dict)
}
func (obj *dataDictionary) Add(Key string, Value CustomDataType) {
	_, isNull := Value.(DataNull)
	if isNull {
		obj.Delete(Key)
		return
	}
	obj.dict[Key] = Value
}

func (obj *dataDictionary) Delete(Key string) {
	delete(obj.dict, Key)
}
func (obj *dataDictionary) Value(Key string) CustomDataType {
	Value, ok := obj.dict[Key]
	if !ok {
		return CreateNull()
	}
	return Value
}

func (obj *dataDictionary) Copy() CustomDataType {
	l := len(obj.dict)
	tmp := CreateDictionary(l)
	for k, v := range obj.dict {
		tmp.Add(k, v.Copy())
	}
	return tmp
}

func ConvertToRASP(data CustomDataType) []byte {
	switch value := data.(type) {
	case DataNull:
		return []byte("$-1\r\n")
	case *dataString:
		return []byte("$" + strconv.Itoa(len(value.val)) + "\r\n" + value.val + "\r\n")
	case *dataInt:
		return []byte(":" + strconv.Itoa(value.val) + "\r\n")
	case *dataArray:
		a := bytes.NewBufferString("*" + strconv.Itoa(value.cnt) + "\r\n")
		for i := 0; i < value.cnt; i++ {
			a.Write(ConvertToRASP(value.list[i]))
		}
		return a.Bytes()
	}
	return []byte("")
}

func ConvertCommandToRASP(Command string, arguments ...CustomDataType) []byte {
	arr := CreateArray(1 + len(arguments))
	arr.Add(CreateString(Command))
	arr.Add(arguments...)
	return ConvertToRASP(arr)
}
