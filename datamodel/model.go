package datamodel

/*const (
    ModelType
)*/

type DataBase interface {
	getLength() int
	writeToBytes() error
}

type DataNull interface {
	DataBase
	IsNull()
}

type dataNull struct{}

var storageNull = dataNull{}

func (dataNull) IsNull() {}

func (dataNull) getLength() int { return 4 }

type dataBool struct {
	value bool
}

func (obj *dataBool) getLength() int {
	if obj.value {
		return 4
	}
	return 5
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
	Remove(Index int)
	Get(Index int) DataBase
}

type DataDictionary interface {
	DataBase
	Count() int
	Add(Key string, Value DataBase)
	Delete(Key string)
	Value(Key string) DataBase
}
