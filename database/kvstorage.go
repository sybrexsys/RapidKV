package database

import (
	"hash/crc64"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sybrexsys/RapidKV/datamodel"
)

type ProcessError string

func (conerr ProcessError) Error() string { return string(conerr) }

const defShardCount = 32

const ttlCheckPeriod = time.Millisecond * 5000

type Element struct {
	Value datamodel.CustomDataType
	Ttl   time.Duration
	Ttc   time.Duration
}

type ElementCopy struct {
	key  string
	data *Element
}

var startTime = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

const maxTime = time.Duration(0x7FFFFFFFFFFFFFFF)

var crc64Table = crc64.MakeTable(crc64.ECMA)

type shardElem struct {
	sync.RWMutex
	mapkv             map[string]*Element
	ttlMutex          sync.Mutex
	nextTTLProcessing int64
	quit              chan struct{}
	parent            *Database
	id                int
}

type Database struct {
	sync.RWMutex
	baseList   []*shardElem
	group      *sync.WaitGroup
	shardCount int
}

func crc(Key string) uint {
	return uint(crc64.Checksum([]byte(Key), crc64Table))
}

func (shard *shardElem) ttlChecker() {
	shard.parent.RLock()
	defer shard.parent.RUnlock()
	shard.Lock()
	defer shard.Unlock()
	ct := time.Since(startTime)
	nextttl := maxTime
	for k, v := range shard.mapkv {
		if v.Ttl < ct {
			delete(shard.mapkv, k)
		} else {
			if nextttl > v.Ttl {
				nextttl = v.Ttl
			}
		}
	}
	shard.nextTTLProcessing = int64(nextttl)
}

func (shard *shardElem) ttlloop(group *sync.WaitGroup) {
	ticker := time.NewTicker(ttlCheckPeriod).C
loop:
	for {
		select {
		case _ = <-ticker:
			next := atomic.LoadInt64(&shard.nextTTLProcessing)
			if next < int64(time.Since(startTime)) {
				shard.ttlChecker()
			}
		case _ = <-shard.quit:
			break loop
		}
	}
	group.Done()
}

func CreateDatabase(shardCount int) *Database {
	if shardCount <= 0 && shardCount > 1024 {
		shardCount = defShardCount
	}
	db := &Database{
		group:      &sync.WaitGroup{},
		baseList:   make([]*shardElem, shardCount),
		shardCount: shardCount,
	}
	db.group.Add(shardCount)
	for i := 0; i < shardCount; i++ {
		shred := &shardElem{
			mapkv:             make(map[string]*Element, 256),
			nextTTLProcessing: 0x7FFFFFFFFFFFFFFF,
			quit:              make(chan struct{}),
			parent:            db,
			id:                i,
		}
		db.baseList[i] = shred
		go shred.ttlloop(db.group)
	}
	return db
}

func (db *Database) Close() {
	for i := 0; i < db.shardCount; i++ {
		db.baseList[i].quit <- struct{}{}
	}
	db.group.Wait()
}

func (db *Database) keyToShard(Key string) *shardElem {
	shardidx := crc(Key) % uint(db.shardCount)
	return db.baseList[shardidx]
}

const (
	SetAny = iota
	SetIfExists
	SetIfNotExists
)

func (db *Database) SetValue(Key string, Value datamodel.CustomDataType, state int, ttl int) (datamodel.CustomDataType, bool) {
	shard := db.keyToShard(Key)
	shard.Lock()
	defer shard.Unlock()
	var t time.Duration
	if ttl > 0 {
		t = time.Since(startTime) + time.Duration(ttl)*time.Millisecond
	} else {
		t = maxTime
	}

	elem, ok := shard.mapkv[Key]
	if ok {
		if state == SetIfNotExists {
			return nil, false
		}
		oldValue := elem.Value
		elem.Value = Value
		elem.Ttc = time.Since(startTime)
		elem.Ttl = t
		return oldValue, true
	}
	if state == SetIfNotExists {
		return nil, false
	}

	elem = &Element{
		Value: Value,
		Ttc:   time.Since(startTime),
		Ttl:   t,
	}
	next := atomic.LoadInt64(&shard.nextTTLProcessing)
	if next > int64(elem.Ttl) {
		if shard.nextTTLProcessing > int64(elem.Ttl) {
			atomic.StoreInt64(&shard.nextTTLProcessing, int64(elem.Ttl))
		}
	}
	shard.mapkv[Key] = elem
	return nil, true
}

func (db *Database) Del(Key string) bool {
	shard := db.keyToShard(Key)
	shard.Lock()
	defer shard.Unlock()
	_, ok := shard.mapkv[Key]
	if !ok {
		return false
	}
	delete(shard.mapkv, Key)
	return true
}

func (db *Database) Move(Key string, newDB *Database) bool {
	shard := db.keyToShard(Key)
	shard.Lock()
	defer shard.Unlock()
	value, ok := shard.mapkv[Key]
	if !ok {
		return false
	}
	newDB.RLock()
	defer newDB.RUnlock()
	shardnew := newDB.keyToShard(Key)
	shardnew.Lock()
	defer shardnew.Unlock()
	_, ok = shardnew.mapkv[Key]
	if ok {
		return false
	}
	delete(shard.mapkv, Key)
	shardnew.mapkv[Key] = value
	return true
}

func (db *Database) GetValue(Key string) (datamodel.CustomDataType, bool) {
	shard := db.keyToShard(Key)
	shard.RLock()
	defer shard.RUnlock()
	el, ok := shard.mapkv[Key]
	if !ok {
		return nil, ok
	}
	return el.Value, ok
}

func AddKeysFromShard(shard *shardElem, array datamodel.DataArray, pattern string) error {
	shard.RLock()
	defer shard.RUnlock()
	for key := range shard.mapkv {
		ok, err := Match(pattern, key)
		if err != nil {
			return err
		}
		if ok {
			array.Add(datamodel.CreateString(key))
		}
	}
	return nil
}

func (db *Database) GetKeys(pattern string) datamodel.CustomDataType {
	arr := datamodel.CreateArray(10)
	for i := 0; i < db.shardCount; i++ {
		shard := db.baseList[i]
		if err := AddKeysFromShard(shard, arr, pattern); err != nil {
			return datamodel.CreateError(err.Error())
		}
	}
	return arr
}

func (db *Database) SetTTL(Key string, TTL int) bool {
	shard := db.keyToShard(Key)
	shard.Lock()
	defer shard.Unlock()
	elem, ok := shard.mapkv[Key]
	if !ok {
		return false
	}
	if TTL > 0 {
		elem.Ttl = time.Since(startTime) + time.Millisecond*time.Duration(TTL)
	} else {
		elem.Ttl = time.Millisecond * time.Duration(-TTL)
	}
	if elem.Ttl < time.Since(startTime) {
		delete(shard.mapkv, Key)
	} else {
		next := atomic.LoadInt64(&shard.nextTTLProcessing)
		if next > int64(elem.Ttl) {
			if shard.nextTTLProcessing > int64(elem.Ttl) {
				atomic.StoreInt64(&shard.nextTTLProcessing, int64(elem.Ttl))
			}
		}
	}
	return true
}

func (db *Database) GetTTL(Key string) (int, bool) {
	shard := db.keyToShard(Key)
	shard.RLock()
	defer shard.RUnlock()
	elem, ok := shard.mapkv[Key]
	if !ok {
		return -1, false
	}
	if elem.Ttl == maxTime {
		return -1, true
	}
	return int((elem.Ttl - time.Since(startTime)) / time.Millisecond), true
}

func (db *Database) Rename(OldKey, NewKey string, override bool) (int, bool) {
	oldshard := db.keyToShard(OldKey)
	oldshard.Lock()
	defer oldshard.Unlock()
	elem, ok := oldshard.mapkv[OldKey]
	if !ok {
		return 0, false
	}
	newshard := db.keyToShard(NewKey)
	newshard.Lock()
	defer newshard.Unlock()

	_, newok := newshard.mapkv[NewKey]
	if newok && !override {
		return 0, true
	}
	newshard.mapkv[NewKey] = elem
	delete(oldshard.mapkv, OldKey)
	return 1, true
}

func (db *Database) SkipTTL(Key string) bool {
	shard := db.keyToShard(Key)
	shard.Lock()
	defer shard.Unlock()
	elem, ok := shard.mapkv[Key]
	if !ok {
		return false
	}
	if elem.Ttl == maxTime {
		return false
	}
	elem.Ttl = maxTime
	return true
}

func (db *Database) GetCount() int {
	cnt := int(0)
	for i := 0; i < db.shardCount; i++ {
		db.baseList[i].RLock()
		cnt += len(db.baseList[i].mapkv)
		db.baseList[i].RUnlock()
	}
	return cnt
}

func (shard *shardElem) copyShard() []ElementCopy {
	shard.Lock()
	defer shard.Unlock()
	tmp := make([]ElementCopy, len(shard.mapkv))
	i := 0
	for k, v := range shard.mapkv {
		tmp[i].key = k
		tmp[i].data = v
	}
	return tmp
}

const (
	TypeString = iota
	TypeList
	TypeHash
)

func (db *Database) ProcessValue(Key string, needCreate bool,
	f func(elem *Element) (datamodel.CustomDataType, bool)) datamodel.CustomDataType {
	shard := db.keyToShard(Key)
	shard.Lock()
	defer shard.Unlock()
	el, ok := shard.mapkv[Key]
	if ok {
		res, _ := f(el)
		return res
	}
	if !needCreate {
		return datamodel.CreateError("ERR Key not found")
	}

	el = &Element{
		Ttc: time.Since(startTime),
		Ttl: maxTime,
	}
	res, add := f(el)
	if add {
		shard.mapkv[Key] = el
	}
	return res
}
