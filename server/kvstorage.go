package server

import (
	"errors"
	"hash/crc64"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sybrexsys/RapidKV/datamodel"
)

const shardCount = 128

const ttlCheckPeriod = time.Millisecond * 500

type kvElementh struct {
	sync.RWMutex
	value       datamodel.CustomDataType
	ttl         time.Duration
	ttc         time.Duration
	lockSession int64
}

var startTime = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

const maxTime = time.Duration(0x7FFFFFFFFFFFFFFF)

var crc64Table = crc64.MakeTable(crc64.ECMA)

type shardElem struct {
	sync.RWMutex
	mapkv             map[string]*kvElementh
	ttlMutex          sync.Mutex
	nextTTLProcessing int64
	quit              chan struct{}
}

type serverKV struct {
	baseList [shardCount]*shardElem
	group    *sync.WaitGroup
}

func crc(Key string) uint {
	return uint(crc64.Checksum([]byte(Key), crc64Table))
}

func (shard *shardElem) ttlChecker() {
	shard.Lock()
	defer shard.Unlock()
	ct := time.Since(startTime)
	nextttl := maxTime
	for k, v := range shard.mapkv {
		if v.lockSession != 0 {
			continue
		}
		if v.ttl < ct {
			delete(shard.mapkv, k)
		} else {
			if nextttl > v.ttl {
				nextttl = v.ttl
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
				//		shard.ttlChecker()
			}
		case _ = <-shard.quit:
			break loop
		}
	}
	group.Done()
}

func createServer() *serverKV {
	server := &serverKV{group: &sync.WaitGroup{}}
	server.group.Add(shardCount)
	for i := 0; i < shardCount; i++ {
		shred := &shardElem{
			mapkv:             make(map[string]*kvElementh, 256),
			nextTTLProcessing: 0x7FFFFFFFFFFFFFFF,
			quit:              make(chan struct{}),
		}
		server.baseList[i] = shred
		go shred.ttlloop(server.group)
	}
	return server
}

func (server *serverKV) Close() {
	for i := 0; i < shardCount; i++ {
		server.baseList[i].quit <- struct{}{}
	}
	server.group.Wait()
}

func (server *serverKV) keyToShard(Key string) *shardElem {
	shardidx := crc(Key) % shardCount
	return server.baseList[shardidx]
}

func (server *serverKV) SetValue(Key string, Value datamodel.CustomDataType, Session int64) (*kvElementh, error) {
	shard := server.keyToShard(Key)
	shard.Lock()
	defer shard.Unlock()
	elem, ok := shard.mapkv[Key]
	if ok {
		if elem.lockSession != 0 && elem.lockSession != Session {
			return nil, errors.New("record busy")
		}
		elem.value = Value
		elem.ttc = time.Since(startTime)
		elem.ttl = maxTime
		return elem, nil
	}
	elem = &kvElementh{
		value: Value,
		ttc:   time.Since(startTime),
		ttl:   maxTime,
	}
	shard.mapkv[Key] = elem
	return elem, nil
}

func (server *serverKV) GetValue(Key string) (datamodel.CustomDataType, bool) {
	shard := server.keyToShard(Key)
	shard.RLock()
	defer shard.RUnlock()
	el, ok := shard.mapkv[Key]
	return el.value, ok
}

func (server *serverKV) LockValue(Key string, Session int64) error {
	shard := server.keyToShard(Key)
	shard.Lock()
	defer shard.Unlock()
	elem, ok := shard.mapkv[Key]
	if !ok {
		return errors.New("record not found")
	}
	if elem.lockSession != 0 && elem.lockSession != Session {
		return errors.New("record busy")
	}
	elem.lockSession = Session
	return nil
}

func (server *serverKV) UnlockValue(Key string, Session int64) error {
	shard := server.keyToShard(Key)
	shard.Lock()
	defer shard.Unlock()
	elem, ok := shard.mapkv[Key]
	if !ok {
		return errors.New("record not found")
	}
	if elem.lockSession == 0 {
		return nil
	}
	if elem.lockSession != Session {
		return errors.New("record busy by other session")
	}
	elem.lockSession = 0
	elem.ttc = time.Since(startTime)
	elem.ttl = maxTime
	return nil
}

func (server *serverKV) SetTTL(Key string, TTL int, Session int64) error {
	shard := server.keyToShard(Key)
	shard.Lock()
	defer shard.Unlock()
	elem, ok := shard.mapkv[Key]
	if !ok {
		return errors.New("record not found")
	}
	if elem.lockSession != 0 && elem.lockSession != Session {
		return errors.New("record busy")
	}
	elem.ttl = elem.ttc + time.Millisecond*time.Duration(TTL)
	if elem.ttl < time.Since(startTime) {
		delete(shard.mapkv, Key)
	} else {
		next := atomic.LoadInt64(&shard.nextTTLProcessing)
		if next > int64(elem.ttl) {
			if shard.nextTTLProcessing > int64(elem.ttl) {
				atomic.StoreInt64(&shard.nextTTLProcessing, int64(elem.ttl))
			}
		}
	}
	return nil
}

func (server *serverKV) GetCount() int {
	cnt := int(0)
	for i := 0; i < shardCount; i++ {
		server.baseList[i].RLock()
		cnt += len(server.baseList[i].mapkv)
		server.baseList[i].RUnlock()
	}
	return cnt
}
