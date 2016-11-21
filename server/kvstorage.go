package server

import (
	"errors"
	"github.com/sybrexsys/RapidKV/datamodel"
	"hash/crc64"
	"sync"
	"sync/atomic"
	"time"
)

const shardCount = 256

const ttlCheckPeriod = time.Millisecond * 500

type kvElementh struct {
	value       datamodel.CustomDataType
	ttl         time.Duration
	ttc         time.Duration
	lockSession time.Duration
	lockttc     time.Duration
	lock        int32
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

func crc(Key string) int {
	return int(crc64.Checksum([]byte(Key), crc64Table))
}

func (shard *shardElem) ttlChecker() {
	shard.Lock()
	defer shard.Unlock()
	ct := time.Since(startTime)
	nextttl := maxTime
	for k, v := range shard.mapkv {
		if (v.ttl < ct) && (v.lock == 0) {
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
	group.Add(1)
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

func createServer() *serverKV {
	server := &serverKV{group: &sync.WaitGroup{}}
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

func (server *serverKV) SetValue(Key string, Value datamodel.CustomDataType, Session time.Duration) (*kvElementh, error) {
	elem, ok := server.GetValue(Key)
	if ok {
		if !atomic.CompareAndSwapInt32(&elem.lock, 0, 1) {
			if elem.lockSession != Session {
				return nil, errors.New("record busy")
			}
		} else {
			defer atomic.StoreInt32(&elem.lock, 0)
		}
		elem.value = Value
		elem.ttc = time.Since(startTime)
		elem.ttl = maxTime
		return elem, nil
	}
	shard := server.keyToShard(Key)
	shard.Lock()
	defer shard.Unlock()
	// check on append
	elem, ok = shard.mapkv[Key]
	if ok {
		if !atomic.CompareAndSwapInt32(&elem.lock, 0, 1) {
			return nil, errors.New("record busy")
		}
		defer atomic.StoreInt32(&elem.lock, 0)
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

func (server *serverKV) GetValue(Key string) (*kvElementh, bool) {
	shard := server.keyToShard(Key)
	shard.RLock()
	defer shard.RUnlock()
	el, ok := shard.mapkv[Key]
	return el, ok
}

func (server *serverKV) LockValue(Key string, Session time.Duration) error {
	shard := server.keyToShard(Key)
	shard.RLock()
	defer shard.RUnlock()
	elem, ok := shard.mapkv[Key]
	if !ok {
		return errors.New("record not found")
	}
	if !atomic.CompareAndSwapInt32(&elem.lock, 0, 1) {
		if elem.lockSession != Session {
			return errors.New("record busy")
		}
	}
	return nil
}

func (server *serverKV) UnlockValue(Key string, Session time.Duration) error {
	shard := server.keyToShard(Key)
	shard.RLock()
	defer shard.RUnlock()
	elem, ok := shard.mapkv[Key]
	if !ok {
		return errors.New("record not found")
	}
	if !atomic.CompareAndSwapInt32(&elem.lock, 0, 1) {
		if elem.lockSession != Session {
			return errors.New("record busy")
		}
	}
	elem.ttc = time.Since(startTime)
	elem.ttl = maxTime
	atomic.StoreInt32(&elem.lock, 0)
	return nil
}

func (server *serverKV) SetTTL(Key string, TTL time.Duration, Session time.Duration) error {
	var needRLock bool
	shard := server.keyToShard(Key)
	shard.RLock()
	defer func() {
		if needRLock {
			shard.RUnlock()
		}
	}()
	elem, ok := shard.mapkv[Key]
	if !ok {
		return errors.New("record not found")
	}
	if !atomic.CompareAndSwapInt32(&elem.lock, 0, 1) {
		if elem.lockSession != Session {
			return errors.New("record busy")
		}
	}
	elem.ttl = elem.ttc + TTL
	if elem.ttl < time.Since(startTime) {
		shard.RUnlock()
		needRLock = false
		shard.Lock()
		defer shard.Unlock()
		delete(shard.mapkv, Key)
	} else {
		next := atomic.LoadInt64(&shard.nextTTLProcessing)
		if next > int64(elem.ttl) {
			shard.ttlMutex.Lock()
			defer shard.ttlMutex.Unlock()
			if shard.nextTTLProcessing > int64(elem.ttl) {
				shard.nextTTLProcessing = int64(elem.ttl)
			}
		}
	}
	return nil
}
