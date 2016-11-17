package server

import (
	"crypto/sha1"
	"sync"
	"time"

	"github.com/sybrexsys/RapidKV/common"
)

type kvElementh struct {
	key   string
	value common.DataCommon
	ttl   time.Duration
}

type keyHash [sha1.BlockSize]byte

type kvStorage struct {
	sync.RWMutex
	mapkv map[keyHash]kvElementh
}

type kvBase struct {
	sync.RWMutex
	list map[int]*kvStorage
}
