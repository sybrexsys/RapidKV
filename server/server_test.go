package server

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	"path/filepath"
	"time"

	"github.com/sybrexsys/RapidKV/datamodel"
)

func TestStartStop(t *testing.T) {
	server := CreateServer()
	server.Close()
}

func BenchmarkSetGet(b *testing.B) {
	treats := b.N
	server := CreateServer()
	var group sync.WaitGroup
	group.Add(treats + 1)
	go func() {
		for i := 0; i < treats; i++ {
			go func(a int) {
				for i := 0; i < 1000; i++ {
					server.SetValue("test"+strconv.Itoa(a<<32+i), datamodel.CreateInt(a<<32+i), int64(a))
				}
				group.Done()
			}(i)
		}
		group.Done()
	}()
	group.Wait()
	fmt.Printf("Server consist %d records\n", server.GetCount())
	server.Close()
}

func TestTTL(t *testing.T) {
	treats := 50
	server := CreateServer()
	var group sync.WaitGroup
	group.Add(treats + 1)
	go func() {
		for i := 0; i < treats; i++ {
			go func(a int) {
				for i := 0; i < 100; i++ {
					key := "test" + strconv.Itoa(a<<32+i)
					server.SetValue(key, datamodel.CreateInt(a<<32+i), int64(a))
					server.SetTTL(key, 100*a, int64(a))
				}
				group.Done()
			}(i)
		}
		group.Done()
	}()
	group.Wait()
	time.Sleep(500 * time.Millisecond)
	server.Close()
}

type sss struct {
	some int
}

func BenchmarkMove(b *testing.B) {
	ttt := make(map[string]*sss, b.N)
	for i := 0; i < b.N; i++ {
		ttt[strconv.Itoa(i)] = &sss{some: i}
	}
	s := make([]string, b.N)
	t := time.Now()
	i := 0
	for k, _ := range ttt {
		s[i] = k
		i++
	}
	fnd := 0
	for i := 0; i < b.N; i++ {
		if f, err := filepath.Match("1*", s[i]); f && err == nil {
			fnd++
		}
	}
	fmt.Println(b.N, "    ", time.Since(t), "   ", fnd)
}
