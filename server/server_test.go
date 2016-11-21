package server

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/sybrexsys/RapidKV/datamodel"
)

func TestStartStop(t *testing.T) {
	server := createServer()
	server.Close()
}

func TestSetGet(t *testing.T) {
	treats := 500
	server := createServer()
	var group sync.WaitGroup
	group.Add(treats + 1)
	go func() {
		for i := 0; i < treats; i++ {
			go func(a int) {
				for i := 0; i < 1000; i++ {
					//_, err :=
					server.SetValue("test"+strconv.Itoa(a*i%10000), datamodel.CreateInt(a<<32+i), int64(a))
					//					if err != nil {
					//						t.Fatal(err.Error)
					//					}
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
	server := createServer()
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
	fmt.Printf("Server consist %d records\n", server.GetCount())
	server.Close()
}
