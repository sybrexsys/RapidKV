package main

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/sybrexsys/RapidKV/datamodel"
)

func TestStartStop(t *testing.T) {
	database := CreateDatabase(32)
	database.Close()
}

func TestGetSet(t *testing.T) {
	database := CreateDatabase(10)
	database.SetValue("100", datamodel.CreateString("test"), 0, 0)
	keys := datamodel.DataObjectToString(database.GetKeys("*"))
	if keys != `["100"]` {
		t.Fatal("invalid key was found")
	}
	get, a := database.GetValue("100")
	if !a {
		t.Fatal("value was not found")
	}
	res := datamodel.DataObjectToString(get)
	if res != `"test"` {
		t.Fatal("invalid value by key was found")
	}
	database.Close()
}

func BenchmarkSetGet(b *testing.B) {
	treats := b.N
	server := CreateDatabase(32)
	var group sync.WaitGroup
	group.Add(treats + 1)
	go func() {
		for i := 0; i < treats; i++ {
			go func(a int) {
				for i := 0; i < 1000; i++ {
					server.SetValue("test"+strconv.Itoa(a<<32+i), datamodel.CreateInt(a<<32+i), 0, 0)
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

func TrestTTL(t *testing.T) {
	treats := 50
	server := CreateDatabase(32)
	var group sync.WaitGroup
	group.Add(treats + 1)
	go func() {
		for i := 0; i < treats; i++ {
			go func(a int) {
				for i := 0; i < 100; i++ {
					key := "test" + strconv.Itoa(a<<32+i)
					server.SetValue(key, datamodel.CreateInt(a<<32+i), 0, 0)
					server.SetTTL(key, 100*a)
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
