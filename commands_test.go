package main

import (
	"bufio"
	"testing"

	"bytes"

	"fmt"
	"github.com/sybrexsys/RapidKV/datamodel"
	"strconv"
	"sync"
	"time"
)

var commands = []string{

	"*2\r\n$3\r\nget\r\n$-1\r\n",
	`"ERR Unknown parameter"`,

	"set 100 100",
	`"OK"`,

	"set 200 100 ex 100",
	`"OK"`,

	"TTL 200",
	"99",

	":1",
	`"ERR Invalid command"`,

	"keys",
	`"ERR Unknown parameter"`,

	"keys *",
	`["100", "200"]`,

	"type 100",
	`"string"`,

	"type 150",
	`"none"`,

	"*2\r\n$4\r\ntype\r\n$-1\r\n",
	`"ERR Unknown parameter"`,

	"*2\r\n$3\r\ndel\r\n$-1\r\n",
	`"ERR Unknown parameter"`,

	"del 150",
	"0",

	"del 100",
	"1",

	"*2\r\n$6\r\nexists\r\n$-1\r\n",
	`"ERR Unknown parameter"`,

	"exists 150",
	"0",

	"exists 200",
	"1",

	"rename",
	`"ERR Unknown parameter"`,

	"rename 100",
	`"ERR Unknown parameter"`,

	"rename 150, 111",
	`"ERR no such key"`,

	"rename 200 100",
	`"OK"`,

	"renamenx",
	`"ERR Unknown parameter"`,

	"renamenx 100",
	`"ERR Unknown parameter"`,

	"renamenx 150, 111",
	`"ERR no such key"`,

	"set 250 100",
	`"OK"`,

	"renamenx 100 250",
	`0`,

	"renamenx 100 220",
	`1`,

	"persist",
	`"ERR Unknown parameter"`,

	"persist 20",
	`0`,

	"persist 250",
	`0`,

	"ttl",
	`"ERR Unknown parameter"`,

	"ttl 2",
	`-2`,

	"ttl 250",
	`-1`,

	"persist 220",
	`1`,

	"pttl",
	`"ERR Unknown parameter"`,

	"pttl 2",
	`-2`,

	"pttl 250",
	`-1`,

	"select 1",
	`"OK"`,

	"set 220 100",
	`"OK"`,

	"exists 100",
	"0",

	"select 0",
	`"OK"`,

	"move",
	`"ERR Unknown parameter"`,

	"move 1",
	`"ERR Unknown parameter"`,

	"move 250 1",
	`1`,

	"move 220 1",
	`0`,

	"set 1 1",
	`"OK"`,

	"expire",
	`"ERR Unknown parameter"`,

	"expire key a",
	`"ERR Unknown parameter"`,

	"expire 220 -1",
	"1",

	"expire 220 -1",
	"0",

	"expire 1 1",
	"1",

	"expire 220 1",
	"0",

	"set 1 1",
	`"OK"`,

	"set 220 1",
	`"OK"`,

	"pexpire 220 -1",
	"1",

	"pexpire 220 -1",
	"0",

	"pexpire 1 1",
	"1",

	"pexpire 220 1",
	"0",

	"pexpire",
	`"ERR Unknown parameter"`,

	"pexpire key a",
	`"ERR Unknown parameter"`,

	"set 1 1",
	`"OK"`,

	"set 220 1",
	`"OK"`,

	"pexpireat 220 -1",
	"1",

	"pexpireat 220 -1",
	"0",

	"pexpireat 1 1",
	"1",

	"pexpireat 220 1",
	"0",

	"pexpireat",
	`"ERR Unknown parameter"`,

	"pexpireat key a",
	`"ERR Unknown parameter"`,

	"set 1 1",
	`"OK"`,

	"set 220 1",
	`"OK"`,

	"expireat 220 -1",
	"1",

	"expireat 220 -1",
	"0",

	"expireat 1 1",
	"1",

	"expireat 220 1",
	"0",

	"expireat",
	`"ERR Unknown parameter"`,

	"expireat key a",
	`"ERR Unknown parameter"`,

	"select",
	`"ERR Invalid parameter"`,

	"ping",
	`"PONG"`,

	"ping test",
	`"test"`,

	"ping 1",
	`"1"`,

	"echo",
	`"ERR Invalid parameter"`,

	"echo test",
	`"test"`,

	"multi",
	`"OK"`,

	"set 2 2",
	`"QUEUED"`,
	"set 3 3",
	`"QUEUED"`,
	"set 4 4",
	`"QUEUED"`,

	"exec",
	`["OK", "OK", "OK"]`,

	"get 2",
	`"2"`,

	"multi",
	`"OK"`,

	"set 5 2",
	`"QUEUED"`,
	"set 6 3",
	`"QUEUED"`,
	"set 7 4",
	`"QUEUED"`,
	"discard",
	`"OK"`,

	"*0\r\n",
	`"ERR Invalid command"`,
	"*1\r\n:1\r\n",
	`"ERR Invalid command"`,

	"quit",
	`"OK"`,

	"append",
	`"ERR Unknown parameter"`,

	"append 30",
	`"ERR Unknown parameter"`,

	"append 30 Hello",
	`5`,

	`append 30 " world"`,
	`11`,

	`set`,
	`"ERR Unknown parameter"`,

	`set 32`,
	`"ERR Unknown parameter"`,

	`set 32 33 nx xx`,
	`"ERR Syntax error"`,

	`set 32 33 test`,
	`"ERR Unknown parameter"`,

	`set 32 33 ex`,
	`"ERR Syntax error"`,

	`set 32 33 ex test`,
	`"ERR Syntax error"`,

	`set 32 33 ex -100`,
	`"ERR Syntax error"`,

	`set 32 33 ex 100`,
	`"OK"`,

	`set 32 33 px`,
	`"ERR Syntax error"`,

	`set 32 33 px test`,
	`"ERR Syntax error"`,

	`set 32 33 px -100`,
	`"ERR Syntax error"`,

	`set 32 33 px 100`,
	`"OK"`,

	`set key1 "Hello"`,
	`"OK"`,

	`set key2 "World"`,
	`"OK"`,

	`mget key1 key2 nonexisting`,
	`["Hello", "World", null]`,

	`mset key1 key2 nonexisting`,
	`"ERR Wrong count of the parameters"`,

	`mset key1 key2 nonexisting existing`,
	`"OK"`,

	`get a100`,
	`null`,

	"*2\r\n$4\r\nmget\r\n*0\r\n",
	`[null]`,

	"*4\r\n$3\r\nset\r\n$3\r\nset\r\n$3\r\nset\r\n*0\r\n",
	`"ERR Unknown parameter"`,

	`strlen a100`,
	`0`,

	`strlen`,
	`"ERR Unknown parameter"`,

	`strlen key2`,
	`5`,

	`incr`,
	`"ERR Unknown parameter"`,

	`decr`,
	`"ERR Unknown parameter"`,

	`incrby`,
	`"ERR Unknown parameter"`,

	`decrby`,
	`"ERR Unknown parameter"`,

	`incrby q`,
	`"ERR Unknown parameter"`,

	`decrby q`,
	`"ERR Unknown parameter"`,

	`incr key0`,
	`1`,

	`get key0`,
	`"1"`,

	`incr key0`,
	`2`,

	`get key0`,
	`"2"`,

	`decr keyz`,
	`-1`,

	`get keyz`,
	`"-1"`,

	`incr keyz`,
	`0`,

	`decr keyz`,
	`-1`,

	`get keyz`,
	`"-1"`,

	`incrby ey0 10`,
	`10`,

	`get ey0`,
	`"10"`,

	`incrby ey0 -1`,
	`9`,

	`get ey0`,
	`"9"`,

	`decrby eyz 100`,
	`-100`,

	`get eyz`,
	`"-100"`,

	`decr eyz`,
	`-101`,

	`decrby eyz -1`,
	`-100`,

	`get eyz`,
	`"-100"`,

	`msetnx 121 1 22 2`,
	`1`,

	`msetnx 21 1 22 2`,
	`0`,

	`msetnx 1 1 2 `,
	`"ERR Wrong count of the parameters"`,

	`setnx 122 1`,
	`1`,

	`setnx 122 22`,
	`0`,

	`setnx`,
	`"ERR Unknown parameter"`,

	`setnx 122 `,
	`"ERR Unknown parameter"`,

	`getset 22 1`,
	`"2"`,

	`getset 2s2 1`,
	`null`,

	`getset`,
	`"ERR Unknown parameter"`,

	`getset 122 `,
	`"ERR Unknown parameter"`,

	`setex`,
	`"ERR Unknown parameter"`,

	`setex 122 22`,
	`"ERR Syntax error"`,

	`setex 122z zzz sss`,
	`"ERR Syntax error"`,

	`setex 122 -122 zzz`,
	`"ERR Syntax error"`,

	`setex 122s 10 zzz`,
	`"OK"`,

	`psetex`,
	`"ERR Unknown parameter"`,

	`psetex 122 22`,
	`"ERR Syntax error"`,

	`psetex 122z zzz sss`,
	`"ERR Syntax error"`,

	`psetex 122 -122 zzz`,
	`"ERR Syntax error"`,

	`psetex 122s 10 zzz`,
	`"OK"`,

	`lpush zaq 1 2 3`,
	`3`,

	`rpush zaqz 1 2 3`,
	`3`,

	`rpush`,
	`"ERR Unknown parameter"`,

	`rpush 122s 1 2 3`,
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	`lpush`,
	`"ERR Unknown parameter"`,

	`lpush 122s 1 2 3`,
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	`lpop key0`,
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	`rpop key0`,
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	`rpop`,
	`"ERR Unknown parameter"`,
	`lpop`,
	`"ERR Unknown parameter"`,

	`rpop zaq`,
	`"1"`,

	`lrange zaq 0 -1`,
	`["3", "2"]`,

	`lpop zaq`,
	`"3"`,

	`lrange zaq 0 -1`,
	`["2"]`,

	`rpop zad`,
	`null`,

	`lpop zad`,
	`null`,

	`llen`,
	`"ERR Unknown parameter"`,

	`llen key0`,
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	`llen zaq`,
	`1`,

	`llen zadzssss`,
	`0`,

	"ltrim",
	`"ERR Unknown parameter"`,

	"ltrim zor",
	`"OK"`,

	`lrange`,
	`"ERR Unknown parameter"`,

	`lrange a`,
	`"ERR Unknown parameter"`,
	`lrange a b`,
	`"ERR Unknown parameter"`,
	`lrange a 1`,
	`"ERR Unknown parameter"`,
	`lrange a -100 -1`,
	`[]`,

	"rpush list 1 2 3 4 5 6 7",
	"7",
	`lrange list -100 -1`,
	`["1", "2", "3", "4", "5", "6", "7"]`,

	`lrange list 5 -1`,
	`["6", "7"]`,

	`lrange list 1005 -1`,
	`[]`,

	`lrange key0 1005 -1`,
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	`lrange list 5 100`,
	`["6", "7"]`,

	`lrange list 5 -100`,
	`[]`,

	"rpushx",
	`"ERR Unknown parameter"`,

	"rpushx key0 a",
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	"rpushx 1s 1 ",
	`0`,

	"rpushx list 1 ",
	`8`,

	"lpushx",
	`"ERR Unknown parameter"`,

	"lpushx key0 a",
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	"lpushx 1s 1 ",
	`0`,

	"lpushx list 1 ",
	`9`,

	

}

func Get(i int, t *testing.T) datamodel.CustomDataType {
	b := bytes.NewBufferString(commands[i] + "\r\n")
	bf := bufio.NewReader(b)
	res, err := datamodel.LoadRespFromIO(bf, true)
	if err != nil {
		t.Fatal("error was detected: " + err.Error())
	}
	return res
}

func TestCommands(t *testing.T) {
	databases = make(map[int]*Database)
	cfg = &defConfig
	firstDatabase = CreateDatabase(defConfig.ShardCount)
	databases[0] = firstDatabase
	cc := &clientConnection{
		answers:         make([]datamodel.CustomDataType, 100),
		answersSize:     0,
		currentDatabase: firstDatabase,
		authorized:      !needAuth,
	}
	for i := 0; i < len(commands); i += 2 {
		res := cc.processOneRESPCommand(Get(i, t))
		//	fmt.Println(commands[i])
		s := datamodel.DataObjectToString(res)
		if s != commands[i+1] {
			t.Fatalf("invalid answer. Step: %d \rSource:%s\rWait  : %s\rResult: %s\r", i/2, commands[i], commands[i+1], s)
		}
	}
}

func GetBench(str string, bn *testing.B) datamodel.CustomDataType {
	b := bytes.NewBufferString(str + "\r\n")
	bf := bufio.NewReader(b)
	res, err := datamodel.LoadRespFromIO(bf, true)
	if err != nil {
		bn.Fatal("error was detected: " + err.Error())
	}
	return res
}

func BenchmarkProcessOneThread(b *testing.B) {
	databases = make(map[int]*Database)
	cfg = &defConfig
	firstDatabase = CreateDatabase(defConfig.ShardCount)
	databases[0] = firstDatabase
	cc := &clientConnection{
		answers:         make([]datamodel.CustomDataType, 100),
		answersSize:     0,
		currentDatabase: firstDatabase,
		authorized:      !needAuth,
	}
	t := time.Now()
	for i := 0; i < b.N; i++ {
		command := "set " + strconv.Itoa(i) + " " + strconv.Itoa(i)
		cc.processOneRESPCommand(GetBench(command, b))
	}
	fmt.Println(b.N, "    ", time.Since(t))
	for _, db := range databases {
		db.Close()
	}
}

func BenchmarkProcessLotOfThread(b *testing.B) {
	var wg sync.WaitGroup
	databases = make(map[int]*Database)
	cfg = &defConfig
	firstDatabase = CreateDatabase(defConfig.ShardCount)
	databases[0] = firstDatabase
	cc := &clientConnection{
		answers:         make([]datamodel.CustomDataType, 100),
		answersSize:     0,
		currentDatabase: firstDatabase,
		authorized:      !needAuth,
	}
	wg.Add(b.N)
	t := time.Now()
	for i := 0; i < b.N; i++ {
		go func(cnt int) {
			for i := 0; i < 1000; i++ {
				command := "set " + strconv.Itoa(cnt<<32+i) + " " + strconv.Itoa(i)
				cc.processOneRESPCommand(GetBench(command, b))
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	fmt.Println(b.N, "    ", firstDatabase.GetCount(), "  ", time.Since(t))
	for _, db := range databases {
		db.Close()
	}
}
