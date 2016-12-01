package client

import (
	"testing"
	"time"

	"strconv"

	"fmt"

	"github.com/sybrexsys/RapidKV/datamodel"
)

var commands = []string{

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

	"del 150",
	"0",

	"del 100",
	"1",

	"exists 150",
	"0",

	"exists 200",
	"1",

	"rename 100",
	`"ERR Unknown parameter"`,

	"rename 150, 111",
	`"ERR no such key"`,

	"rename 200 100",
	`"OK"`,

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

	"persist 20",
	`0`,

	"persist 250",
	`0`,

	"ttl 2",
	`-2`,

	"ttl 250",
	`-1`,

	"persist 220",
	`1`,

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

	"move 1",
	`"ERR Unknown parameter"`,

	"move 250 1",
	`1`,

	"move 220 1",
	`0`,

	"set 1 1",
	`"OK"`,

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

	"append 30",
	`"ERR Unknown parameter"`,

	"append 30 Hello",
	`5`,

	`append 30 " world"`,
	`11`,

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

	`set 32 33 px 100`,
	`"OK"`,

	`set 32 33 px`,
	`"ERR Syntax error"`,

	`set 32 33 px test`,
	`"ERR Syntax error"`,

	`set 32 33 px -100`,
	`"ERR Syntax error"`,

	`set key1 "Hello"`,
	`"OK"`,

	`set key2 "World"`,
	`"OK"`,

	`mget key1 key2 nonexisting`,
	`["Hello", "World", null]`,

	`exists key1 key2 nonexisting`,
	`2`,

	`mset key1 key2 nonexisting`,
	`"ERR Wrong count of the parameters"`,

	`mset key1 key2 nonexisting existing`,
	`"OK"`,

	`get a100`,
	`null`,

	`strlen a100`,
	`0`,

	`strlen key2`,
	`5`,

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

	`setnx 122 `,
	`"ERR Unknown parameter"`,

	`getset 22 1`,
	`"2"`,

	`getset 2s2 1`,
	`null`,

	`getset 122 `,
	`"ERR Unknown parameter"`,

	`setex 122 22`,
	`"ERR Syntax error"`,

	`setex 122z zzz sss`,
	`"ERR Syntax error"`,

	`setex 122 -122 zzz`,
	`"ERR Syntax error"`,

	`setex 122s 10 zzz`,
	`"OK"`,

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

	`rpush 122s 1 2 3`,
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	`lpush 122s 1 2 3`,
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	`lpop key0`,
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	`rpop key0`,
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

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

	`llen key0`,
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	`llen zaq`,
	`1`,

	`llen zadzssss`,
	`0`,

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

	"rpushx key0 a",
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	"rpushx 1s 1 ",
	`0`,

	"rpushx list 1 ",
	`8`,

	"lpushx key0 a",
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	"lpushx 1s 1 ",
	`0`,

	"lpushx list 1 ",
	`9`,

	"mset q1 q1 q2 q2 q3 q3 ",
	`"OK"`,

	"del q1 q2 q3",
	"3",

	"lpush t1 1",
	"1",

	"lpop t1",
	`"1"`,

	"lpop t1",
	`null`,

	"rpop t1",
	`null`,

	`lrange list -100 -1`,
	`["1", "1", "2", "3", "4", "5", "6", "7", "1"]`,

	"lindex list -1 ",
	`"1"`,

	"lindex list a ",
	`"ERR Unknown parameter"`,

	"lindex alist 1",
	`null`,

	"lindex key0 1",
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	"linsert list before 3 three",
	"10",

	"linsert list after 3 three",
	"11",

	`lrange list -100 -1`,
	`["1", "1", "2", "three", "3", "three", "4", "5", "6", "7", "1"]`,

	"linsert list here 3 three",
	`"ERR Unknown parameter"`,

	"linsert listnor after 3 three",
	"0",

	"linsert list before ",
	`"ERR Unknown parameter"`,

	"linsert list before 3",
	`"ERR Unknown parameter"`,

	"linsert key0 before 3 6",
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	"linsert list after 43 three",
	"-1",

	"ltrim key0 1 1",
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	"ltrim list before",
	`"ERR Unknown parameter"`,

	"ltrim list 1 before",
	`"ERR Unknown parameter"`,

	"ltrim list 1 1",
	`"OK"`,

	`lrange list -100 -1`,
	`["1", "2", "three", "3", "three", "4", "5", "6", "7", "1"]`,

	"ltrim list -2 -1",
	`"OK"`,

	`lrange list -100 -1`,
	`["1", "2", "three", "3", "three", "4", "5", "6"]`,

	"ltrim list -100 -100",
	`"OK"`,

	`lrange list -100 -1`,
	`["2", "three", "3", "three", "4", "5", "6"]`,

	"ltrim list 100 -100",
	`"OK"`,

	`lrange list -100 -1`,
	`[]`,

	"rpush list 1 2 3 4 5 6 7",
	"7",

	`lrange list -100 -1`,
	`["1", "2", "3", "4", "5", "6", "7"]`,

	`lset a`,
	`"ERR Unknown parameter"`,

	`lset a 2`,
	`"ERR Unknown parameter"`,

	`lset list 2 a`,
	`"OK"`,

	`lrange list -100 -1`,
	`["1", "2", "a", "4", "5", "6", "7"]`,

	`lset nolist 2 a`,
	`"ERR No such key"`,

	`lset key0 2 a`,
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	`lset list -12 a`,
	`"ERR Out of range"`,

	`select 2`,
	`"OK"`,

	"rpush list 1 2 3 4 5 6 7",
	"7",

	"hset hash field1 Hello",
	"1",

	"hset hash ",
	`"ERR Unknown parameter"`,

	"hset hash field1 ",
	`"ERR Unknown parameter"`,

	"hset list field1 Hello",
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	"hdel hash ",
	`"ERR Unknown parameter"`,

	"hdel list field1 Hello",
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	"hdel hash field2 Hello",
	"0",

	"hdel hash field1 Hello",
	"1",

	"hdel hashwrong field1 Hello",
	"0",

	"hset hash field1 Hello",
	"1",

	"hset hash field2 World",
	"1",

	"hdel hash field2 field1",
	"2",

	"hkeys hash",
	"[]",

	"hkeys hashwrong",
	"[]",

	"hkeys list",
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	"hset hash field1 Hello",
	"1",

	"hset hash field2 World",
	"1",

	"hdel hash field2 ",
	"1",

	"hvals hash",
	`["Hello"]`,

	"hvals hashwrong",
	"[]",

	"hvals list",
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	"hexists hash",
	`"ERR Unknown parameter"`,

	"hexists hashs 100",
	`0`,

	"hexists list 100",
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	"hexists hash 100",
	`0`,

	"hexists hash field1",
	`1`,

	"hget hash",
	`"ERR Unknown parameter"`,

	"hget hashs 100",
	`null`,

	"hget list 100",
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	"hget hash field1",
	`"Hello"`,

	"hlen hash",
	`1`,

	"hlen hashs",
	`0`,

	"hlen list",
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	"hstrlen hash",
	`"ERR Unknown parameter"`,

	"hstrlen hashs 100",
	`0`,

	"hstrlen list 100",
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	"hstrlen hash field1",
	`5`,

	"hstrlen hash field10",
	`0`,

	"hgetall hash",
	`["field1", "Hello"]`,

	"hgetall hashwrong",
	"[]",

	"hgetall list",
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	"hsetnx hash100 field1 Hello",
	"1",

	"hsetnx hash100 field1 World",
	"0",

	"hsetnx list field2 World",
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	"hsetnx hash",
	`"ERR Unknown parameter"`,

	"hsetnx hash 100",
	`"ERR Unknown parameter"`,

	`hincrby hash test 5`,
	`5`,

	`hincrby hash1 test 5`,
	`5`,

	`hincrby hash1 test 5`,
	`10`,

	`hincrby hash field1 5`,
	`"ERR value is not an integer or out of range"`,

	`hincrby list test 5`,
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	`hincrby hash `,
	`"ERR Unknown parameter"`,

	`hincrby hash test ss`,
	`"ERR Unknown parameter"`,

	`hmget hash test testempty`,
	`["5", null]`,

	`hmget list test testempty`,
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	`hmget hash12 test testempty`,
	`[]`,

	`hmset newhash`,
	`"ERR Invalid syntax"`,

	`hmset newhash eles`,
	`"ERR Invalid syntax"`,

	`hmset list eles ss`,
	`"WRONGTYPE Operation against a key holding the wrong kind of value"`,

	`hmset new eles ss`,
	`"OK"`,

	"quit",
	`"OK"`,
}

var address = "188.226.131.142"

func TestClient(t *testing.T) {
	options := DefaultOptions
	options.Address = address
	options.Password = "trytestmeagain"
	options.Port = 18018
	client, err := CreateClient(&options)
	if err != nil {
		t.Fatalf("Error of the creating client: %s", err.Error())
	}
	answ, err := client.SendCommand("ping")
	if err != nil {
		t.Fatalf("Error of the reading client: %s", err.Error())
	}
	str := datamodel.DataObjectToString(answ)
	if str != `"PONG"` {
		t.Fatalf("Error of the receving information from client: %s", str)
	}

	client.Pipelining(true)
	for i := 0; i < 1000; i++ {
		client.SendCommand("ping")
	}
	res, err := client.Flush()
	if err != nil {
		t.Fatalf("Error of the receiving Pipelining answer: %s", err.Error())
	}
	for i := 0; i < 1000; i++ {
		str := datamodel.DataObjectToString(res[i])
		if str != `"PONG"` {
			t.Fatalf("Error of the receving information from client: %s", str)
		}
	}

	client.Close()
}

var acc = 0

func BenchmarkClient(b *testing.B) {
	fmt.Println("Started with ", b.N)
	options := DefaultOptions
	options.Address = address
	options.Password = "trytestmeagain"
	options.Port = 18018
	t := time.Now()
	client, err := CreateClient(&options)
	if err != nil {
		b.Fatalf("Error of the creating client: %s", err.Error())
	}
	client.SendCommand("select", strconv.Itoa(acc+1))
	client.Pipelining(true)
	for i := 0; i < b.N; i++ {
		client.SendCommand("set", strconv.Itoa(i), "test", "px", "15000")
	}
	answers, err := client.Flush()
	if err != nil {
		b.Fatalf("Error of the receive information from client: %s", err.Error())
	}

	for _, answ := range answers {
		switch value := answ.(type) {
		case datamodel.DataString:
			if value.IsError() {
				b.Fatalf("Error of the reading client: %s", value.Get())
			}
		}
	}
	client.Close()
	acc++
	fmt.Println("Total time ", b.N, "  ", time.Since(t))
}

func TestClient1000(b *testing.T) {
	options := DefaultOptions
	options.Address = address
	options.Password = "trytestmeagain"
	options.Port = 18018
	t := time.Now()
	client, err := CreateClient(&options)
	if err != nil {
		b.Fatalf("Error of the creating client: %s", err.Error())
	}
	client.SendCommand("select", strconv.Itoa(acc+1))
	client.Pipelining(true)
	for i := 0; i < 200000; i++ {
		client.SendCommand("set", strconv.Itoa(i), "test", "px", "15000")
	}
	fmt.Println("Preparing time  ", time.Since(t), "input request size ", client.commands.Len())
	answers, err := client.Flush()
	if err != nil {
		b.Fatalf("Error of the receive information from client: %s", err.Error())
	}

	for i, answ := range answers {
		switch value := answ.(type) {
		case datamodel.DataString:
			if value.IsError() {
				b.Fatalf("Error of the reading client step %i: %s", i, value.Get())
			}
		}
	}
	client.Close()
	acc++
	fmt.Println("Total time  ", time.Since(t), "Total aswers: ", len(answers))
}
