package main

import (
	"bufio"
	"testing"

	"bytes"

	"github.com/sybrexsys/RapidKV/database"
	"github.com/sybrexsys/RapidKV/datamodel"
)

var commands = []string{
	"*2\r\n$3\r\nget\r\n$-1\r\n",
	`"ERR Unknown parameter"`,

	"set 100 100",
	`"OK"`,

	"set 200 100 ex",
	`"ERR Syntax error"`,

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
	databases = make(map[int]*database.Database)
	cfg = &defConfig
	firstDatabase = database.CreateDatabase(defConfig.ShardCount)
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
