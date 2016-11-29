package main

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

var MatchList = []string{
	"[a-c]d",
	"bd",
	"*",
	"test",
	"?test",
	"btest",
	"*r",
	"tractor",
	"[ab]ulk",
	"bulk",
}

var NMatchList = []string{
	"*k",
	"test",
	"?test",
	"btesta",
	"a*r",
	"btractor",
	"[ab]ulk",
	"dulk",
}

func TestMatch(t *testing.T) {
	for i := 0; i < len(MatchList); i += 2 {
		ok, err := Match(MatchList[i], MatchList[i+1])
		if err != nil {
			t.Fatal(err.Error())
		}
		if !ok {
			t.Fatal("must pass. Pattern:%s String:%s", MatchList[i], MatchList[i+1])
		}
	}
	for i := 0; i < len(NMatchList); i += 2 {
		ok, err := Match(NMatchList[i], NMatchList[i+1])
		if err != nil {
			t.Fatal(err.Error())
		}
		if ok {
			t.Fatal("must not pass. Pattern:%s String:%s", NMatchList[i], NMatchList[i+1])
		}
	}
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
		if f, err := Match("1*", s[i]); f && err == nil {
			fnd++
		}
	}
	fmt.Println(b.N, "    ", time.Since(t), "   ", fnd)
}
