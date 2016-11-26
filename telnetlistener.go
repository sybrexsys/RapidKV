package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"runtime"
	"time"

	"github.com/sybrexsys/RapidKV/datamodel"
)

var telnetstop chan struct{}

type clientConnection struct {
	answers     []datamodel.CustomDataType
	answersSize int
}

func (cc *clientConnection) setCapacity(newCapacity int) {
	tmp := make([]datamodel.CustomDataType, newCapacity)
	copy(tmp, cc.answers)
	cc.answers = tmp
}

func (cc *clientConnection) grow() {
	var Delta int
	Cap := len(cc.answers)
	if Cap > 64 {
		Delta = Cap / 4
	} else {
		if Cap > 8 {
			Delta = 16
		} else {
			Delta = 4
		}
	}
	cc.setCapacity(Cap + Delta)
}

func (cc *clientConnection) pushAnswer(answer datamodel.CustomDataType) {
	if cc.answersSize == len(cc.answers) {
		cc.grow()
	}
	cc.answers[cc.answersSize] = answer
	cc.answersSize++
}
func (cc *clientConnection) popAnswers(writer *bufio.Writer) error {
	for i := 0; i < cc.answersSize; i++ {
		_, err := writer.Write(datamodel.ConvertToRASP(cc.answers[i]))
		if err != nil {
			return err
		}
		cc.answers[i] = nil
	}
	writer.Flush()
	if cc.answersSize > 1024 {
		cc.answers = make([]datamodel.CustomDataType, 100)
	}
	cc.answersSize = 0
	return nil
}

func readOneRecord(reader *bufio.Reader) (datamodel.CustomDataType, error) {

	return nil, nil

}

func processOneRESPCommand(command datamodel.CustomDataType) (datamodel.CustomDataType, error) {

	return nil, nil
}

func processLazyCommand(command datamodel.CustomDataType) (datamodel.CustomDataType, error) {
	return nil, nil
}

func checkEOF(reader *bufio.Reader) (bool, error) {
	_, err := reader.ReadByte()
	if err == nil {
		reader.UnreadByte()
		return false, nil
	}
	if err == io.EOF {
		return true, nil
	}
	return false, err
}

func saveOneDataToOut(answer datamodel.CustomDataType, writer *bufio.Writer) error {
	return nil
}

func processRESPConnection(c net.Conn) {
	var (
		answer datamodel.CustomDataType
		err    error
	)
	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			buf = buf[0:n]
			log.Fatalf("client run panic %s:%v", buf, e)
		}
		c.Close()
	}()
	reader := bufio.NewReader(c)
	writer := bufio.NewWriter(c)
	cc := &clientConnection{
		answers:     make([]datamodel.CustomDataType, 100),
		answersSize: 0,
	}
	for {
		_, err = reader.ReadByte()
		if err != nil {
			if err != io.EOF {
				return
			}
			if cc.answersSize != 0 {
				if cc.popAnswers(writer) != nil {
					return
				}
			} else {
				runtime.Gosched()
			}
			continue
		} else {
			reader.UnreadByte()
		}
		request, isLazy, erro := datamodel.LoadRespFromIO(reader, true)
		if err != nil {
			parseError, ok := erro.(datamodel.ParseError)
			if !ok {
				return
			}
			answer = datamodel.CreateError(parseError.Error())
			cc.pushAnswer(answer)
		}
		if isLazy {
			answer, err = processOneRESPCommand(request)

		} else {
			answer, err = processLazyCommand(request)
		}
		if err != nil {
			return
		}
		cc.pushAnswer(answer)
	}
}

func startTelnetListener(cfg *config) {
	fmt.Println("telnet started")
	defer func() {
		fmt.Println("stopped telnet server...")
		notifier.Done()
	}()
	notifier.Add(1)
	listener, err := net.Listen("tcp", "localhost:8000")
	if err != nil {
		log.Fatal("Telnet listener was not created")
		return
	}
	ls, ok := listener.(*net.TCPListener)
	if !ok {
		log.Fatal("invalid listener was created for telnet connection")
	}
mainloop:
	for {
		ls.SetDeadline(time.Now().Add(time.Millisecond * 200))
		conn, err := ls.Accept()
		select {
		case <-telnetstop:
			fmt.Println("stop signal was received")
			break mainloop
		default:
		}
		if err != nil {
			netErr, ok := err.(net.Error)
			//If this is a timeout, then continue to wait for
			//new connections
			if ok && netErr.Timeout() && netErr.Temporary() {
				continue
			} else {
				fmt.Println(err.Error())
				return
			}
		}
		go processRESPConnection(conn)
	}
}
