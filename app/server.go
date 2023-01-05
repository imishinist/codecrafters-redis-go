package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
)

const (
	CmdPing    = "PING"
	CmdEcho    = "ECHO"
	CmdCommand = "COMMAND"
	CmdSet     = "SET"
	CmdGet     = "GET"
)

type CommandEntity string
type Command []CommandEntity

func NewRequestParser(input []byte) *RequestParser {
	return &RequestParser{
		input:   input,
		pointer: 0,
	}
}

type RequestParser struct {
	input   []byte
	pointer uint

	err error
}

func (p *RequestParser) current() byte {
	p.pointerRangeCheck()
	if p.err != nil {
		return 0
	}
	return p.input[p.pointer]
}

func (p *RequestParser) pointerRangeCheck() {
	if p.err != nil {
		return
	}
	if int(p.pointer) >= len(p.input) {
		p.err = fmt.Errorf("pointer overflow")
	}
}

// expect consume pointer
func (p *RequestParser) expect(b byte) {
	if p.err != nil {
		return
	}

	if p.current() != b {
		p.err = fmt.Errorf("expected: '%c'", b)
	}
	p.pointer++
}

func (p *RequestParser) expects(bs []byte) {
	if p.err != nil {
		return
	}

	for _, b := range bs {
		p.expect(b)
	}
}

func (p *RequestParser) number() int {
	if p.err != nil {
		return 0
	}
	num := 0
	for {
		current := p.current()
		if '0' > current || current > '9' {
			break
		}
		num *= 10
		num += int(current) - '0'
		p.pointer++
	}
	p.expect('\r')
	p.expect('\n')
	return num
}

// slice consume pointer
func (p *RequestParser) slice(n uint) []byte {
	if len(p.input) < int(p.pointer+n) {
		p.err = fmt.Errorf("index out of range: %d", n)
		return nil
	}
	ret := p.input[p.pointer : p.pointer+n]
	p.pointer += n
	return ret
}

func (p *RequestParser) Parse() (Command, error) {
	log.Printf("rawMessage = %q\n", p.input)

	p.expect('*')
	numArg := p.number()
	if p.err != nil {
		return nil, fmt.Errorf("invalid argument: %w", p.err)
	}
	parsed := make([]CommandEntity, 0, numArg)
	for i := 0; i < numArg; i++ {
		p.expect('$')
		num := p.number()
		slice := p.slice(uint(num))
		if p.err != nil {
			return nil, fmt.Errorf("invalid argument: %w", p.err)
		}
		parsed = append(parsed, CommandEntity(slice))
		p.expects([]byte("\r\n"))
	}
	log.Printf("%q: %d\n", parsed, len(parsed))

	return parsed, nil
}

var (
	storage = make(map[string]string)
)

func handlePing(conn net.Conn, command Command) error {
	writeMessage(conn, "PONG")
	return nil
}

func handleEcho(conn net.Conn, command Command) error {
	if len(command) != 2 {
		writeError(conn, "wrong number of arguments for 'ping' command")
		return nil
	}

	writeMessage(conn, string(command[1]))
	return nil
}

func handleSet(conn net.Conn, command Command) error {
	if len(command) != 3 {
		writeError(conn, "wrong number of arguments for 'set' command")
		return nil
	}
	storage[string(command[1])] = string(command[2])
	writeMessage(conn, "OK")
	return nil
}

func handleGet(conn net.Conn, command Command) error {
	if len(command) != 2 {
		writeError(conn, "wrong number of arguments for 'get' command")
		return nil
	}
	value, ok := storage[string(command[1])]
	if !ok {
		conn.Write([]byte("$-1\r\n"))
		return nil
	}
	// TODO: レスポンス用の型を定義する
	writeMessage(conn, value)
	return nil
}

func writeMessage(conn net.Conn, message string) {
	if _, err := conn.Write(fmt.Appendf(nil, "+%s\r\n", message)); err != nil {
		log.Println("write message error")
		return
	}
}

func writeError(conn net.Conn, response string) {
	log.Println(response)
	if _, err := conn.Write(fmt.Appendf(nil, "-ERR %s\r\n", response)); err != nil {
		log.Println("write error response error:", err)
		return
	}
}

func handleMessage(conn net.Conn, input []byte) error {
	parser := NewRequestParser(input)
	command, err := parser.Parse()
	if err != nil {
		writeError(conn, fmt.Sprintf("parse input error: %s", err.Error()))
		return nil
	}

	if len(command) == 0 {
		writeError(conn, "empty command")
		return nil
	}

	switch strings.ToUpper(string(command[0])) {
	case CmdPing:
		if err := handlePing(conn, command); err != nil {
			log.Println(err)
			return nil
		}
	case CmdEcho:
		if err := handleEcho(conn, command); err != nil {
			log.Println(err)
			return nil
		}
	case CmdSet:
		if err := handleSet(conn, command); err != nil {
			log.Println(err)
			return nil
		}
	case CmdGet:
		if err := handleGet(conn, command); err != nil {
			log.Println(err)
			return nil
		}
	case CmdCommand:
		writeMessage(conn, "OK")
	default:
		writeError(conn, "unknown command")
	}
	return nil
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	// main-loop
	for {
		buf := make([]byte, 64)
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				log.Println("disconnected", conn.RemoteAddr())
				return
			}
			log.Println("Error read response:", err.Error())
			return
		}

		if err := handleMessage(conn, buf[:n]); err != nil {
			log.Println("handle message error:", err)
			log.Println("closing connection")
			return
		}
	}
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		log.Println("accepted")
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}
