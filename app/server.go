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

func handlePing(conn net.Conn, command Command) error {
	if _, err := conn.Write([]byte("+PONG\r\n")); err != nil {
		return err
	}
	return nil
}

func handleEcho(conn net.Conn, command Command) error {
	if len(command) != 2 {
		if _, err := conn.Write([]byte("-ERR wrong number of arguments for 'ping' command\r\n")); err != nil {
			return err
		}
		return nil
	}

	ret := fmt.Appendf(nil, "+%s\r\n", command[1])
	if _, err := conn.Write(ret); err != nil {
		return err
	}
	return nil
}

func handleRequest(conn net.Conn) {
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

		parser := NewRequestParser(buf[:n])
		command, err := parser.Parse()
		if err != nil {
			fmt.Println("Error parse input:", err.Error())
			return
		}
		if len(command) == 0 {
			if _, err := conn.Write([]byte("-Err empty command\r\n")); err != nil {
				log.Println(err)
				return
			}
			continue
		}

		switch strings.ToUpper(string(command[0])) {
		case CmdPing:
			if err := handlePing(conn, command); err != nil {
				fmt.Println(err.Error())
				return
			}
		case CmdEcho:
			if err := handleEcho(conn, command); err != nil {
				fmt.Println(err.Error())
				return
			}
		case CmdCommand:
			if _, err := conn.Write([]byte("+OK\r\n")); err != nil {
				fmt.Println(err.Error())
				return
			}
		default:
			if _, err := conn.Write([]byte("-Err unknown command\r\n")); err != nil {
				fmt.Println(err.Error())
				return
			}
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
		go handleRequest(conn)
	}
}
