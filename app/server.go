package main

import (
	"errors"
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
}

func (p *RequestParser) number() (int, error) {
	num := 0
	for {
		if int(p.pointer) >= len(p.input) {
			return 0, errors.New("pointer overflow")
		}
		if '0' > p.input[p.pointer] || p.input[p.pointer] > '9' {
			break
		}
		num *= 10
		num += int(p.input[p.pointer]) - '0'
		p.pointer++
	}
	// expects '\r' '\n'
	if p.input[p.pointer] != '\r' {
		return 0, errors.New("expect '\\r'")
	}
	p.pointer++
	if p.input[p.pointer] != '\n' {
		return 0, errors.New("expect '\\n'")
	}
	p.pointer++
	return num, nil
}

func (p *RequestParser) Parse() (Command, error) {
	log.Printf("rawMessage = %q\n", p.input)
	if p.input[p.pointer] != '*' {
		return nil, errors.New("invalid argument: expected: '*'")
	}
	p.pointer++

	numArg, err := p.number()
	if err != nil {
		return nil, fmt.Errorf("invalid argument: %w", err)
	}

	parsed := make([]CommandEntity, 0, numArg)
	for i := 0; i < numArg; i++ {
		if p.input[p.pointer] != '$' {
			return nil, errors.New("invalid argument: expected: '$'")
		}
		p.pointer++

		num, err := p.number()
		if err != nil {
			return nil, fmt.Errorf("invalid argument: %w", err)
		}

		// get num bytes
		if len(p.input) < int(p.pointer)+num {
			return nil, fmt.Errorf("invalid argument: number is invalid: %d", num)
		}
		parsed = append(parsed, CommandEntity(p.input[p.pointer:p.pointer+uint(num)]))
		p.pointer += uint(num)
		if p.input[p.pointer] != '\r' {
			return nil, errors.New("expect '\\r'")
		}
		p.pointer++
		if p.input[p.pointer] != '\n' {
			return nil, errors.New("expect '\\n'")
		}
		p.pointer++
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
