package protocol

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

type token string

const (
	Error  token = "E"
	Query  token = "Q"
	Result token = "R"
)

var (
	tokenMap = map[byte]token{
		'E': Error,
		'Q': Query,
		'R': Result,
	}
)

type Message struct {
	Token token
	Value string
}

func SendResult(conn io.Writer, t token, lastInsertedId int, rowsAffected int) error {
	return Send(conn, t, fmt.Sprintf("%d %d", lastInsertedId, rowsAffected))
}

func Send(conn io.Writer, t token, m string) error {
	log.Printf("protocol.Send: Sending %v <%s>", t, m)

	n, err := fmt.Fprintf(conn, "%s%s\n", string(t), m)

	if err != nil {
		return err
	}

	if n != len(m)+2 {
		log.Printf("Send: Cannot send entire message: %d bytes over %d", n, len(m)+2)
		return fmt.Errorf("Cannot send entire message")
	}

	return nil
}

func Read(conn net.Conn) (*Message, error) {

	// Set query deadline
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	buffer, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		// Check if error is Timeout
		if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
			log.Printf("Stmt.Exec: socket timeout: %s", err)
			return nil, fmt.Errorf("Request timeout")
		}

		return nil, err
	}

	// Remove '\n'
	buffer = buffer[:len(buffer)-1]

	m := &Message{}

	m.Token = tokenMap[buffer[0]]
	m.Value = string(buffer[1:])

	log.Printf("protocol.Read: Read %v <%s>", m.Token, m.Value)
	return m, nil
}
