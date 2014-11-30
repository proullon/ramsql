package protocol

import (
	"errors"
	"fmt"
)

const (
	errMessage       = "ERR"
	queryMessage     = "QUERY"
	execMessage      = "EXEC"
	resultMessage    = "RES"
	rowHeaderMessage = "ROWHEAD"
	rowValueMessage  = "ROWVAL"
	rowEndMessage    = "ROWEND"
)

type message struct {
	Type  string
	Value []string
}

type ChannelDriverConn struct {
	conn chan message
}

type ChannelDriverEndpoint struct {
	newConnChannel chan<- chan message
}

func NewChannelEndpoints() (DriverEndpoint, EngineEndpoint) {
	channel := make(chan chan message)

	return NewChannelDriverEndpoint(channel), NewChannelEngineEndpoint(channel)
}

func (cde *ChannelDriverEndpoint) New(uri string) (DriverConn, error) {

	channel := make(chan message)
	cdc := &ChannelDriverConn{conn: channel}

	cde.newConnChannel <- channel
	return cdc, nil
}

func NewChannelDriverEndpoint(channel chan<- chan message) DriverEndpoint {
	cde := &ChannelDriverEndpoint{
		newConnChannel: channel,
	}

	return cde
}

type ChannelEngineEndpoint struct {
	newConnChannel <-chan chan message
}

func NewChannelEngineEndpoint(channel <-chan chan message) EngineEndpoint {
	cee := &ChannelEngineEndpoint{
		newConnChannel: channel,
	}

	return cee
}

func (cee *ChannelEngineEndpoint) Accept() (EngineConn, error) {
	newConn, ok := <-cee.newConnChannel
	if !ok {
		return nil, errors.New("connection closed")
	}

	return NewChannelEngineConn(newConn), nil
}

type ChannelEngineConn struct {
	conn chan message
}

func NewChannelEngineConn(newConn chan message) EngineConn {
	cec := &ChannelEngineConn{
		conn: newConn,
	}

	return cec
}

func (cec *ChannelEngineConn) ReadStatement() (string, error) {
	message, ok := <-cec.conn
	if !ok {
		return "", errors.New("connection closed")
	}

	return message.Value[0], nil
}

func (cec *ChannelEngineConn) WriteResult(lastInsertedId int, rowsAffected int) error {
	m := message{
		Type:  resultMessage,
		Value: []string{fmt.Sprintf("%d %d", lastInsertedId, rowsAffected)},
	}

	cec.conn <- m
	return nil
}

func (cec *ChannelEngineConn) WriteError(err string) error {
	m := message{
		Type:  errMessage,
		Value: []string{err},
	}

	cec.conn <- m
	return nil

}

func (cec *ChannelEngineConn) WriteRowHeader(header []string) error {
	m := message{
		Type:  rowHeaderMessage,
		Value: header,
	}

	cec.conn <- m
	return nil

}

func (cec *ChannelEngineConn) WriteRow(row []string) error {
	m := message{
		Type:  rowValueMessage,
		Value: row,
	}

	cec.conn <- m
	return nil
}

func (cec *ChannelEngineConn) WriteRowEnd() error {
	m := message{
		Type: rowEndMessage,
	}

	cec.conn <- m
	return nil
}

func (cdc *ChannelDriverConn) WriteQuery(query string) error {
	m := message{
		Type:  queryMessage,
		Value: []string{query},
	}
	cdc.conn <- m
	return nil
}

func (cdc *ChannelDriverConn) WriteExec(statement string) error {
	m := message{
		Type:  execMessage,
		Value: []string{statement},
	}
	cdc.conn <- m
	return nil
}

func (cdc *ChannelDriverConn) ReadResult() (lastInsertedId int, rowsAffected int, err error) {

	m := <-cdc.conn
	if m.Type != resultMessage {
		if m.Type == errMessage {
			return 0, 0, errors.New(m.Value[0])
		}
		return 0, 0, errors.New("not a result")
	}

	_, err = fmt.Sscanf(m.Value[0], "%d %d", &lastInsertedId, &rowsAffected)
	return lastInsertedId, rowsAffected, err
}

func (cdc *ChannelDriverConn) ReadRows() (chan []string, error) {
	channel := make(chan []string)

	m := <-cdc.conn
	if m.Type != rowHeaderMessage {
		return nil, errors.New("not a rows header")
	}

	go func() {
		channel <- m.Value

		for {
			m, ok := <-cdc.conn
			if !ok {
				close(channel)
				return
			}

			if m.Type == rowEndMessage {
				close(channel)
				return
			}

			channel <- m.Value
		}
	}()

	return channel, nil
}
