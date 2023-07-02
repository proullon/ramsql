package protocol

import (
	"errors"
	"fmt"
	"io"
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
	Value []any
}

// ChannelDriverConn implements DriverConn for channel backend
type ChannelDriverConn struct {
	conn chan message
}

// ChannelDriverEndpoint implements DriverEndpoint for channel backend
type ChannelDriverEndpoint struct {
	newConnChannel chan<- chan message
}

// Close method closes the connection to RamSQL server
func (cdc *ChannelDriverConn) Close() {
	if cdc.conn == nil {
		return
	}
	close(cdc.conn)
	cdc.conn = nil
}

// New method creates a new DriverConn from DriverEndpoint
func (cde *ChannelDriverEndpoint) New(uri string) (DriverConn, error) {

	if cde.newConnChannel == nil {
		return nil, fmt.Errorf("connection closed")
	}

	channel := make(chan message)
	cdc := &ChannelDriverConn{conn: channel}
	cde.newConnChannel <- channel
	return cdc, nil
}

// NewChannelDriverEndpoint initialize a DriverEndpoint with channel backend
func NewChannelDriverEndpoint(channel chan<- chan message) DriverEndpoint {
	cde := &ChannelDriverEndpoint{
		newConnChannel: channel,
	}

	return cde
}

// ChannelEngineEndpoint implements EngineEndpoint for channel backend
type ChannelEngineEndpoint struct {
	newConnChannel <-chan chan message
}

// NewChannelEngineEndpoint initialize a EngineEndpoint with channel backend
func NewChannelEngineEndpoint(channel <-chan chan message) EngineEndpoint {
	cee := &ChannelEngineEndpoint{
		newConnChannel: channel,
	}

	return cee
}

// Accept read from new channels channel and return an EngineConn
func (cee *ChannelEngineEndpoint) Accept() (EngineConn, error) {
	newConn, ok := <-cee.newConnChannel
	if !ok {
		return nil, io.EOF
	}

	return NewChannelEngineConn(newConn), nil
}

// Close close the connection with client
func (cee *ChannelEngineEndpoint) Close() {
}

// ChannelEngineConn implements EngineConn for channel backend
type ChannelEngineConn struct {
	conn chan message
}

// NewChannelEngineConn initializes a new EngineConn with channel backend
func NewChannelEngineConn(newConn chan message) EngineConn {
	cec := &ChannelEngineConn{
		conn: newConn,
	}

	return cec
}

// ReadStatement get SQL statements from client
func (cec *ChannelEngineConn) ReadStatement() (string, error) {
	message, ok := <-cec.conn
	if !ok {
		cec.conn = nil
		return "", io.EOF
	}

	if len(message.Value) == 0 {
		return "", fmt.Errorf("incorrect statement")
	}
	stmt, ok := message.Value[0].(string)
	if !ok {
		return "", fmt.Errorf("incorrect statement type")
	}

	return stmt, nil
}

// WriteResult is used to answer to statements other than SELECT
func (cec *ChannelEngineConn) WriteResult(lastInsertedID int64, rowsAffected int64) error {
	m := message{
		Type:  resultMessage,
		Value: []any{lastInsertedID, rowsAffected},
	}

	cec.conn <- m
	return nil
}

// WriteError when error occurs
func (cec *ChannelEngineConn) WriteError(err error) error {
	m := message{
		Type:  errMessage,
		Value: []any{err.Error()},
	}

	cec.conn <- m
	return nil

}

// WriteRowHeader indicates that rows are coming next
func (cec *ChannelEngineConn) WriteRowHeader(header []string) error {
	var v []any

	for _, h := range header {
		v = append(v, h)
	}

	m := message{
		Type:  rowHeaderMessage,
		Value: v,
	}

	cec.conn <- m
	return nil

}

// WriteRow must be called after WriteRowHeader and before WriteRowEnd
func (cec *ChannelEngineConn) WriteRow(row []any) error {
	m := message{
		Type:  rowValueMessage,
		Value: row,
	}

	cec.conn <- m
	return nil
}

// WriteRowEnd indicates that query is done
func (cec *ChannelEngineConn) WriteRowEnd() error {
	m := message{
		Type: rowEndMessage,
	}

	cec.conn <- m
	return nil
}

// WriteQuery allows client to query the RamSQL server
func (cdc *ChannelDriverConn) WriteQuery(query string) error {
	if cdc.conn == nil {
		return fmt.Errorf("connection closed")
	}

	m := message{
		Type:  queryMessage,
		Value: []any{query},
	}

	cdc.conn <- m
	return nil
}

// WriteExec allows client to manipulate the RamSQL server
func (cdc *ChannelDriverConn) WriteExec(statement string) error {
	if cdc.conn == nil {
		return fmt.Errorf("connection closed")
	}

	m := message{
		Type:  execMessage,
		Value: []any{statement},
	}

	cdc.conn <- m
	return nil
}

// ReadResult when Exec has been used
func (cdc *ChannelDriverConn) ReadResult() (lastInsertedID int64, rowsAffected int64, err error) {
	if cdc.conn == nil {
		return 0, 0, fmt.Errorf("connection closed")
	}

	m := <-cdc.conn
	if m.Type != resultMessage {
		if m.Type == errMessage {
			return 0, 0, getErrorFromValue(m)
		}
		return 0, 0, fmt.Errorf("Protocol error: ReadResult received %v", m)
	}

	if val, ok := m.Value[0].(int64); ok {
		lastInsertedID = val
	}
	if val, ok := m.Value[1].(int64); ok {
		rowsAffected = val
	}
	return lastInsertedID, rowsAffected, nil
}

// ReadRows when Query has been used
func (cdc *ChannelDriverConn) ReadRows() (chan []any, error) {
	if cdc.conn == nil {
		return nil, fmt.Errorf("connection closed")
	}

	m := <-cdc.conn
	if m.Type == errMessage {
		return nil, getErrorFromValue(m)
	}

	if m.Type != rowHeaderMessage {
		return nil, errors.New("not a rows header")
	}

	return UnlimitedRowsChannel(cdc.conn, m), nil
}

func getErrorFromValue(m message) error {
	if len(m.Value) == 0 {
		return errors.New("unknown error")
	}

	switch m.Value[0].(type) {
	case string:
		msg, _ := m.Value[0].(string)
		return errors.New(msg)
	case []byte:
		msg, _ := m.Value[0].([]byte)
		return errors.New(string(msg))
	case error:
		e, _ := m.Value[0].(error)
		return e
	default:
		return errors.New("unknown error")
	}
}
