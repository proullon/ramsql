package protocol

type DriverConn interface {
	WriteQuery(query string) error
	WriteExec(stmt string) error
	ReadResult() (lastInsertedId int64, rowsAffected int64, err error)
	ReadRows() (chan []string, error)
	Close()
}

type EngineConn interface {
	ReadStatement() (string, error)
	WriteResult(lastInsertedId int, rowsAffected int) error
	WriteError(err error) error
	WriteRowHeader(header []string) error
	WriteRow(row []string) error
	WriteRowEnd() error
}

type EngineEndpoint interface {
	Accept() (EngineConn, error)
	Close()
}

type DriverEndpoint interface {
	New(string) (DriverConn, error)
}

func NewChannelEndpoints() (DriverEndpoint, EngineEndpoint) {
	channel := make(chan chan message)

	return NewChannelDriverEndpoint(channel), NewChannelEngineEndpoint(channel)
}
