package protocol

type DriverConn interface {
	WriteQuery(string) error
	WriteExec(string) error
	ReadResult() (lastInsertedId int, rowsAffected int, err error)
	ReadRows() (chan []string, error)
}

type EngineConn interface {
	ReadStatement() (string, error)
	WriteResult(lastInsertedId int, rowsAffected int) error
	WriteError(err string) error
	WriteRowHeader(header []string) error
	WriteRow(row []string) error
	WriteRowEnd() error
}

type EngineEndpoint interface {
	Accept() (EngineConn, error)
}

type DriverEndpoint interface {
	New(string) (DriverConn, error)
}
