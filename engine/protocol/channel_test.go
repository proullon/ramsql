package protocol

import (
	"errors"
	"testing"
)

func TestQuery(t *testing.T) {

	driverE, engineE := NewChannelEndpoints()

	go func() {
		for {
			engineConn, err := engineE.Accept()
			if err != nil {
				t.Fatal(err)
			}
			st, err := engineConn.ReadStatement()
			if err != nil {
				t.Fatal(err)
			}

			_ = st
			err = engineConn.WriteRowHeader([]string{"foo", "bar"})
			if err != nil {
				t.Fatal(err)
			}

			err = engineConn.WriteRow([]string{"hello", "world"})
			if err != nil {
				t.Fatal(err)
			}

			err = engineConn.WriteRowEnd()
			if err != nil {
				t.Fatal(err)
			}
		}
	}()

	driverConn, err := driverE.New("")
	if err != nil {
		t.Fatal(err)
	}

	err = driverConn.WriteQuery("toto")
	if err != nil {
		t.Fatal(err)
	}

	channel, err := driverConn.ReadRows()
	if err != nil {
		t.Fatal(err)
	}

	header, ok := <-channel
	if !ok {
		t.Fatal("Cannot read rows")
	}

	if len(header) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(header))
	}

	if header[0] != "foo" {
		t.Fatalf("Expected first columns name to be <foo>, got <%s>", header[0])
	}

	if header[1] != "bar" {
		t.Fatalf("Expected second columns name to be <bar>, got <%s>", header[1])
	}

	rows, ok := <-channel
	if !ok {
		t.Fatal("Cannot read rows")
	}

	if len(rows) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(rows))
	}

	if rows[0] != "hello" {
		t.Fatalf("Expected first column value to be <hello>, got <%s>", rows[0])
	}

	if rows[1] != "world" {
		t.Fatalf("Expected first columns value to be <world>, got <%s>", rows[1])
	}

}

func TestExecAndResult(t *testing.T) {

	driverE, engineE := NewChannelEndpoints()

	go func() {
		for {
			engineConn, err := engineE.Accept()
			if err != nil {
				t.Fatal(err)
			}
			st, err := engineConn.ReadStatement()
			if err != nil {
				t.Fatal(err)
			}

			_ = st
			engineConn.WriteResult(3, 4)
		}
	}()

	driverConn, err := driverE.New("")
	if err != nil {
		t.Fatal(err)
	}

	err = driverConn.WriteExec("toto")
	if err != nil {
		t.Fatal(err)
	}

	lastInserted, rowsAffected, err := driverConn.ReadResult()
	if err != nil {
		t.Fatal(err)
	}

	if lastInserted != 3 {
		t.Fatalf("Expected lastInsertedValue at 3, got %d", lastInserted)
	}

	if rowsAffected != 4 {
		t.Fatalf("Expected rowsAffected at 4, got %d", rowsAffected)
	}
}

func TestError(t *testing.T) {
	errMessage := errors.New("oh shoot")
	driverE, engineE := NewChannelEndpoints()

	go func() {
		for {
			engineConn, err := engineE.Accept()
			if err != nil {
				t.Fatal(err)
			}
			st, err := engineConn.ReadStatement()
			if err != nil {
				t.Fatal(err)
			}
			_ = st
			engineConn.WriteError(errMessage)
		}
	}()

	driverConn, err := driverE.New("")
	if err != nil {
		t.Fatal(err)
	}

	err = driverConn.WriteExec("toto")
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = driverConn.ReadResult()
	if err.Error() != errMessage.Error() {
		t.Fatalf("Expected error <%s>, got <%s>", errMessage, err)
	}
}
