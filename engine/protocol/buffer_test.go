package protocol

import (
	"fmt"
	"testing"

	"github.com/proullon/ramsql/engine/log"
)

func TestBufferChannel(t *testing.T) {
	log.UseTestLogger(t)
	NumberRows := 10

	engineChannel := make(chan message)
	m := message{
		Type:  rowHeaderMessage,
		Value: []string{"foo", "bar"},
	}

	driverChannel := UnlimitedRowsChannel(engineChannel, m)

	// We should be able to push 100 rows
	for i := 0; i < NumberRows; i++ {
		row := message{
			Type:  rowValueMessage,
			Value: []string{"row", fmt.Sprintf("%d", i)},
		}
		engineChannel <- row
	}
	// send rowEnd
	m = message{
		Type: rowEndMessage,
	}
	engineChannel <- m

	// We should be able to read NumberRows+1 rows
	var count int
	for {
		m, ok := <-driverChannel
		if !ok {
			if count != NumberRows+1 {
				t.Fatalf("Expected %d messages, got %d\n", NumberRows+1, count)
			}
			break
		}
		_ = m
		count++
	}
}
