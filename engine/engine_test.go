package engine

import (
	"testing"
)

func TestNewEngine(t *testing.T) {
	e, err := New()
	if err != nil {
		t.Fatalf("Cannot create new engine: %s", err)
	}

	e.Stop()
}
