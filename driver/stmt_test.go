package ramsql

import (
	"testing"
)

func TestNumInputQuestionMarker(t *testing.T) {

	// Create a new stub Conn
	c := &Conn{}

	stmt := prepareStatement(c, "SELECT * FROM account WHERE email = '?'")
	if stmt == nil {
		t.Fatal("prepareStatement should not return nil")
	}

	if stmt.numInput != 1 {
		t.Fatalf("prepareStatement expected 1 input, got %d", stmt.numInput)
	}
}

func TestNumInputPostgreMarker(t *testing.T) {

	// Create a new stub Conn
	c := &Conn{}

	stmt := prepareStatement(c, "SELECT * FROM account WHERE email = '$1' AND foo = $2 LIMIT $2")
	if stmt == nil {
		t.Fatal("prepareStatement should not return nil")
	}

	if stmt.numInput != 2 {
		t.Fatalf("prepareStatement expected 2 input, got %d", stmt.numInput)
	}
}
