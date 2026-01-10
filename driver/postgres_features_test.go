package ramsql

import (
	"database/sql"
	"testing"
)

// TestLike tests the SQL LIKE operator with pattern matching
func TestLike(t *testing.T) {
	db, err := sql.Open("ramsql", "TestLike")
	if err != nil {
		t.Fatalf("sql.Open: %s", err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE users (id INT, name TEXT, email TEXT)`)
	if err != nil {
		t.Fatalf("cannot create table: %s", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (1, 'John Doe', 'john@example.com')`)
	if err != nil {
		t.Fatalf("cannot insert: %s", err)
	}
	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (2, 'Jane Smith', 'jane@test.org')`)
	if err != nil {
		t.Fatalf("cannot insert: %s", err)
	}
	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (3, 'Bob Johnson', 'bob@example.com')`)
	if err != nil {
		t.Fatalf("cannot insert: %s", err)
	}

	// Test LIKE with %
	rows, err := db.Query(`SELECT name FROM users WHERE email LIKE '%@example.com'`)
	if err != nil {
		t.Fatalf("LIKE query failed: %s", err)
	}

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan failed: %s", err)
		}
		names = append(names, name)
	}
	rows.Close()

	if len(names) != 2 {
		t.Errorf("expected 2 results for LIKE '%%@example.com', got %d: %v", len(names), names)
	}

	// Test LIKE with prefix
	rows, err = db.Query(`SELECT name FROM users WHERE name LIKE 'J%'`)
	if err != nil {
		t.Fatalf("LIKE query failed: %s", err)
	}

	names = nil
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan failed: %s", err)
		}
		names = append(names, name)
	}
	rows.Close()

	if len(names) != 2 {
		t.Errorf("expected 2 results for LIKE 'J%%', got %d: %v", len(names), names)
	}

	// Test LIKE with _ wildcard
	rows, err = db.Query(`SELECT name FROM users WHERE name LIKE 'J_ne Smith'`)
	if err != nil {
		t.Fatalf("LIKE query failed: %s", err)
	}

	names = nil
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan failed: %s", err)
		}
		names = append(names, name)
	}
	rows.Close()

	if len(names) != 1 || names[0] != "Jane Smith" {
		t.Errorf("expected 'Jane Smith' for LIKE 'J_ne Smith', got %v", names)
	}
}

// TestILike tests the PostgreSQL ILIKE operator (case-insensitive LIKE)
func TestILike(t *testing.T) {
	db, err := sql.Open("ramsql", "TestILike")
	if err != nil {
		t.Fatalf("sql.Open: %s", err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE products (id INT, name TEXT)`)
	if err != nil {
		t.Fatalf("cannot create table: %s", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO products (id, name) VALUES (1, 'Apple iPhone')`)
	if err != nil {
		t.Fatalf("cannot insert: %s", err)
	}
	_, err = db.Exec(`INSERT INTO products (id, name) VALUES (2, 'APPLE MacBook')`)
	if err != nil {
		t.Fatalf("cannot insert: %s", err)
	}
	_, err = db.Exec(`INSERT INTO products (id, name) VALUES (3, 'Samsung Galaxy')`)
	if err != nil {
		t.Fatalf("cannot insert: %s", err)
	}

	// Test ILIKE - case insensitive
	rows, err := db.Query(`SELECT name FROM products WHERE name ILIKE 'apple%'`)
	if err != nil {
		t.Fatalf("ILIKE query failed: %s", err)
	}

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan failed: %s", err)
		}
		names = append(names, name)
	}
	rows.Close()

	if len(names) != 2 {
		t.Errorf("expected 2 results for ILIKE 'apple%%', got %d: %v", len(names), names)
	}

	// Test ILIKE with mixed case pattern
	rows, err = db.Query(`SELECT name FROM products WHERE name ILIKE '%GALAXY'`)
	if err != nil {
		t.Fatalf("ILIKE query failed: %s", err)
	}

	names = nil
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan failed: %s", err)
		}
		names = append(names, name)
	}
	rows.Close()

	if len(names) != 1 || names[0] != "Samsung Galaxy" {
		t.Errorf("expected 'Samsung Galaxy' for ILIKE '%%GALAXY', got %v", names)
	}
}

// TestCountFilter tests the COUNT(*) FILTER (WHERE ...) syntax for filtered aggregates
func TestCountFilter(t *testing.T) {
	db, err := sql.Open("ramsql", "TestCountFilter")
	if err != nil {
		t.Fatalf("sql.Open: %s", err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE orders (id INT, status TEXT, amount INT)`)
	if err != nil {
		t.Fatalf("cannot create table: %s", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO orders (id, status, amount) VALUES (1, 'completed', 100)`)
	if err != nil {
		t.Fatalf("cannot insert: %s", err)
	}
	_, err = db.Exec(`INSERT INTO orders (id, status, amount) VALUES (2, 'pending', 200)`)
	if err != nil {
		t.Fatalf("cannot insert: %s", err)
	}
	_, err = db.Exec(`INSERT INTO orders (id, status, amount) VALUES (3, 'completed', 150)`)
	if err != nil {
		t.Fatalf("cannot insert: %s", err)
	}
	_, err = db.Exec(`INSERT INTO orders (id, status, amount) VALUES (4, 'cancelled', 50)`)
	if err != nil {
		t.Fatalf("cannot insert: %s", err)
	}
	_, err = db.Exec(`INSERT INTO orders (id, status, amount) VALUES (5, 'completed', 300)`)
	if err != nil {
		t.Fatalf("cannot insert: %s", err)
	}

	// Test COUNT with FILTER
	rows, err := db.Query(`SELECT COUNT(*) FILTER (WHERE status = 'completed') FROM orders`)
	if err != nil {
		t.Fatalf("COUNT FILTER query failed: %s", err)
	}

	var count int64
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			t.Fatalf("scan failed: %s", err)
		}
	}
	rows.Close()

	if count != 3 {
		t.Errorf("expected 3 completed orders, got %d", count)
	}

	// Test COUNT with FILTER for pending
	rows, err = db.Query(`SELECT COUNT(*) FILTER (WHERE status = 'pending') FROM orders`)
	if err != nil {
		t.Fatalf("COUNT FILTER query failed: %s", err)
	}

	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			t.Fatalf("scan failed: %s", err)
		}
	}
	rows.Close()

	if count != 1 {
		t.Errorf("expected 1 pending order, got %d", count)
	}
}

// TestTypeCast tests the PostgreSQL :: type casting syntax
func TestTypeCast(t *testing.T) {
	db, err := sql.Open("ramsql", "TestTypeCast")
	if err != nil {
		t.Fatalf("sql.Open: %s", err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE data (id INT, value INT)`)
	if err != nil {
		t.Fatalf("cannot create table: %s", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO data (id, value) VALUES (1, 42)`)
	if err != nil {
		t.Fatalf("cannot insert: %s", err)
	}
	_, err = db.Exec(`INSERT INTO data (id, value) VALUES (2, 100)`)
	if err != nil {
		t.Fatalf("cannot insert: %s", err)
	}

	// Test ::text cast in WHERE clause - cast value to text for comparison
	rows, err := db.Query(`SELECT id FROM data WHERE value::text = '42'`)
	if err != nil {
		t.Fatalf("type cast query failed: %s", err)
	}

	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan failed: %s", err)
		}
		ids = append(ids, id)
	}
	rows.Close()

	if len(ids) != 1 || ids[0] != 1 {
		t.Errorf("expected id=1 for value::text = '42', got %v", ids)
	}
}

// TestLikePatterns tests various LIKE pattern edge cases
func TestLikePatterns(t *testing.T) {
	db, err := sql.Open("ramsql", "TestLikePatterns")
	if err != nil {
		t.Fatalf("sql.Open: %s", err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE strings (id INT, val TEXT)`)
	if err != nil {
		t.Fatalf("cannot create table: %s", err)
	}

	testCases := []struct {
		value string
	}{
		{"hello"},
		{"world"},
		{"hello world"},
		{""},
		{"h"},
		{"hello123"},
	}

	for i, tc := range testCases {
		_, err = db.Exec(`INSERT INTO strings (id, val) VALUES (?, ?)`, i+1, tc.value)
		if err != nil {
			t.Fatalf("cannot insert: %s", err)
		}
	}

	patterns := []struct {
		pattern  string
		expected int
	}{
		{"hello", 1},
		{"%world", 2},              // "world", "hello world"
		{"hello%", 3},              // "hello", "hello world", "hello123"
		{"%lo%", 3},                // "hello", "hello world", "hello123"
		{"_____", 2},               // 5 underscores matches "hello" and "world"
		{"%", 6},                   // matches everything including empty string
		{"h_llo", 1},               // "hello"
		{"hello123", 1},            // exact match
		{"hello_world", 1},         // underscore matches space in "hello world"
		{"hello world", 1},         // exact match with space
	}

	for _, p := range patterns {
		rows, err := db.Query(`SELECT id FROM strings WHERE val LIKE ?`, p.pattern)
		if err != nil {
			t.Fatalf("LIKE query with pattern '%s' failed: %s", p.pattern, err)
		}

		count := 0
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				t.Errorf("pattern '%s': scan error: %s", p.pattern, err)
			}
			count++
		}
		rows.Close()

		if count != p.expected {
			t.Errorf("pattern '%s': expected %d matches, got %d", p.pattern, p.expected, count)
		}
	}
}
