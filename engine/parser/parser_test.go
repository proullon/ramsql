package parser

import (
	"testing"

	"github.com/proullon/ramsql/engine/log"
)

func TestParserCreateTableSimple(t *testing.T) {
	query := `CREATE TABLE account (id INT, email TEXT)`
	parse(query, 1, t)
}

func TestParserCreateTableSimpleWithPrimaryKey(t *testing.T) {
	query := `CREATE TABLE account (id INT PRIMARY KEY, email TEXT)`
	parse(query, 1, t)
}

func TestParserMultipleInstructions(t *testing.T) {
	query := `CREATE TABLE account (id INT, email TEXT);CREATE TABLE user (id INT, email TEXT)`
	parse(query, 2, t)
}

// func TestParserLowerCase(t *testing.T) {
// 	query := `create table account (id INT PRIMARY KEY NOT NULL)`
// 	parse(query, 1, t)
// }

func TestParserComplete(t *testing.T) {
	query := `CREATE TABLE user
	(
    	id INT PRIMARY KEY,
	    last_name TEXT,
	    first_name TEXT,
	    email TEXT,
	    birth_date DATE,
	    country TEXT,
	    town TEXT,
	    zip_code TEXT
	)`
	parse(query, 1, t)
}

func TestParserCompleteWithBacktickQuotes(t *testing.T) {
	query := `CREATE TABLE `+"`"+`user`+"`"+`
	(
		`+"`"+`id`+"`"+` INT PRIMARY KEY,
		`+"`"+`last_name`+"`"+` TEXT,
		`+"`"+`first_name`+"`"+` TEXT,
		`+"`"+`email`+"`"+` TEXT,
		`+"`"+`birth_date`+"`"+` DATE,
		`+"`"+`country`+"`"+` TEXT,
		`+"`"+`town`+"`"+` TEXT,
		`+"`"+`zip_code`+"`"+` TEXT
	)`
	parse(query, 1, t)
}

// func TestParserCreateTableWithVarchar(t *testing.T) {
// 	query := `CREATE TABLE user
// 	(
//     	id INT PRIMARY KEY,
// 	    last_name VARCHAR(100)
// 	)`
// 	parse(query, 1, t)
// }

func TestSelectStar(t *testing.T) {
	query := `SELECT * FROM account WHERE email = 'foo@bar.com'`
	parse(query, 1, t)
}

func TestSelectMultipleAttribute(t *testing.T) {
	query := `SELECT id, email FROM account WHERE email = 'foo@bar.com'`
	parse(query, 1, t)
}

func TestSelectOneAttribute(t *testing.T) {
	query := `SELECT id FROM account WHERE email = 'foo@bar.com'`
	parse(query, 1, t)
}

func TestSelectAttributeWithTable(t *testing.T) {
	query := `SELECT account.id FROM account WHERE email = 'foo@bar.com'`
	parse(query, 1, t)
}

func TestSelectAttributeWithQuotedTable(t *testing.T) {
	query := `SELECT "account".id FROM account WHERE email = 'foo@bar.com'`
	parse(query, 1, t)
}

func TestSelectAttributeWithBacktickQuotedTable(t *testing.T) {
	query := `SELECT `+"`"+`account`+"`"+`.id FROM account WHERE email = 'foo@bar.com'`
	parse(query, 1, t)
}

func TestSelectAllFromTable(t *testing.T) {
	query := `SELECT "account".* FROM account WHERE email = 'foo@bar.com'`
	parse(query, 1, t)
}

func TestSelectOnePredicate(t *testing.T) {
	query := `SELECT * FROM account WHERE 1`
	parse(query, 1, t)
}

func TestSelectQuotedTableName(t *testing.T) {
	query := `SELECT * FROM "account" WHERE 1`
	parse(query, 1, t)

	query = `SELECT * FROM "account"`
	parse(query, 1, t)
}

func TestSelectBacktickQuotedTableName(t *testing.T) {
	query := `SELECT * FROM `+"`"+`account`+"`"+` WHERE 1`
	parse(query, 1, t)

	query = `SELECT * FROM `+"`"+`account`+"`"+``
	parse(query, 1, t)
}

func TestSelectJoin(t *testing.T) {
	query := `SELECT address.* FROM address
	JOIN user_addresses ON address.id=user_addresses.address_id
	WHERE user_addresses.user_id=1`
	parse(query, 1, t)
}

func TestInsertMinimal(t *testing.T) {
	query := `INSERT INTO account ('email', 'password', 'age') VALUES ('foo@bar.com', 'tititoto', '4')`
	parse(query, 1, t)
}

func TestInsertNumber(t *testing.T) {
	query := `INSERT INTO account ('email', 'password', 'age') VALUES ('foo@bar.com', 'tititoto', 4)`
	parse(query, 1, t)
}

func TestInsertNumberWithQuote(t *testing.T) {
	query := `INSERT INTO "account" ('email', 'password', 'age') VALUES ('foo@bar.com', 'tititoto', 4)`
	parse(query, 1, t)
}

func TestInsertNumberWithBacktickQuote(t *testing.T) {
	query := `INSERT INTO `+"`"+`account`+"`"+` ('email', 'password', 'age') VALUES ('foo@bar.com', 'tititoto', 4)`
	parse(query, 1, t)
}

func TestCreateTableWithKeywordName(t *testing.T) {
	query := `CREATE TABLE test ("id" bigserial not null primary key, "name" text, "key" text)`
	parse(query, 1, t)
}

// func TestInsertStringWithDoubleQuote(t *testing.T) {
// 	query := `insert into "posts" ("post_id","Created","Title","Body") values (null,12321123,"Hello world !","!");`
// 	parse(query, 1, t)
// }

func TestInsertStringWithSimpleQuote(t *testing.T) {
	query := `insert into "posts" ("post_id","Created","Title","Body") values (null,12321123,'Hello world !','!');`
	parse(query, 1, t)
}

// func TestInsertImplicitAttributes(t *testing.T) {
// 	query := `INSERT INTO account VALUES ('foo@bar.com', 'tititoto', 4)`
// 	parse(query, 1, t)
// }

func TestParseDelete(t *testing.T) {
	query := `delete from "posts"`
	parse(query, 1, t)
}

func TestParseUpdate(t *testing.T) {
	query := `UPDATE account SET email = 'roger@gmail.com' WHERE id = 2`
	parse(query, 1, t)
}

func TestUpdateMultipleAttributes(t *testing.T) {
	query := `update "posts" set "Created"=1435760856063203203, "Title"='Go 1.2 is better than ever', "Body"='Lorem ipsum lorem ipsum' where "post_id"=2`
	parse(query, 1, t)
}

func TestParseMultipleJoin(t *testing.T) {
	query := `SELECT group.id, user.username FROM group JOIN group_user ON group_user.group_id = group.id JOIN user ON user.id = group_user.user_id WHERE group.name = 1`
	parse(query, 1, t)
}

func TestParseMultipleOrderBy(t *testing.T) {
	query := `SELECT group.id, user.username FROM group JOIN group_user ON group_user.group_id = group.id JOIN user ON user.id = group_user.user_id WHERE group.name = 1 ORDER BY group.name, user.username ASC`
	parse(query, 1, t)
}

func TestSelectForUpdate(t *testing.T) {
	query := `SELECT * FROM user WHERE user.id = 1 FOR UPDATE`

	parse(query, 1, t)
}

func TestCreateDefault(t *testing.T) {
	query := `CREATE TABLE foo (bar BIGINT, riri TEXT, fifi BOOLEAN NOT NULL DEFAULT false)`

	parse(query, 1, t)
}

func TestCreateDefaultNumerical(t *testing.T) {
	query := `CREATE TABLE foo (bar BIGINT, riri TEXT, fifi BIGINT NOT NULL DEFAULT 0)`

	parse(query, 1, t)
}

func TestCreateWithTimestamp(t *testing.T) {
	query := `CREATE TABLE IF NOT EXISTS "pokemon" (id BIGSERIAL PRIMARY KEY, name TEXT, type TEXT, seen TIMESTAMP WITH TIME ZONE)`

	parse(query, 1, t)
}

func TestCreateDefaultTimestamp(t *testing.T) {
	query := `CREATE TABLE IF NOT EXISTS "pokemon" (id BIGSERIAL PRIMARY KEY, name TEXT, type TEXT, seen TIMESTAMP WITH TIME ZONE DEFAULT LOCALTIMESTAMP)`

	parse(query, 1, t)
}

func TestCreateNumberInNames(t *testing.T) {
	query := `CREATE TABLE IF NOT EXISTS "pokemon" (id BIGSERIAL PRIMARY KEY, name TEXT, type TEXT, md5sum TEXT)`

	parse(query, 1, t)
}

func TestOffset(t *testing.T) {
	query := `SELECT * FROM mytable LIMIT 1 OFFSET 0`

	parse(query, 1, t)
}

func TestUnique(t *testing.T) {
	queries := []string{
		`CREATE TABLE pokemon (id BIGSERIAL, name TEXT UNIQUE NOT NULL)`,
		`CREATE TABLE pokemon (id BIGSERIAL, name TEXT NOT NULL UNIQUE)`,
		`CREATE TABLE pokemon_name (id BIGINT, name VARCHAR(255) PRIMARY KEY NOT NULL UNIQUE)`,
	}

	for _, q := range queries {
		parse(q, 1, t)
	}
}

func parse(query string, instructionNumber int, t *testing.T) []Instruction {
	log.UseTestLogger(t)

	parser := parser{}
	lexer := lexer{}
	decls, err := lexer.lex([]byte(query))
	if err != nil {
		t.Fatalf("Cannot lex <%s> string: %s", query, err)
	}

	instructions, err := parser.parse(decls)
	if err != nil {
		t.Fatalf("Cannot parse tokens from '%s': %s", query, err)
	}

	if len(instructions) != instructionNumber {
		t.Fatalf("Should have parsed %d instructions, got %d", instructionNumber, len(instructions))
	}

	return instructions
}
