package ramsql

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

// InitSchemas execute each query in provided sql file
// Expected sql file path into $GOPATH/src
func InitSchemas(db *sql.DB, sqlfile string) error {
	gopath := os.Getenv("GOPATH")

	content, err := ioutil.ReadFile(path.Join(gopath, "src", sqlfile))
	if err != nil {
		return err
	}

	queries := strings.Split(string(content), ";")

	for _, q := range queries {
		q = strings.Trim(q, "\n")

		if q == "" {
			continue
		}

		_, err := db.Exec(q)
		if err != nil {
			return fmt.Errorf("Query '%s': %s", q, err)
		}

	}

	return nil
}
