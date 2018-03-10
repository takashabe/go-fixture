package main

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/takashabe/go-fixture"
	_ "github.com/takashabe/go-fixture/mysql"
)

func main() {
	// omit error handling
	db, _ := sql.Open("mysql", "fixture@/db_fixture")
	f, _ := fixture.NewFixture(db, "mysql")
	f.Load("fixture/setup.yaml")
}
