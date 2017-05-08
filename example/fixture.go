package main

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/takashabe/go-fixture"
	_ "github.com/takashabe/go-fixture/mysql"
)

func main() {
	db, err := sql.Open("mysql", "fixture@/db_fixture")
	if err != nil {
		panic(err.Error())
	}

	f := fixture.NewFixture(db, "mysql")
	if err := f.Load("fixture/setup.yaml"); err != nil {
		panic(err.Error())
	}
}
