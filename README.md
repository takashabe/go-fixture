# go-fixture

[![GoDoc](https://godoc.org/github.com/takashabe/go-fixture?status.svg)](https://godoc.org/github.com/takashabe/go-fixture)
[![CircleCI](https://circleci.com/gh/takashabe/go-fixture.svg?style=shield)](https://circleci.com/gh/takashabe/go-fixture)
[![Go Report Card](https://goreportcard.com/badge/github.com/takashabe/go-fixture)](https://goreportcard.com/report/github.com/takashabe/go-fixture)

Management the fixtures of the database for testing.

## features

* Load database fixture from .yaml or .sql file
* Before cleanup table(.yaml only)
* Support databases
  * MySQL
  * and more drivers are todo...

## usage

* Import `go-fixture` and drirver `go-fixture/mysql` as like `database/sql` driver

```go
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
  f.Load("fixture/setup.yml")
  f.Load("fixture/setup.sql")
}
```

## fixture file format

#### .yml .yaml

```yml
table: foo
record:
  - id: 1
    first_name: foo
    last_name: bar
  - id: 2
    first_name: piyo
    last_name: fuga
```

#### .sql

* Need to add a semicolon at the end of the line
* If table cleanup is required it will need to be in the fixture file

```sql
DELETE FROM person;
INSERT INTO person(id, name) VALUES (1, 'foo');
```
