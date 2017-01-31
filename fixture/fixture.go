package fixture

import "database/sql"

var drivers = make(map[string]interface{})

type Driver interface {
}

// sample:
//   import _ "github.com/takashabe/go-fixture/mysql"
//   ...
//     fixture.Load("path/to/hoge.yml", &Hoge{})
type Fixture struct {
	db *sql.DB
}

type FixtureFile struct {
	path string
}

func NewFixture(db *sql.DB) *Fixture {
	return &Fixture{db: db}
}

func (f *Fixture) SetUp(path string, model interface{}) error {
}

func (f *Fixture) Teardown() error {
}
