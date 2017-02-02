package fixture

import (
	"database/sql"
	"io/ioutil"
	"strings"

	"github.com/pkg/errors"
)

var (
	ErrFailRegisterDriver = errors.New("failed to register driver")
	ErrFailReadFile       = errors.New("failed to read file")

	// db driver. register by any driver files
	drivers = make(map[string]Driver)
)

type Driver interface {
	TrimComment(sql string) string
}

// sample:
//   import _ "github.com/takashabe/go-fixture/mysql"
//   ...
//     fixture.Load("path/to/hoge.yml", &Hoge{})
type Fixture struct {
	db     *sql.DB
	driver Driver
}

func Register(name string, driver Driver) error {
	if _, dup := drivers[name]; driver == nil || dup {
		return ErrFailRegisterDriver
	}
	drivers[name] = driver
	return nil
}

func NewFixture(db *sql.DB, driverName string) *Fixture {
	return &Fixture{db: db, driver: drivers[driverName]}
}

// load .yml script
func (f *Fixture) Load(path string, model interface{}) error {
	return nil
}

// load .sql script
func (f *Fixture) LoadSQL(path string) error {
	data, err := getFileData(path)
	if err != nil {
		return err
	}
	stmts, err := f.createSQLs(string(data))
	if err != nil {
		return err
	}
	for _, stmt := range stmts {
		_, err = stmt.Exec()
		if err != nil {
			return err
		}
		defer stmt.Close()
	}

	return nil
}

func (f *Fixture) createSQLs(fileData string) ([]*sql.Stmt, error) {
	sqls := []*sql.Stmt{}
	sql := strings.Trim(fileData, "\n")
	sql = f.driver.TrimComment(sql)
	for _, line := range strings.Split(sql, ";") {
		if line == "" {
			continue
		}
		stmt, err := f.db.Prepare(line)
		if err != nil {
			return nil, err
		}
		sqls = append(sqls, stmt)
	}
	return sqls, nil
}

func getFileData(path string) ([]byte, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.Wrap(ErrFailReadFile, err.Error())
	}

	return data, nil
}
