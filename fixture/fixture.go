package fixture

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

var (
	ErrFailRegisterDriver = errors.New("failed to register driver")
	ErrFailReadFile       = errors.New("failed to read file")
	ErrInvalidFixture     = errors.New("invalid fixture file format")

	// db driver. register by any driver files
	drivers = make(map[string]Driver)
)

type Driver interface {
	TrimComment(sql string) string
	EscapeKeyword(keyword string) string
	EscapeValue(value string) string
}

// sample:
//   import _ "github.com/takashabe/go-fixture/mysql"
//   ...
//     fixture.Load("path/to/hoge.yml", &Hoge{})
type Fixture struct {
	db     *sql.DB
	driver Driver
}

type FixtureModel struct {
	Table  string              `yaml:"table"`
	Record []map[string]string `yaml:"record"`
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
func (f *Fixture) Load(path string) error {
	data, err := getFileData(path)
	if err != nil {
		return err
	}
	model := FixtureModel{}
	err = yaml.Unmarshal(data, &model)
	if err != nil {
		return errors.Wrapf(ErrInvalidFixture, ": err %v", err)
	}
	stmts, err := f.createInsertSQLs(model)
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

// load .sql script
func (f *Fixture) LoadSQL(path string) error {
	data, err := getFileData(path)
	if err != nil {
		return err
	}
	stmts, err := f.chooseSQLs(string(data))
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

// create sqls from .yml
func (f *Fixture) createInsertSQLs(model FixtureModel) ([]*sql.Stmt, error) {
	if model.Table == "" || len(model.Record) == 0 {
		return nil, ErrInvalidFixture
	}

	sqls := []*sql.Stmt{}
	for _, record := range model.Record {
		sql, _ := f.createInsertSQL(model.Table, record)
		stmt, err := f.db.Prepare(sql)
		if err != nil {
			return nil, err
		}
		sqls = append(sqls, stmt)
	}

	return sqls, nil
}

func (f *Fixture) createInsertSQL(table string, record map[string]string) (string, error) {
	var (
		columns []string
		values  []string
	)
	for c, v := range record {
		columns = append(columns, f.driver.EscapeKeyword(c))
		values = append(values, f.driver.EscapeValue(v))
	}
	sql := fmt.Sprintf(
		"insert into %s (%s) values (%s)",
		f.driver.EscapeKeyword(table),
		strings.Join(columns, ", "),
		strings.Join(values, ", "),
	)
	return sql, nil
}

// choose sqls from .sql
func (f *Fixture) chooseSQLs(fileData string) ([]*sql.Stmt, error) {
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
