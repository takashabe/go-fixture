package fixture

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

// error variables
var (
	ErrFailRegisterDriver = errors.New("failed to register driver")
	ErrFailReadFile       = errors.New("failed to read file")
	ErrInvalidFixture     = errors.New("invalid fixture file format")
)

// db driver. register by any driver files
var drivers = make(map[string]Driver)

// Driver is database adapter
type Driver interface {
	TrimComment(sql string) string
	EscapeKeyword(keyword string) string
	EscapeValue(value string) string
}

// Fixture supply fixture methods
// sample:
//   import _ "github.com/takashabe/go-fixture/mysql"
//   ...
//     fixture.Load("path/to/hoge.yml", &Hoge{})
type Fixture struct {
	db     *sql.DB
	driver Driver
}

// QueryModelWithYaml represent fixture yaml file mapper
type QueryModelWithYaml struct {
	Table  string              `yaml:"table"`
	Record []map[string]string `yaml:"record"`
}

// Register registers driver
func Register(name string, driver Driver) error {
	if _, dup := drivers[name]; driver == nil || dup {
		return ErrFailRegisterDriver
	}
	drivers[name] = driver
	return nil
}

// NewFixture returns initialized Fixture
func NewFixture(db *sql.DB, driverName string) *Fixture {
	return &Fixture{db: db, driver: drivers[driverName]}
}

// Load load .yml script
func (f *Fixture) Load(path string) error {
	data, err := getFileData(path)
	if err != nil {
		return err
	}
	model := QueryModelWithYaml{}
	err = yaml.Unmarshal(data, &model)
	if err != nil {
		return errors.Wrapf(ErrInvalidFixture, ": err %v", err)
	}

	tx, err := f.db.Begin()
	if err != nil {
		return err
	}
	err = f.clearTable(tx, model.Table)
	if err != nil {
		tx.Rollback()
		return err
	}

	sqls, err := f.createInsertSQLs(tx, model)
	if err != nil {
		return err
	}
	err = f.execSQLs(tx, sqls)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	return nil
}

// LoadSQL load .sql script
func (f *Fixture) LoadSQL(path string) error {
	data, err := getFileData(path)
	if err != nil {
		return err
	}

	tx, err := f.db.Begin()
	if err != nil {
		return err
	}
	sqls, err := f.chooseSQLs(tx, string(data))
	if err != nil {
		return err
	}
	err = f.execSQLs(tx, sqls)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	return nil
}

func (f *Fixture) clearTable(tx *sql.Tx, tableName string) error {
	_, err := tx.Exec(fmt.Sprintf("delete from %s", f.driver.EscapeKeyword(tableName)))
	if err != nil {
		return err
	}
	return nil
}

func (f *Fixture) execSQLs(tx *sql.Tx, sqls []string) error {
	for _, sql := range sqls {
		stmt, err := tx.Prepare(sql)
		if err != nil {
			return err
		}
		defer stmt.Close()
		_, err = stmt.Exec()
		if err != nil {
			return err
		}
	}
	return nil
}

// create sqls from .yml
func (f *Fixture) createInsertSQLs(tx *sql.Tx, model QueryModelWithYaml) ([]string, error) {
	if model.Table == "" || len(model.Record) == 0 {
		return nil, ErrInvalidFixture
	}

	sqls := []string{}
	for _, record := range model.Record {
		sql, _ := f.createInsertSQL(model.Table, record)
		sqls = append(sqls, sql)
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
func (f *Fixture) chooseSQLs(tx *sql.Tx, fileData string) ([]string, error) {
	sqls := []string{}
	sql := strings.Trim(fileData, "\n")
	sql = f.driver.TrimComment(sql)
	for _, line := range strings.Split(sql, ";") {
		if line == "" {
			continue
		}
		sqls = append(sqls, line)
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
