package fixture

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

func TestMain(m *testing.M) {
	setup()
	m.Run()
}

var (
	ErrTestMySQL = errors.New("for test mysql error")
	testTables   = []string{"person", "book"}
)

func setup() {
	// Generate test tables
	// NOTE: Expect that MySQL users are already created
	raw, err := ioutil.ReadFile("testdata/create.sql")
	if err != nil {
		panic(err)
	}
	db, err := connectDB()
	if err != nil {
		panic(err)
	}
	data := strings.TrimSpace(string(raw))
	for _, sql := range strings.Split(data, ";") {
		if sql == "" {
			continue
		}
		_, err = db.Exec(sql)
		if err != nil {
			panic(err)
		}
	}
	Register("mysql", &TestDriver{})
}

func connectDB() (*sql.DB, error) {
	name := getEnvWithDefault("DB_NAME", "db_fixture")
	host := getEnvWithDefault("DB_HOST", "127.0.0.1")
	port := getEnvWithDefault("DB_PORT", "3306")
	user := getEnvWithDefault("DB_USER", "root")
	pass := getEnvWithDefault("DB_PASSWORD", "")
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, pass, host, port, name)

	return sql.Open("mysql", dsn)
}

func prepareDBWithDriver(t *testing.T) *sql.DB {
	db, err := connectDB()
	if err != nil {
		t.Fatalf("failed to connect db: %v", err)
	}
	// Clear table data
	for _, name := range testTables {
		_, err = db.Exec(fmt.Sprintf("truncate table %s", name))
		if err != nil {
			t.Fatalf("failed to clear table: %s", name)
		}
	}
	return db
}

func getEnvWithDefault(name, def string) string {
	env := os.Getenv(name)
	if len(env) != 0 {
		return env
	}
	return def
}

type TestDriver struct{}

func (d *TestDriver) TrimComment(sql string) string {
	return sql
}

func (d *TestDriver) EscapeKeyword(keyword string) string {
	return fmt.Sprintf("`%s`", keyword)
}

func (d *TestDriver) EscapeValue(value string) string {
	return fmt.Sprintf("'%s'", value)
}

func (d *TestDriver) ExecSQL(tx *sql.Tx, sql string) error {
	stmt, err := tx.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec()
	return err
}

func TestNewFixture(t *testing.T) {
	cases := []struct {
		input        string
		expectDriver Driver
		expectErr    error
	}{
		{
			"mysql",
			&TestDriver{},
			nil,
		},
		{
			"none",
			nil,
			ErrNotFoundDriver,
		},
	}
	for i, c := range cases {
		f, err := NewFixture(&sql.DB{}, c.input)
		if errors.Cause(err) != c.expectErr {
			t.Errorf("#%d: want %q, got %q", i, c.expectErr, err)
		}
		if err != nil {
			continue
		}
		if f.driver != c.expectDriver {
			t.Errorf("#%d: want %q, got %q", i, c.expectDriver, err)
		}
	}
}

func TestGetFileData(t *testing.T) {
	cases := []struct {
		input      string
		expectData string
		expectErr  error
	}{
		{
			"testdata/test.yml",
			`table: person
record:
  - id: 1
    first_name: foo
    last_name: bar
  - id: 2
    first_name: piyo
    last_name: fuga`,
			nil,
		},
		{
			"testdata/test.sql",
			"insert into person (id, first_name, last_name) values (1, 'foo', 'bar');",
			nil,
		},
		{
			"testdata/none.yml",
			"",
			ErrFailReadFile,
		},
	}
	for i, c := range cases {
		result, err := getFileData(c.input)
		if errors.Cause(err) != c.expectErr {
			t.Errorf("#%d: want %s, got %s", i, c.expectErr, err)
		}
		if result != nil && c.expectData != strings.TrimRight(string(result), "\n") {
			t.Errorf("#%d: want: \n%s\n, got: \n%s", i, c.expectData, result)
		}
	}
}

func TestChooseSQLs(t *testing.T) {
	db := prepareDBWithDriver(t)
	defer db.Close()

	cases := []struct {
		input      string
		expectSQLs []string
	}{
		{
			"insert into person (id, first_name, last_name) values (1, 'foo', 'bar');desc person;",
			[]string{
				"insert into person (id, first_name, last_name) values (1, 'foo', 'bar');",
				"desc person;",
			},
		},
		{
			"a; b\nc;",
			nil,
		},
	}
	for i, c := range cases {
		f, err := NewFixture(db, "mysql")
		if err != nil {
			t.Fatalf("#%d: want non error, got %v", i, err)
		}
		tx, err := db.Begin()
		if err != nil {
			t.Errorf("#%d: want no error, got %v", i, err)
		}
		sqls, err := f.chooseSQLs(tx, c.input)
		if _, ok := err.(*mysql.MySQLError); err != nil && !ok {
			t.Errorf("#%d: want mysql.MySQLError, got %v", i, err)
		}
		if reflect.DeepEqual(sqls, c.expectSQLs) {
			t.Errorf("#%d: want %v, got %v", i, c.expectSQLs, err)
		}
	}
}

func TestCreateInsertSQLs(t *testing.T) {
	db := prepareDBWithDriver(t)
	defer db.Close()

	cases := []struct {
		input      QueryModelWithYaml
		expectSQLs []string
		expectErr  error
	}{
		{
			QueryModelWithYaml{
				Table: "person",
				Record: []map[string]string{
					{"id": "1", "first_name": "foo", "last_name": "bar"},
					{"id": "2", "first_name": "fuga", "last_name": "piyo"},
				}},
			[]string{
				"insert into `person` values ('1', 'foo', 'bar')",
				"insert into `person` values ('2', 'fuga', 'piyo')",
			},
			nil,
		},
		{
			QueryModelWithYaml{},
			[]string{},
			ErrInvalidFixture,
		},
	}
	for i, c := range cases {
		f, err := NewFixture(db, "mysql")
		if err != nil {
			t.Errorf("#%d: want no error, got %v", i, err)
		}
		tx, err := db.Begin()
		if err != nil {
			t.Errorf("#%d: want no error, got %v", i, err)
		}
		sqls, err := f.createInsertSQLs(tx, c.input)
		if err != c.expectErr {
			t.Errorf("#%d: want %v, got %v", i, c.expectErr, err)
		}
		if reflect.DeepEqual(sqls, c.expectSQLs) {
			t.Errorf("#%d: want %v, got %v", i, c.expectSQLs, sqls)
		}
	}
}

func TestClearTable(t *testing.T) {
	db := prepareDBWithDriver(t)
	defer db.Close()

	cases := []struct {
		beforeSQL        string
		input            string
		expectNotExistID int
	}{
		{
			"insert into person values (1, 'foo', 'bar')",
			"person",
			1,
		},
		{
			"insert into person values (1, 'foo', 'bar')",
			"none_table",
			0,
		},
	}
	for i, c := range cases {
		f, err := NewFixture(db, "mysql")
		if err != nil {
			t.Errorf("#%d: want no error, got %v", i, err)
		}
		tx, err := db.Begin()
		if err != nil {
			t.Errorf("#%d: want no error, got %v", i, err)
		}
		tx.Exec(c.beforeSQL)
		err = f.clearTable(tx, c.input)
		if err != nil {
			tx.Rollback()
			if _, ok := err.(*mysql.MySQLError); !ok {
				t.Errorf("#%d: want mysql.MySQLError, got %v", i, err)
			}
		} else {
			tx.Commit()
			checkSelectNotExistIDs(t, db, []int{c.expectNotExistID}, c.input)
		}
	}
}

func TestExecSQLs(t *testing.T) {
	db := prepareDBWithDriver(t)
	defer db.Close()

	cases := []struct {
		input          []string
		expectExistIDs []int
		checkTable     string
	}{
		{
			[]string{
				"insert into `person` values ('1', 'foo', 'bar')",
				"insert into `person` values ('2', 'fuga', 'piyo')",
			},
			[]int{1, 2},
			"person",
		},
		{
			[]string{"insert"},
			nil,
			"",
		},
	}
	for i, c := range cases {
		f, err := NewFixture(db, "mysql")
		if err != nil {
			t.Errorf("#%d: want no error, got %v", i, err)
		}
		tx, err := db.Begin()
		if err != nil {
			t.Errorf("#%d: want no error, got %v", i, err)
		}
		err = f.execSQLs(tx, c.input)
		if err != nil {
			tx.Rollback()
			if _, ok := err.(*mysql.MySQLError); !ok {
				t.Errorf("#%d: want mysql.MySQLError, got %v", i, err)
			}
		} else {
			tx.Commit()
			if c.expectExistIDs != nil {
				checkSelectExistIDs(t, db, c.expectExistIDs, c.checkTable)
			}
		}
	}
}

func TestLoadSQL(t *testing.T) {
	db := prepareDBWithDriver(t)
	defer db.Close()

	cases := []struct {
		input          string
		expectErr      error
		expectTable    string
		expectExistIDs []int
	}{
		{"testdata/create.sql", nil, "", nil},
		{"testdata/test.sql", nil, "person", []int{1}},
		{"testdata/error.sql", ErrTestMySQL, "", nil},
		{"testdata/none", ErrFailReadFile, "", nil},
	}
	for i, c := range cases {
		f, err := NewFixture(db, "mysql")

		if err != nil {
			t.Errorf("#%d: want no error, got %v", i, err)
		}
		err = f.LoadSQL(c.input)
		if err != nil {
			if c.expectErr == ErrTestMySQL {
				if _, ok := err.(*mysql.MySQLError); !ok {
					t.Errorf("#%d: want mysql.MySQLError, got %v", i, err)
				}
			} else if errors.Cause(err) != errors.Cause(c.expectErr) {
				t.Errorf("#%d: want %v, got %v", i, errors.Cause(c.expectErr), errors.Cause(err))
			}
		}
		if c.expectExistIDs != nil {
			checkSelectExistIDs(t, db, c.expectExistIDs, c.expectTable)
		}
	}
}

func TestLoad(t *testing.T) {
	db := prepareDBWithDriver(t)
	defer db.Close()

	cases := []struct {
		input          string
		expectErr      error
		expectTable    string
		expectExistIDs []int
	}{
		{"testdata/test.yml", nil, "person", []int{1, 2}},
		{"testdata/error.yml", ErrTestMySQL, "", nil},
		{"testdata/none", ErrFailReadFile, "", nil},
	}
	for i, c := range cases {
		f, err := NewFixture(db, "mysql")
		if err != nil {
			t.Fatalf("#%d: want non error, got %v", i, err)
		}
		err = f.Load(c.input)
		if err != nil {
			if c.expectErr == ErrTestMySQL {
				if _, ok := err.(*mysql.MySQLError); !ok {
					t.Errorf("#%d: want mysql.MySQLError, got %v", i, err)
				}
			} else if errors.Cause(err) != errors.Cause(c.expectErr) {
				t.Errorf("#%d: want %v, got %v", i, errors.Cause(c.expectErr), errors.Cause(err))
			}
		}
		if c.expectExistIDs != nil {
			checkSelectExistIDs(t, db, c.expectExistIDs, c.expectTable)
		}
	}
}

func TestLoadWithRollback(t *testing.T) {
	db := prepareDBWithDriver(t)
	defer db.Close()

	cases := []struct {
		beforeSQL        string
		input            string
		expectExistID    int
		expectNotExistID int
		checkTable       string
	}{
		{
			"insert into person values (1, 'foo', 'bar')",
			"testdata/rollback1.yml", // insert id 2, expect error by clearTable
			1,
			2,
			"person",
		},
		{
			"insert into person values (1, 'foo', 'bar')",
			"testdata/rollback2.yml", // insert id 2, expect error by execSQLs
			1,
			2,
			"person",
		},
	}
	for i, c := range cases {
		f, err := NewFixture(db, "mysql")
		if err != nil {
			t.Fatalf("#%d: want non error, got %v", i, err)
		}
		db.Exec(c.beforeSQL)
		err = f.Load(c.input)
		if _, ok := err.(*mysql.MySQLError); !ok {
			t.Errorf("#%d: want mysql.MySQLError, got %v", i, err)
		}
		checkSelectExistIDs(t, db, []int{c.expectExistID}, c.checkTable)
		checkSelectNotExistIDs(t, db, []int{c.expectNotExistID}, c.checkTable)
	}
}

func checkSelectExistIDs(t *testing.T, db *sql.DB, ids []int, table string) {
	for _, id := range ids {
		row := db.QueryRow(fmt.Sprintf("select id from %s where id=%d", table, id))
		var a int
		err := row.Scan(&a)
		if err != nil {
			t.Errorf("id%d: want no error, got %v", id, err)
		}
	}
}

func checkSelectNotExistIDs(t *testing.T, db *sql.DB, ids []int, table string) {
	for _, id := range ids {
		row := db.QueryRow(fmt.Sprintf("select id from %s where id=%d", table, id))
		var a int
		err := row.Scan(&a)
		if err == nil {
			t.Errorf("id%d: want no rows in result set, got %v", id, err)
		}
	}
}
