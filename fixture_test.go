package fixture

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

type Helper struct{}

type TestDriver struct{}

var (
	ErrTestMySQL = errors.New("for test mysql error")

	h          = Helper{}
	testTables = []string{"person", "book"}
)

// TODO changeable DB by env
func (h *Helper) TestDB(t *testing.T) *sql.DB {
	Register("mysql", &TestDriver{})
	db, err := sql.Open("mysql", "db_fixture@/db_fixture")
	if err != nil {
		t.Fatal("failed to open db")
	}
	for _, v := range testTables {
		h.ClearTable(t, db, v)
	}
	return db
}

func (h *Helper) ClearTable(t *testing.T, db *sql.DB, tableName string) {
	sql := fmt.Sprintf("truncate table %s", tableName)
	_, err := db.Exec(sql)
	if err != nil {
		t.Fatalf("failed to clear table: %s", tableName)
	}
}

func (d *TestDriver) TrimComment(sql string) string {
	return sql
}

func (d *TestDriver) EscapeKeyword(keyword string) string {
	return fmt.Sprintf("`%s`", keyword)
}

func (d *TestDriver) EscapeValue(value string) string {
	return fmt.Sprintf("'%s'", value)
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
	db := h.TestDB(t)
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
		f := NewFixture(db, "mysql")
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
	db := h.TestDB(t)
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
		f := NewFixture(db, "mysql")
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
	db := h.TestDB(t)
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
		f := NewFixture(db, "mysql")
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
	db := h.TestDB(t)
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
		f := NewFixture(db, "mysql")
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
	db := h.TestDB(t)
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
		f := NewFixture(db, "mysql")
		err := f.LoadSQL(c.input)
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
	db := h.TestDB(t)
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
		f := NewFixture(db, "mysql")
		err := f.Load(c.input)
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
	db := h.TestDB(t)
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
		f := NewFixture(db, "mysql")
		db.Exec(c.beforeSQL)
		err := f.Load(c.input)
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
