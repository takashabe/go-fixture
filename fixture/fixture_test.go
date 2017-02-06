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

func (hd *TestDriver) TrimComment(sql string) string {
	return sql
}

func (hd *TestDriver) EscapeKeyword(keyword string) string {
	return fmt.Sprintf("`%s`", keyword)
}

func (hd *TestDriver) EscapeValue(value string) string {
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

func TestCreateSQLs(t *testing.T) {
	db := h.TestDB(t)
	defer db.Close()

	cases := []struct {
		input     string
		expectErr error
	}{
		{
			"insert into person (id, first_name, last_name) values (1, 'foo', 'bar');",
			nil,
		},
		{
			"a; b\nc;",
			&mysql.MySQLError{},
		},
	}
	for i, c := range cases {
		f := NewFixture(db, "mysql")
		_, err := f.chooseSQLs(c.input)
		if err != nil && reflect.TypeOf(err) != reflect.TypeOf(c.expectErr) {
			t.Errorf("#%d: want %v, got %v", i, c.expectErr, err)
		}
	}
}

// TODO compare database values
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
			checkSelectIDs(t, db, c.expectExistIDs, c.expectTable)
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
		{"testdata/error.yml", ErrInvalidFixture, "", nil},
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
			checkSelectIDs(t, db, c.expectExistIDs, c.expectTable)
		}
	}
}

func checkSelectIDs(t *testing.T, db *sql.DB, ids []int, table string) {
	for _, id := range ids {
		row := db.QueryRow(fmt.Sprintf("select id from %s where id=%d", table, id))
		var a int
		err := row.Scan(&a)
		if err != nil {
			t.Errorf("id%d: want no error, got %v", id, err)
		}
	}
}
