package fixture

import (
	"database/sql"
	"reflect"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

type Helper struct{}

type TestDriver struct{}

var h Helper = Helper{}

// TODO changeable DB by env
func (h *Helper) TestDB(t *testing.T) *sql.DB {
	Register("mysql", &TestDriver{})
	db, err := sql.Open("mysql", "db_fixture@/db_fixture")
	if err != nil {
		t.Fatal("failed to open db")
	}
	return db
}

func (hd *TestDriver) TrimComment(sql string) string {
	return sql
}

func TestGetFileData(t *testing.T) {
	cases := []struct {
		input      string
		expectData string
		expectErr  error
	}{
		{
			"testdata/test.yml",
			`- table: foo
  records:
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
			"delete from person where id=1;\ninsert into person (id, first_name, last_name) values (1, 'foo', 'bar');",
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
			"insert into person (id, first_name, last_name) person (1, foo, bar);",
			nil,
		},
		{
			"a; b\nc;",
			&mysql.MySQLError{},
		},
	}
	for i, c := range cases {
		f := NewFixture(db, "mysql")
		_, err := f.createSQLs(c.input)
		if c.expectErr != nil && reflect.TypeOf(err) != reflect.TypeOf(c.expectErr) {
			t.Errorf("#%d: want %v, got %v", i, c.expectErr, err)
		}
	}
}

func TestLoadSQL(t *testing.T) {
	db := h.TestDB(t)
	defer db.Close()

	cases := []struct {
		input     string
		expectErr error
	}{
		{"testdata/create.sql", nil},
		{"testdata/test.sql", nil},
		{"testdata/error.sql", &mysql.MySQLError{}},
	}
	for i, c := range cases {
		f := NewFixture(db, "mysql")
		err := f.LoadSQL(c.input)
		if c.expectErr != nil && reflect.TypeOf(err) != reflect.TypeOf(c.expectErr) {
			t.Errorf("#%d: want %v, got %v", i, c.expectErr, err)
		}
	}
}
