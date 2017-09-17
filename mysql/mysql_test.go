package mysql

import (
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func testDB(t *testing.T) *sql.DB {
	db, err := sql.Open("mysql", "db_fixture@/db_fixture")
	if err != nil {
		t.Fatal("failed to open db")
	}
	return db
}

func TestTrimComment(t *testing.T) {
	cases := []struct {
		input  string
		expect string
	}{
		{
			"-- comment\na; -- comment\nb;# comment",
			"a; b;",
		},
		{
			"/* comment */c; \nd;/*\ne;\n*/f;-- comment\n",
			"c; d;f;",
		},
	}
	for i, c := range cases {
		m := &FixtureDriver{}
		actual := m.TrimComment(c.input)
		if actual != c.expect {
			t.Errorf("#%d: want %v, got %v", i, c.expect, actual)
		}
	}
}

func TestEscapeKeyword(t *testing.T) {
	cases := []struct {
		input  string
		expect string
	}{
		{"foo", "`foo`"},
	}
	for i, c := range cases {
		m := &FixtureDriver{}
		actual := m.EscapeKeyword(c.input)
		if actual != c.expect {
			t.Errorf("#%d: want %v, got %v", i, c.expect, actual)
		}
	}
}

func TestEscapeValue(t *testing.T) {
	cases := []struct {
		input  string
		expect string
	}{
		{"foo", "'foo'"},
	}
	for i, c := range cases {
		m := &FixtureDriver{}
		actual := m.EscapeValue(c.input)
		if actual != c.expect {
			t.Errorf("#%d: want %v, got %v", i, c.expect, actual)
		}
	}
}

func TestExecSQL(t *testing.T) {
	cases := []struct {
		input string
	}{
		{
			"use db_fixture;",
		},
		{
			`create table if not exists book (
  id int unsigned,
  name varchar(255) not null,
  content text not null,
  primary key(id)
);`,
		},
	}
	for i, c := range cases {
		db := testDB(t)
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("#%d: want non error, got %v", i, err)
		}
		m := &FixtureDriver{}
		err = m.ExecSQL(tx, c.input)
		if err != nil {
			t.Errorf("#%d: want non error, got %v", i, err)
		}
	}
}
