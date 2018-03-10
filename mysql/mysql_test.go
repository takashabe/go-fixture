package mysql

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func connectDB() (*sql.DB, error) {
	name := getEnvWithDefault("DB_NAME", "db_fixture")
	host := getEnvWithDefault("DB_HOST", "127.0.0.1")
	port := getEnvWithDefault("DB_PORT", "3306")
	user := getEnvWithDefault("DB_USER", "root")
	pass := getEnvWithDefault("DB_PASSWORD", "")
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, pass, host, port, name)

	return sql.Open("mysql", dsn)
}

func getEnvWithDefault(name, def string) string {
	env := os.Getenv(name)
	if len(env) != 0 {
		return env
	}
	return def
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
	db, err := connectDB()
	if err != nil {
		t.Fatalf("want non error, got %v", err)
	}

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
