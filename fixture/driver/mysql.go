package mysql

import "github.com/takashabe/go-db-fixture/fixture"

type MySQLDriver struct {
}

func (m *MySQLDriver) TrimComment(sql string) string {
	return sql
}

func init() {
	fixture.Register("mysql", &MySQLDriver{})
}
