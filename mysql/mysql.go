package mysql

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/takashabe/go-db-fixture/fixture"
)

type MySQLDriver struct {
}

func (d *MySQLDriver) EscapeKeyword(keyword string) string {
	return fmt.Sprintf("`%s`", keyword)
}

func (d *MySQLDriver) EscapeValue(value string) string {
	return fmt.Sprintf("'%s'", value)
}

func (m *MySQLDriver) TrimComment(sql string) string {
	result := ""
	for _, line := range strings.Split(sql, "\n") {
		result += trimLineComment(line)
	}
	return trimBlockComment(result)
}

func trimLineComment(s string) string {
	if i := strings.Index(s, "--"); 0 <= i {
		return s[:i]
	}
	if i := strings.Index(s, "#"); 0 <= i {
		return s[:i]
	}
	return s
}

var blockCommentRegex = regexp.MustCompile(`(?m)/\*(.*?)\*/`)

func trimBlockComment(s string) string {
	return blockCommentRegex.ReplaceAllString(s, "")
}

func init() {
	fixture.Register("mysql", &MySQLDriver{})
}
