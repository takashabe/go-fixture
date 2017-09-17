package mysql

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/takashabe/go-fixture"
)

// FixtureDriver is mysql driver
type FixtureDriver struct{}

// EscapeKeyword define keyword escape
func (d *FixtureDriver) EscapeKeyword(keyword string) string {
	return fmt.Sprintf("`%s`", keyword)
}

// EscapeValue define value escape
func (d *FixtureDriver) EscapeValue(value string) string {
	return fmt.Sprintf("'%s'", value)
}

// TrimComment define comment trimmed
func (d *FixtureDriver) TrimComment(sql string) string {
	result := ""
	for _, line := range strings.Split(sql, "\n") {
		result += trimLineComment(line)
	}
	return trimBlockComment(result)
}

// ExecSQL exec a sql statement
func (d *FixtureDriver) ExecSQL(tx *sql.Tx, sql string) error {
	stmt, err := tx.Prepare(sql)
	if err != nil {
		// non supported prepare statement
		if strings.HasPrefix(err.Error(), "Error 1295:") {
			_, err := tx.Exec(sql)
			return err
		}
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec()
	return err
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
	fixture.Register("mysql", &FixtureDriver{})
}
