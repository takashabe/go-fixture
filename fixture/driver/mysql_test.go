package mysql

import "testing"

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
		m := &MySQLDriver{}
		actual := m.TrimComment(c.input)
		if actual != c.expect {
			t.Errorf("#%d: want %v, got %v", i, c.expect, actual)
		}
	}
}
