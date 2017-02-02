package mysql

import "testing"

func TestTrimComment(t *testing.T) {
	cases := []struct {
		input  string
		expect string
	}{
		{"", ""},
	}
	for i, c := range cases {
		m := &MySQLDriver{}
		actual := m.TrimComment(c.input)
		if actual != c.expect {
			t.Errorf("#%d: want %v, got %v", i, c.expect, actual)
		}
	}
}
