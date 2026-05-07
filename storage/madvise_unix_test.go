//go:build unix

package storage

import (
	"testing"

	"github.com/go-quicktest/qt"
	"golang.org/x/sys/unix"
)

func TestParsePostMapAdvice(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want []int
	}{
		{"", nil},
		{"random", []int{unix.MADV_RANDOM}},
		{"sequential,willneed", []int{unix.MADV_SEQUENTIAL, unix.MADV_WILLNEED}},
		{"normal", nil},
		{"random,bogus,willneed", []int{unix.MADV_RANDOM, unix.MADV_WILLNEED}},
	}
	for _, c := range cases {
		got := parsePostMapAdvice(c.in)
		qt.Assert(t, qt.DeepEquals(got, c.want), qt.Commentf("input=%q", c.in))
	}
}
