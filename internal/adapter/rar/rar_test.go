package rar

import (
	"strings"
	"testing"
)

func TestPickFirstVolume(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want string
	}{
		{
			name: "single file",
			in:   []string{"/tmp/a.rar"},
			want: "/tmp/a.rar",
		},
		{
			name: "RAR5 part1",
			in:   []string{"/tmp/x.part2.rar", "/tmp/x.part1.rar", "/tmp/x.part3.rar"},
			want: "/tmp/x.part1.rar",
		},
		{
			name: "RAR5 part01",
			in:   []string{"/tmp/x.part02.rar", "/tmp/x.part01.rar"},
			want: "/tmp/x.part01.rar",
		},
		{
			name: "classic .rar with siblings",
			in:   []string{"/tmp/x.r02", "/tmp/x.rar", "/tmp/x.r01"},
			want: "/tmp/x.rar",
		},
		{
			name: "alphabetically smallest .rar",
			in:   []string{"/tmp/zzz.rar", "/tmp/aaa.rar"},
			want: "/tmp/aaa.rar",
		},
		{
			name: "no .rar — fall back to first sorted",
			in:   []string{"/tmp/x.r02", "/tmp/x.r01"},
			want: "/tmp/x.r01",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := pickFirstVolume(tc.in)
			if got != tc.want {
				t.Errorf("got %q; want %q", got, tc.want)
			}
		})
	}
}

func TestExtract_RejectsMissingArchive(t *testing.T) {
	// Trying to extract a non-existent file should error cleanly,
	// not panic or leave temp files around.
	_, err := Extractor{}.Extract(t.Context(), []string{"/nonexistent/missing.rar"}, t.TempDir())
	if err == nil {
		t.Fatal("expected error opening nonexistent archive")
	}
	if !strings.Contains(err.Error(), "rar: open") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestExtract_NoPaths(t *testing.T) {
	_, err := Extractor{}.Extract(t.Context(), nil, t.TempDir())
	if err == nil {
		t.Fatal("expected error on empty input")
	}
}
