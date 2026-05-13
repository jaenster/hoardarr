package deliver

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIsProbablyObfuscated(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		// Clear obfuscation patterns.
		{"abcdef1234567890abcdef1234567890.mkv", true},
		{"aaaabbbbccccddddeeeeffff00001111.mkv", true},
		{"abc.xyz.something.mkv", true},

		// Hand-named — mixed case + separator.
		{"Great.Movie.2020.1080p.WEB-DL.x264.mkv", false},
		{"Some_Series_S01E01_Title.mkv", false},
		{"My Movie (2019).mkv", false},

		// Long lowercase release-name (typical scene release).
		{"some.lower.case.movie.2020.mkv", false},

		// Hex-looking but short enough to not trip the 32-hex rule and
		// has structural cues of a real name.
		{"deadbeef.feedface.zip", false},

		// Short random — neither obfuscated nor hand-named-shaped.
		{"xyz.mkv", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isProbablyObfuscated(tc.name)
			if got != tc.want {
				t.Fatalf("isProbablyObfuscated(%q) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestIsExcludedExt(t *testing.T) {
	cases := map[string]bool{
		"movie.rar":     true,
		"movie.RAR":     true,
		"set.vol01.par2": true,
		"disc.bdmv":     true,
		"chapter.vob":   true,
		"index.ifo":     true,
		"subs.srt":      true,
		"movie.mkv":     false,
		"audio.mp3":     false,
		"podcast.m4a":   false,
	}
	for n, want := range cases {
		t.Run(n, func(t *testing.T) {
			if got := isExcludedExt(n); got != want {
				t.Fatalf("isExcludedExt(%q) = %v, want %v", n, got, want)
			}
		})
	}
}

func TestIsDiscStructure(t *testing.T) {
	dir := t.TempDir()
	if got := isDiscStructure(dir); got {
		t.Fatalf("empty dir reported as disc structure")
	}
	if err := os.Mkdir(filepath.Join(dir, "VIDEO_TS"), 0o755); err != nil {
		t.Fatal(err)
	}
	if got := isDiscStructure(dir); !got {
		t.Fatalf("VIDEO_TS dir not detected as disc structure")
	}

	bd := t.TempDir()
	if err := os.Mkdir(filepath.Join(bd, "BDMV"), 0o755); err != nil {
		t.Fatal(err)
	}
	if got := isDiscStructure(bd); !got {
		t.Fatalf("BDMV dir not detected as disc structure")
	}
}
