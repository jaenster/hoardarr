package deliver

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"unicode"
)

// isProbablyObfuscated returns true when basename looks like an
// auto-generated obfuscated name (32-hex, long hex+dots chains, the
// magic abc.xyz prefix, or hex-in-brackets indexing). Returns false on
// names that have the structural cues of a hand-named release (mixed
// case + separators, lots of separators, letters-plus-digits with
// separators, or a proper-noun-shaped capitalised name).
//
// Port of SABnzbd's is_probably_obfuscated() in deobfuscate_filenames.py.
// Used as a guard before any fallback rename — without it we'd cheerfully
// destroy correctly-named files.
func isProbablyObfuscated(name string) bool {
	base := strings.TrimSuffix(filepath.Base(name), filepath.Ext(name))
	base = strings.ToLower(base)

	if reHex32.MatchString(base) {
		return true
	}
	if reHexDotsLong.MatchString(base) {
		return true
	}
	if reHex30.MatchString(base) && countBracketTokens(base) >= 2 {
		return true
	}
	if strings.HasPrefix(base, "abc.xyz") {
		return true
	}
	if hasHandNamedShape(name) {
		return false
	}
	return false
}

var (
	reHex32       = regexp.MustCompile(`^[a-f0-9]{32}$`)
	reHexDotsLong = regexp.MustCompile(`^[a-f0-9.]{40,}$`)
	reHex30       = regexp.MustCompile(`[a-f0-9]{30}`)
	reBracketTok  = regexp.MustCompile(`\[[\w+]+\]`)
)

func countBracketTokens(s string) int {
	return len(reBracketTok.FindAllString(s, -1))
}

// hasHandNamedShape returns true on names that look like a human typed
// them (Great.Movie.2020 etc). When this trips, isProbablyObfuscated
// short-circuits to false even if some weak regex would otherwise match.
func hasHandNamedShape(name string) bool {
	var upper, lower, digits, seps int
	for _, r := range name {
		switch {
		case unicode.IsUpper(r):
			upper++
		case unicode.IsLower(r):
			lower++
		case unicode.IsDigit(r):
			digits++
		case r == '.' || r == ' ' || r == '_' || r == '-':
			seps++
		}
	}
	if upper >= 2 && lower >= 2 && seps >= 1 {
		return true
	}
	if seps >= 3 {
		return true
	}
	letters := upper + lower
	if letters >= 4 && digits >= 4 && seps >= 1 {
		return true
	}
	if letters > 0 {
		runes := []rune(name)
		if unicode.IsUpper(runes[0]) {
			ratio := float64(upper) / float64(letters)
			if ratio < 0.25 {
				return true
			}
		}
	}
	return false
}

// excludedExts mirrors SABnzbd's EXCLUDED_FILE_EXTS — files we must
// never rename. Archive parts (.rar), parity (.par2), disc-image
// metadata (.bdmv/.vob/.ifo/.bup), and sidecars that are too small to be
// "the main file" anyway.
var excludedExts = map[string]struct{}{
	".rar":  {},
	".par2": {},
	".mts":  {},
	".m2ts": {},
	".bdmv": {},
	".vob":  {},
	".ifo":  {},
	".bup":  {},
	".sfv":  {},
	".nzb":  {},
	".srt":  {},
	".idx":  {},
	".sub":  {},
}

// isExcludedExt returns true when the file's extension marks it as
// off-limits for fallback renaming (archive parts, parity, disc media,
// sidecars).
func isExcludedExt(name string) bool {
	_, ok := excludedExts[strings.ToLower(filepath.Ext(name))]
	return ok
}

// isDiscStructure returns true when dir contains a recognisable DVD or
// Blu-ray layout. Renaming files inside these would destroy the disc;
// callers must skip deobfuscation entirely when this trips.
func isDiscStructure(dir string) bool {
	for _, marker := range discMarkers {
		fi, err := os.Stat(filepath.Join(dir, marker))
		if err == nil && fi.IsDir() {
			return true
		}
	}
	return false
}

var discMarkers = []string{"VIDEO_TS", "AUDIO_TS", "BDMV", "CERTIFICATE"}
