package sab

import (
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// evalSort takes a SAB sort template plus a context map and returns the
// rendered string. Sonarr and Radarr call mode=eval_sort to preview the
// resolved path before committing an import, then refuse to talk to the
// download client if it 4xx's. So returning *something sensible* is more
// important than perfect SAB-fidelity rendering.
//
// SAB tokens we support (matching real SAB's sorter.py):
//
//	%title %t        — release title
//	%year %y         — release year
//	%season %s       — season number, no padding
//	%0s              — season number, zero-padded to width 2
//	%episode %e      — episode number, no padding
//	%0e              — episode zero-padded to width 2
//	%cat %c          — category name
//	%ext             — extension including leading dot
//	%fn              — filename without extension
//	%dn              — dotted release name (e.g. The.Movie.2020)
//	%desc            — episode title / "description"
//	%r               — resolution (e.g. 1080p)
//
// Unknown tokens pass through verbatim — *arr will treat the literal as
// part of the path, which is strictly safer than failing the request.
//
// Curly-brace tokens (e.g. {title}, {season:02d}) are also accepted as a
// convenience; *arr-side clients sometimes use this dialect when the
// download client config is shared with Sonarr v4's own naming engine.
func evalSort(template string, ctx map[string]string) string {
	if template == "" {
		return ""
	}
	out := template

	// Order matters: longer aliases first so %title doesn't get
	// shadowed by %t.
	subs := []struct {
		token string
		key   string
		pad   int // 0 = no padding, N = zero-pad to width N
	}{
		{"%title", "title", 0},
		{"%desc", "desc", 0},
		{"%cat", "cat", 0},
		{"%year", "year", 0},
		{"%fn", "fn", 0},
		{"%dn", "dn", 0},
		{"%ext", "ext", 0},
		{"%r", "r", 0},
		{"%0s", "season", 2},
		{"%season", "season", 0},
		{"%s", "season", 0},
		{"%0e", "episode", 2},
		{"%episode", "episode", 0},
		{"%e", "episode", 0},
		{"%t", "title", 0},
		{"%y", "year", 0},
		{"%c", "cat", 0},
	}
	for _, s := range subs {
		val := ctx[s.key]
		if s.pad > 0 {
			if n, err := strconv.Atoi(val); err == nil {
				val = padZero(n, s.pad)
			}
		}
		out = strings.ReplaceAll(out, s.token, val)
	}
	out = curlyTokenRe.ReplaceAllStringFunc(out, func(match string) string {
		// {name} or {name:02d}
		inner := match[1 : len(match)-1]
		parts := strings.SplitN(inner, ":", 2)
		key := strings.TrimSpace(strings.ToLower(parts[0]))
		val := ctx[key]
		if len(parts) == 2 {
			spec := parts[1]
			// Tolerate the most common form, "0Nd" (zero-pad to width N).
			if strings.HasSuffix(spec, "d") && strings.HasPrefix(spec, "0") {
				if w, err := strconv.Atoi(strings.TrimSuffix(spec[1:], "d")); err == nil {
					if n, err := strconv.Atoi(val); err == nil {
						val = padZero(n, w)
					}
				}
			}
		}
		return val
	})
	return filepath.Clean(out)
}

var curlyTokenRe = regexp.MustCompile(`\{[a-zA-Z][a-zA-Z0-9_]*(?::[^}]+)?\}`)

func padZero(n, width int) string {
	s := strconv.Itoa(n)
	if len(s) >= width {
		return s
	}
	return strings.Repeat("0", width-len(s)) + s
}

// buildSortContext extracts the canonical key set evalSort consumes from
// the form params *arr clients send. Each *arr is slightly different in
// which params it includes; this collects the union and lets missing
// values render as empty (which evalSort tolerates).
func buildSortContext(getter func(string) string) map[string]string {
	return map[string]string{
		"title":   getter("title"),
		"year":    getter("year"),
		"season":  firstNonEmpty(getter("season"), getter("season_num")),
		"episode": firstNonEmpty(getter("episode"), getter("episode_num")),
		"cat":     getter("cat"),
		"ext":     firstNonEmpty(getter("ext"), ".mkv"),
		"fn":      getter("fn"),
		"dn":      getter("dn"),
		"desc":    firstNonEmpty(getter("desc"), getter("episode_title")),
		"r":       firstNonEmpty(getter("resolution"), getter("r")),
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
