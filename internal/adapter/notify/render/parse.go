package render

import (
	"fmt"
	"regexp"
	"strings"
)

// CleanReleaseName converts a typical scene release name into something
// pleasant enough for a notification title.
//
//   Euphoria.US.S03E06.PROPER.MULTi.DV.HDR.2160p.WEB.H265-HiggsBoson
//     → "Euphoria US S03E06 PROPER MULTi DV HDR 2160p WEB H265"
//
// We strip the trailing release group (everything from the last "-" to
// end of string when the suffix has no spaces). Dots become spaces.
// Underscores become spaces. Everything else is left intact — we
// deliberately do *not* try to extract just the show title because
// that requires metadata we don't have (TVDB/TMDB).
func CleanReleaseName(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	// Strip a trailing -GROUP token. We use the *last* hyphen, and only
	// strip if the suffix has no spaces and looks like a release group
	// (alnum, no path separators). Movie titles can legitimately contain
	// hyphens ("Mission: Impossible - Fallout") so we guard against
	// nuking part of the title.
	if i := strings.LastIndex(s, "-"); i >= 0 && i < len(s)-1 {
		suf := s[i+1:]
		if isReleaseGroup(suf) {
			s = strings.TrimRight(s[:i], " .")
		}
	}
	s = strings.ReplaceAll(s, ".", " ")
	s = strings.ReplaceAll(s, "_", " ")
	// Collapse runs of whitespace.
	return collapseWS(s)
}

// isReleaseGroup is a heuristic: the suffix is treated as a release
// group when it's reasonably short, ASCII-alnum (plus a small set of
// allowed punctuation), and has no spaces. Avoids stripping subtitle
// suffixes like " - The Movie".
func isReleaseGroup(s string) bool {
	if s == "" || len(s) > 24 {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '_' || r == '.':
		default:
			return false
		}
	}
	return true
}

func collapseWS(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	prev := byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == ' ' || c == '\t' {
			if prev == ' ' {
				continue
			}
			b.WriteByte(' ')
			prev = ' '
			continue
		}
		b.WriteByte(c)
		prev = c
	}
	return strings.TrimSpace(b.String())
}

// qualityRE matches the most common quality tokens scene/p2p releases
// use. Resolution alone ("1080p") is recognised; source alone ("WEB-DL",
// "BluRay") is recognised; combined output is "Source Resolution"
// (e.g. "WEB-DL 2160p") to mirror Sonarr's quality string.
var (
	resRE    = regexp.MustCompile(`(?i)\b(2160p|1080p|720p|480p|4k|uhd)\b`)
	sourceRE = regexp.MustCompile(`(?i)\b(WEB[- ]?DL|WEBRip|WEB|BluRay|BDRip|BRRip|HDRip|DVDRip|HDTV|PDTV|REMUX)\b`)
)

// ParseQuality returns a short quality string parsed from the release
// name. Returns "" if neither a resolution nor a source token is found.
func ParseQuality(release string) string {
	if release == "" {
		return ""
	}
	src := sourceRE.FindString(release)
	res := resRE.FindString(release)
	src = normaliseSource(src)
	res = strings.ToLower(res)
	switch {
	case src != "" && res != "":
		return src + " " + res
	case src != "":
		return src
	case res != "":
		return res
	default:
		return ""
	}
}

// normaliseSource canonicalises source casing/spacing.
func normaliseSource(s string) string {
	switch strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(s, "-", ""), " ", "")) {
	case "":
		return ""
	case "webdl":
		return "WEB-DL"
	case "webrip":
		return "WEBRip"
	case "web":
		return "WEB"
	case "bluray":
		return "BluRay"
	case "bdrip":
		return "BDRip"
	case "brrip":
		return "BRRip"
	case "hdrip":
		return "HDRip"
	case "dvdrip":
		return "DVDRip"
	case "hdtv":
		return "HDTV"
	case "pdtv":
		return "PDTV"
	case "remux":
		return "REMUX"
	default:
		return s
	}
}

// SourceName turns the job's source (which is the requesting client's
// User-Agent, captured at addfile time per the SAB-API enrichment) into
// a friendly app name. Unknown UAs are passed through verbatim so the
// operator can still tell who sent the job. Empty UA → "Manual" (the
// UI uploads NZBs without a UA hint).
func SourceName(ua string) string {
	s := strings.ToLower(strings.TrimSpace(ua))
	switch {
	case s == "":
		return "Manual"
	case strings.Contains(s, "sonarr"):
		return "Sonarr"
	case strings.Contains(s, "radarr"):
		return "Radarr"
	case strings.Contains(s, "lidarr"):
		return "Lidarr"
	case strings.Contains(s, "readarr"):
		return "Readarr"
	case strings.Contains(s, "prowlarr"):
		return "Prowlarr"
	case strings.Contains(s, "whisparr"):
		return "Whisparr"
	default:
		return ua
	}
}

// BytesHuman renders byte counts as SAB-style "12.34 GB" / "456 MB".
// 1024-base (so MiB-as-MB) which matches the rest of hoardarr's wire
// formatters in api/sab/dto.go.
func BytesHuman(n int64) string {
	if n < 0 {
		n = 0
	}
	const k = 1024
	switch {
	case n < k:
		return fmt.Sprintf("%d B", n)
	case n < k*k:
		return fmt.Sprintf("%.2f KB", float64(n)/k)
	case n < k*k*k:
		return fmt.Sprintf("%.2f MB", float64(n)/(k*k))
	default:
		return fmt.Sprintf("%.2f GB", float64(n)/(k*k*k))
	}
}
