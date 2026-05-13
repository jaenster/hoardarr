package sab

import "testing"

func TestEvalSort_SABTokens(t *testing.T) {
	ctx := map[string]string{
		"title":   "Great Show",
		"year":    "2020",
		"season":  "3",
		"episode": "7",
		"cat":     "tv",
		"ext":     ".mkv",
		"r":       "1080p",
	}
	cases := map[string]string{
		// %ext already includes the leading dot, so templates shouldn't
		// add their own.
		"%title (%year)/Season %0s/S%0sE%0e%ext":          "Great Show (2020)/Season 03/S03E07.mkv",
		"%t (%y) %r":                                      "Great Show (2020) 1080p",
		"%cat/%title/%t.S%0sE%0e%ext":                     "tv/Great Show/Great Show.S03E07.mkv",
		"%s.%e":                                           "3.7",
		"{title} ({year})/Season {season:02d}/S{season:02d}E{episode:02d}{ext}": "Great Show (2020)/Season 03/S03E07.mkv",
	}
	for tmpl, want := range cases {
		t.Run(tmpl, func(t *testing.T) {
			if got := evalSort(tmpl, ctx); got != want {
				t.Errorf("evalSort(%q) = %q, want %q", tmpl, got, want)
			}
		})
	}
}

func TestEvalSort_PreservesUnknownTokens(t *testing.T) {
	got := evalSort("%title %unknownpercent {also_unknown}", map[string]string{"title": "X"})
	// %unknownpercent isn't a registered token so it passes through;
	// {also_unknown} resolves to empty string (curly tokens always
	// substitute, just to nothing). filepath.Clean keeps the trailing
	// run of whitespace as part of the path element.
	want := "X %unknownpercent "
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestBuildSortContext_Aliases(t *testing.T) {
	form := map[string]string{
		"title":      "Foo",
		"season_num": "1",
		"resolution": "720p",
	}
	got := buildSortContext(func(k string) string { return form[k] })
	if got["title"] != "Foo" {
		t.Fatalf("title: got %q", got["title"])
	}
	if got["season"] != "1" {
		t.Fatalf("season alias from season_num failed: %q", got["season"])
	}
	if got["r"] != "720p" {
		t.Fatalf("r alias from resolution failed: %q", got["r"])
	}
	if got["ext"] != ".mkv" {
		t.Fatalf("ext default missing: %q", got["ext"])
	}
}

func TestFilenameFromURL(t *testing.T) {
	cases := map[string]string{
		"https://indexer.example/getnzb?id=abc.nzb&apikey=k": "getnzb",
		"https://nzb.example/foo/bar/release.name.nzb":       "release.name",
		"https://example.com/r.nzb":                          "r",
		"":                                                   "",
	}
	for in, want := range cases {
		if got := filenameFromURL(in); got != want {
			t.Errorf("filenameFromURL(%q) = %q, want %q", in, got, want)
		}
	}
}
