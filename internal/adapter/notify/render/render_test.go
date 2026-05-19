package render

import (
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/event"
)

func TestVerb(t *testing.T) {
	cases := map[string]string{
		"download.job.created":   "Added to queue",
		"download.job.completed": "Completed",
		"download.job.failed":    "Failed",
		"verify.ok":              "Verified",
		"verify.repair_needed":   "Repair needed",
		"deliver.complete":       "Delivered",
		"deliver.failed":         "Delivery failed",
		"notify.test":            "Test notification",
		"unknown.topic":          "unknown.topic",
	}
	for topic, want := range cases {
		if got := Verb(topic); got != want {
			t.Errorf("Verb(%q) = %q; want %q", topic, got, want)
		}
	}
}

func TestOutcomeFor(t *testing.T) {
	cases := map[string]Outcome{
		"verify.ok":             OutcomeOK,
		"repair.failed":         OutcomeFail,
		"download.job.failed":   OutcomeFail,
		"verify.repair_needed":  OutcomeWarn,
		"deliver.complete":      OutcomeOK,
		"download.job.created":  OutcomeInfo,
		"download.job.completed": OutcomeOK,
	}
	for topic, want := range cases {
		if got := OutcomeFor(topic); got != want {
			t.Errorf("OutcomeFor(%q) = %v; want %v", topic, got, want)
		}
	}
}

func TestColor(t *testing.T) {
	if OutcomeOK.Color() != 0x2ECC71 {
		t.Errorf("OK colour drifted from green")
	}
	if OutcomeFail.Color() != 0xF85149 {
		t.Errorf("Fail colour drifted from red")
	}
}

func TestCleanReleaseName(t *testing.T) {
	cases := map[string]string{
		"":                              "",
		"Foo.Bar.S01E02.1080p.WEB-RLS":  "Foo Bar S01E02 1080p WEB",
		"Euphoria.US.S03E06.PROPER.MULTi.DV.HDR.2160p.WEB.H265-HiggsBoson": "Euphoria US S03E06 PROPER MULTi DV HDR 2160p WEB H265",
		// Movie titles with hyphens in the title (e.g. subtitle) — only
		// last token should be considered for stripping. " - The Movie"
		// has a space in the suffix so we keep it.
		"Some.Title - The Movie.2024.1080p": "Some Title - The Movie 2024 1080p",
		// Underscore-separated names get normalised too.
		"foo_bar_2024-RLSGRP": "foo bar 2024",
	}
	for in, want := range cases {
		if got := CleanReleaseName(in); got != want {
			t.Errorf("CleanReleaseName(%q) = %q; want %q", in, got, want)
		}
	}
}

func TestParseQuality(t *testing.T) {
	cases := map[string]string{
		"Foo.S01E01.2160p.WEB-DL.H265-X":   "WEB-DL 2160p",
		"Foo.S01E01.1080p.BluRay-X":        "BluRay 1080p",
		"Foo.720p.HDTV-X":                  "HDTV 720p",
		"Foo.WEBRip-X":                     "WEBRip",
		"Foo.2024.1080p":                   "1080p",
		"Foo":                              "",
		"":                                 "",
	}
	for in, want := range cases {
		if got := ParseQuality(in); got != want {
			t.Errorf("ParseQuality(%q) = %q; want %q", in, got, want)
		}
	}
}

func TestSourceName(t *testing.T) {
	cases := map[string]string{
		"":                       "Manual",
		"Sonarr/4.0.0.123":       "Sonarr",
		"radarr/5.0":             "Radarr",
		"Prowlarr/1.10":          "Prowlarr",
		"unknownclient/1.0":      "unknownclient/1.0",
	}
	for in, want := range cases {
		if got := SourceName(in); got != want {
			t.Errorf("SourceName(%q) = %q; want %q", in, got, want)
		}
	}
}

func TestBytesHuman(t *testing.T) {
	cases := map[int64]string{
		0:              "0 B",
		500:            "500 B",
		2 * 1024:       "2.00 KB",
		5 * 1024 * 1024: "5.00 MB",
		11_500_000_000: "10.71 GB",
	}
	for in, want := range cases {
		if got := BytesHuman(in); got != want {
			t.Errorf("BytesHuman(%d) = %q; want %q", in, got, want)
		}
	}
}

func TestFrom_EnrichedEnvelope(t *testing.T) {
	// Mimics what app/notify.Service.enrichEnvelope produces.
	payload := []byte(`{
		"event": {"job_id": 148},
		"job": {
			"id": 148,
			"name": "Euphoria.US.S03E06.PROPER.MULTi.DV.HDR.2160p.WEB.H265-HiggsBoson",
			"category": "tv",
			"state": "completed",
			"source": "Sonarr/4.0.0.123",
			"total_bytes": 11500000000,
			"file_count": 7
		}
	}`)
	env := event.Envelope{
		Topic:       "deliver.complete",
		AggregateID: "148",
		OccurredAt:  time.Unix(1_700_000_000, 0).UTC(),
		Payload:     payload,
	}
	v := From(env)
	if v.Verb != "Delivered" {
		t.Errorf("Verb = %q; want Delivered", v.Verb)
	}
	if v.Outcome != OutcomeOK {
		t.Errorf("Outcome = %v; want OK", v.Outcome)
	}
	if v.Source != "Sonarr" {
		t.Errorf("Source = %q; want Sonarr", v.Source)
	}
	if v.Category != "tv" {
		t.Errorf("Category = %q; want tv", v.Category)
	}
	if v.SizeHuman != "10.71 GB" {
		t.Errorf("SizeHuman = %q; want 10.71 GB", v.SizeHuman)
	}
	if v.FileCount != 7 {
		t.Errorf("FileCount = %d; want 7", v.FileCount)
	}
	if v.State != "completed" {
		t.Errorf("State = %q; want completed", v.State)
	}
	if v.Release == "" {
		t.Errorf("Release should be set")
	}
	if v.CleanTitle == "" || v.CleanTitle == v.Release {
		t.Errorf("CleanTitle should be derived: got %q", v.CleanTitle)
	}
	if v.Quality != "WEB 2160p" && v.Quality != "WEB-DL 2160p" {
		t.Errorf("Quality = %q; want WEB or WEB-DL prefix with 2160p", v.Quality)
	}
}

func TestFrom_UnhydratedEnvelope(t *testing.T) {
	// Topic with no job_id — notify.test case.
	env := event.Envelope{
		Topic:       "notify.test",
		AggregateID: "0",
		OccurredAt:  time.Unix(1_700_000_000, 0).UTC(),
		Payload:     []byte(`{"name":"smoke probe"}`),
	}
	v := From(env)
	if v.Verb != "Test notification" {
		t.Errorf("Verb = %q; want Test notification", v.Verb)
	}
	if v.Release != "smoke probe" {
		t.Errorf("Release = %q; want smoke probe", v.Release)
	}
	if v.Source != "Manual" {
		t.Errorf("Source should default to Manual; got %q", v.Source)
	}
}

func TestFrom_FailureCarriesErrorMsg(t *testing.T) {
	env := event.Envelope{
		Topic:       "deliver.failed",
		AggregateID: "55",
		Payload:     []byte(`{"event": {"err":"target FS read-only"}, "job": {"name":"X.S01E01-G"}}`),
	}
	v := From(env)
	if v.Outcome != OutcomeFail {
		t.Errorf("Outcome should be Fail")
	}
	if v.ErrorMsg != "target FS read-only" {
		t.Errorf("ErrorMsg = %q; want target FS read-only", v.ErrorMsg)
	}
}
