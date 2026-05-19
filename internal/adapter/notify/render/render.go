// Package render normalises bus envelopes into a structured View for
// chat-style notification adapters (Discord, Slack). Discord and Slack
// emit very different JSON shapes but want the same fields rendered —
// the release name, a friendly verb for what just happened, who sent
// the NZB (Sonarr/Radarr/etc), category, size, file count, and so on.
//
// The notify service hydrates each envelope with a Job snapshot under
// `payload.job` (see internal/app/notify/service.go enrichEnvelope).
// View.From decodes that snapshot plus the topic to produce a flat,
// ready-to-render record that adapters can pour into their own widgets.
package render

import (
	"encoding/json"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/event"
)

// Outcome categorises the topic semantically. Adapters use it to pick
// a colour, an emoji, or a side-bar tone without needing to know the
// full topic taxonomy.
type Outcome int

const (
	// OutcomeInfo is the neutral default — "added to queue", "verifying",
	// "test notification". Blue / grey in most adapters.
	OutcomeInfo Outcome = iota
	// OutcomeOK is success — verified, repaired, delivered, completed.
	// Green.
	OutcomeOK
	// OutcomeWarn is "user attention may be needed but the job didn't
	// fail outright" — currently just repair_needed. Amber.
	OutcomeWarn
	// OutcomeFail is terminal failure — verify.failed, deliver.failed,
	// the job ended in a failed state. Red.
	OutcomeFail
)

// View is the normalised projection of an event ready for rendering.
// Fields are intentionally string-typed so adapters can pour them
// straight into their widget structures.
type View struct {
	Topic       string    // raw bus topic (e.g. "deliver.complete") — adapters may want it for debug/footer
	AggregateID string    // raw aggregate id (e.g. "148") — keep for fallback if no job hydrated
	OccurredAt  time.Time // when the event fired
	Verb        string    // human-friendly "what happened" — "Added to queue", "Delivered", "Failed"
	Outcome     Outcome   // semantic outcome for colour/emoji selection
	Release     string    // raw release name from job.name (or payload.name / .filename / .release fallback)
	CleanTitle  string    // Release with dots → spaces and the trailing release group stripped
	Source      string    // friendly source — "Sonarr", "Radarr", "Manual", or the raw UA if unknown
	Category    string    // job category (e.g. "tv", "movies"); empty means *
	SizeHuman   string    // job.total_bytes formatted like "10.7 GB"; empty if unknown
	State       string    // job state at event time (e.g. "completed", "downloading", "failed")
	FileCount   int       // number of files in the job
	Quality     string    // parsed quality marker — "WEB-DL 2160p", "BluRay 1080p"; empty if not detected
	ErrorMsg   string     // error message from payload.err for .failed topics
}

// From projects an enriched envelope into a View. The envelope is the
// shape produced by app/notify.Service.enrichEnvelope — original event
// payload under "event", job snapshot under "job". For un-hydrated
// envelopes (e.g. notify.test) the function still returns a useful
// View by pulling whatever's available from the raw payload.
func From(env event.Envelope) View {
	v := View{
		Topic:       env.Topic,
		AggregateID: env.AggregateID,
		OccurredAt:  env.OccurredAt,
		Verb:        Verb(env.Topic),
		Outcome:     OutcomeFor(env.Topic),
	}

	if len(env.Payload) == 0 {
		return v
	}

	var raw map[string]any
	if err := json.Unmarshal(env.Payload, &raw); err != nil {
		return v
	}

	// Enriched envelopes nest the original under "event" and the job
	// snapshot under "job". Un-enriched envelopes have the raw payload
	// at the top level. Handle both shapes.
	original := raw
	if inner, ok := raw["event"].(map[string]any); ok {
		original = inner
	}
	job, _ := raw["job"].(map[string]any)

	// Release name: prefer job.name (always set when hydrated). Fall
	// back to common keys on the raw payload.
	if job != nil {
		if s, ok := job["name"].(string); ok {
			v.Release = s
		}
	}
	if v.Release == "" {
		for _, k := range []string{"name", "filename", "release"} {
			if s, ok := original[k].(string); ok && s != "" {
				v.Release = s
				break
			}
		}
	}
	v.CleanTitle = CleanReleaseName(v.Release)
	v.Quality = ParseQuality(v.Release)

	if job != nil {
		if s, ok := job["source"].(string); ok {
			v.Source = SourceName(s)
		}
		if s, ok := job["category"].(string); ok {
			v.Category = s
		}
		if s, ok := job["state"].(string); ok {
			v.State = s
		}
		if n, ok := numericField(job, "total_bytes"); ok && n > 0 {
			v.SizeHuman = BytesHuman(n)
		}
		if n, ok := numericField(job, "file_count"); ok {
			v.FileCount = int(n)
		}
	}
	if v.Source == "" {
		v.Source = "Manual"
	}

	// Failure topics: surface the error message so the user knows why.
	// Some emit "err", some "error", some "fail_message" — accept all.
	for _, k := range []string{"err", "error", "fail_message", "message"} {
		if s, ok := original[k].(string); ok && s != "" {
			v.ErrorMsg = s
			break
		}
	}

	return v
}

// numericField pulls a number out of a JSON-decoded map. JSON numbers
// land as float64; protobuf-style int64 also possible via certain
// encoders. We accept both.
func numericField(m map[string]any, key string) (int64, bool) {
	v, ok := m[key]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	default:
		return 0, false
	}
}

// Verb maps a bus topic to a human-readable label suitable for a
// notification title or subtitle. Unknown topics fall through verbatim
// so debug events stay visible rather than silently mis-rendering.
func Verb(topic string) string {
	switch topic {
	case "download.job.created":
		return "Added to queue"
	case "download.job.download_complete":
		return "Download complete"
	case "download.job.completed":
		return "Completed"
	case "download.job.failed", "download.job.download_failed":
		return "Failed"
	case "download.job.paused":
		return "Paused"
	case "download.job.resumed":
		return "Resumed"
	case "download.job.removed":
		return "Removed"
	case "verify.started":
		return "Verifying"
	case "verify.ok":
		return "Verified"
	case "verify.failed":
		return "Verify failed"
	case "verify.repair_needed":
		return "Repair needed"
	case "repair.started":
		return "Repairing"
	case "repair.ok":
		return "Repaired"
	case "repair.failed":
		return "Repair failed"
	case "extract.started":
		return "Extracting"
	case "extract.complete":
		return "Extracted"
	case "extract.failed":
		return "Extract failed"
	case "deliver.started":
		return "Delivering"
	case "deliver.complete":
		return "Delivered"
	case "deliver.failed":
		return "Delivery failed"
	case "notify.test":
		return "Test notification"
	default:
		return topic
	}
}

// OutcomeFor classifies a topic by suffix. The mapping uses naming
// conventions across bounded contexts (`.ok`, `.complete`, `.completed`
// → OK; `.failed` / `.download_failed` → Fail; `.repair_needed` →
// Warn; everything else → Info).
func OutcomeFor(topic string) Outcome {
	switch {
	case hasSuffix(topic, ".failed"), hasSuffix(topic, ".download_failed"):
		return OutcomeFail
	case hasSuffix(topic, ".repair_needed"):
		return OutcomeWarn
	case hasSuffix(topic, ".ok"), hasSuffix(topic, ".complete"), hasSuffix(topic, ".completed"):
		return OutcomeOK
	default:
		return OutcomeInfo
	}
}

// Color returns a hex RGB int suitable for Discord embed.color or
// Slack attachment.color. The palette is tuned to feel native in both
// dark and light themes.
func (o Outcome) Color() int {
	switch o {
	case OutcomeOK:
		return 0x2ECC71 // green
	case OutcomeFail:
		return 0xF85149 // red
	case OutcomeWarn:
		return 0xD29922 // amber
	default:
		return 0x4A90E2 // hoardarr blue
	}
}

func hasSuffix(s, suf string) bool {
	if len(s) < len(suf) {
		return false
	}
	return s[len(s)-len(suf):] == suf
}
