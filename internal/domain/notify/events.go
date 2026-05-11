package notify

import (
	"strconv"
	"time"
)

// TopicPrefix scopes all events of this bounded context.
const TopicPrefix = "notify."

func aggID(id SubscriptionID) string {
	return strconv.FormatInt(int64(id), 10)
}

type SubscriptionAdded struct {
	ID     SubscriptionID `json:"id"`
	Name   string         `json:"name"`
	Kind   string         `json:"kind"`
	URL    string         `json:"url"`
	Topics []string       `json:"topics"`
	At     time.Time      `json:"at"`
}

func (e SubscriptionAdded) Topic() string         { return TopicPrefix + "subscription.added" }
func (e SubscriptionAdded) AggregateID() string   { return aggID(e.ID) }
func (e SubscriptionAdded) OccurredAt() time.Time { return e.At }

type SubscriptionEnabled struct {
	ID SubscriptionID `json:"id"`
	At time.Time      `json:"at"`
}

func (e SubscriptionEnabled) Topic() string         { return TopicPrefix + "subscription.enabled" }
func (e SubscriptionEnabled) AggregateID() string   { return aggID(e.ID) }
func (e SubscriptionEnabled) OccurredAt() time.Time { return e.At }

type SubscriptionDisabled struct {
	ID SubscriptionID `json:"id"`
	At time.Time      `json:"at"`
}

func (e SubscriptionDisabled) Topic() string         { return TopicPrefix + "subscription.disabled" }
func (e SubscriptionDisabled) AggregateID() string   { return aggID(e.ID) }
func (e SubscriptionDisabled) OccurredAt() time.Time { return e.At }

type SubscriptionRemoved struct {
	ID   SubscriptionID `json:"id"`
	Name string         `json:"name"`
	At   time.Time      `json:"at"`
}

func (e SubscriptionRemoved) Topic() string         { return TopicPrefix + "subscription.removed" }
func (e SubscriptionRemoved) AggregateID() string   { return aggID(e.ID) }
func (e SubscriptionRemoved) OccurredAt() time.Time { return e.At }
