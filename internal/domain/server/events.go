package server

import (
	"strconv"
	"time"
)

// Topic prefix for server-context events.
const TopicPrefix = "server.usenet."

// ServerAdded is emitted after a new UsenetServer is persisted for the
// first time. The ID is the database-assigned identifier.
type ServerAdded struct {
	ID   ServerID  `json:"id"`
	Name string    `json:"name"`
	At   time.Time `json:"at"`
}

func (e ServerAdded) Topic() string         { return TopicPrefix + "added" }
func (e ServerAdded) AggregateID() string   { return strconv.FormatInt(int64(e.ID), 10) }
func (e ServerAdded) OccurredAt() time.Time { return e.At }

// ServerUpdated is emitted on any subset of host/port/tls/credentials/
// max_conns/priority change. The payload intentionally does not enumerate
// changed fields — subscribers that need that fetch the current state.
type ServerUpdated struct {
	ID ServerID  `json:"id"`
	At time.Time `json:"at"`
}

func (e ServerUpdated) Topic() string         { return TopicPrefix + "updated" }
func (e ServerUpdated) AggregateID() string   { return strconv.FormatInt(int64(e.ID), 10) }
func (e ServerUpdated) OccurredAt() time.Time { return e.At }

// ServerEnabled flips the soft-enable flag from off → on.
type ServerEnabled struct {
	ID ServerID  `json:"id"`
	At time.Time `json:"at"`
}

func (e ServerEnabled) Topic() string         { return TopicPrefix + "enabled" }
func (e ServerEnabled) AggregateID() string   { return strconv.FormatInt(int64(e.ID), 10) }
func (e ServerEnabled) OccurredAt() time.Time { return e.At }

// ServerDisabled flips the soft-enable flag from on → off.
type ServerDisabled struct {
	ID ServerID  `json:"id"`
	At time.Time `json:"at"`
}

func (e ServerDisabled) Topic() string         { return TopicPrefix + "disabled" }
func (e ServerDisabled) AggregateID() string   { return strconv.FormatInt(int64(e.ID), 10) }
func (e ServerDisabled) OccurredAt() time.Time { return e.At }

// ServerRemoved is emitted by the application service after a
// successful delete. The aggregate is gone after this; subscribers
// should not expect to load it.
type ServerRemoved struct {
	ID ServerID  `json:"id"`
	At time.Time `json:"at"`
}

func (e ServerRemoved) Topic() string         { return TopicPrefix + "removed" }
func (e ServerRemoved) AggregateID() string   { return strconv.FormatInt(int64(e.ID), 10) }
func (e ServerRemoved) OccurredAt() time.Time { return e.At }
