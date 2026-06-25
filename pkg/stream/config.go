// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xataio/pgstream/pkg/backoff"
	"github.com/xataio/pgstream/pkg/kafka"
	kafkacheckpoint "github.com/xataio/pgstream/pkg/wal/checkpointer/kafka"
	snapshotbuilder "github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
	"github.com/xataio/pgstream/pkg/wal/processor/filter"
	"github.com/xataio/pgstream/pkg/wal/processor/injector"
	kafkaprocessor "github.com/xataio/pgstream/pkg/wal/processor/kafka"
	"github.com/xataio/pgstream/pkg/wal/processor/postgres"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
	"github.com/xataio/pgstream/pkg/wal/processor/search/store"
	"github.com/xataio/pgstream/pkg/wal/processor/transformer"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/notifier"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/server"
	pgreplication "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

type Config struct {
	Listener  ListenerConfig
	Processor ProcessorConfig
}

type ListenerConfig struct {
	Postgres *PostgresListenerConfig
	Kafka    *KafkaListenerConfig
}

type PostgresListenerConfig struct {
	URL         string
	Replication pgreplication.Config
	RetryPolicy backoff.Config
	Snapshot    *snapshotbuilder.SnapshotListenerConfig
}

type KafkaListenerConfig struct {
	Reader       kafka.ReaderConfig
	Checkpointer kafkacheckpoint.Config
}

type SanitizeConfig struct {
	StripNullCharBytes bool
}

type ProcessorConfig struct {
	Kafka       *KafkaProcessorConfig
	Search      *SearchProcessorConfig
	Webhook     *WebhookProcessorConfig
	Postgres    *PostgresProcessorConfig
	Stdout      *StdoutProcessorConfig
	Injector    *injector.Config
	Transformer *transformer.Config
	Filter      *filter.Config
	Sanitize    *SanitizeConfig
}

type StdoutProcessorConfig struct{}

type KafkaProcessorConfig struct {
	Writer *kafkaprocessor.Config
}

type SearchProcessorConfig struct {
	Indexer search.IndexerConfig
	Store   store.Config
	Retrier search.StoreRetryConfig
}

type WebhookProcessorConfig struct {
	Notifier           notifier.Config
	SubscriptionServer server.Config
	SubscriptionStore  WebhookSubscriptionStoreConfig
}

type PostgresProcessorConfig struct {
	BatchWriter postgres.Config
}

type WebhookSubscriptionStoreConfig struct {
	URL                  string
	CacheEnabled         bool
	CacheRefreshInterval time.Duration
}

func (c *Config) IsValid() error {
	if err := c.Listener.IsValid(); err != nil {
		return err
	}

	return c.Processor.IsValid()
}

func (c *ListenerConfig) IsValid() error {
	listenerCount := 0
	if c.Kafka != nil {
		listenerCount++
	}
	if c.Postgres != nil {
		listenerCount++
	}

	switch listenerCount {
	case 0:
		return errors.New("need at least one listener configured")
	case 1:
		// Only one listener is configured, do nothing
		return nil
	default:
		// More than one listener is configured, return an error
		return fmt.Errorf("only one listener can be configured at a time, found %d", listenerCount)
	}
}

func (c *ProcessorConfig) IsValid() error {
	processorCount := 0
	if c.Kafka != nil {
		processorCount++
	}
	if c.Postgres != nil {
		processorCount++
	}
	if c.Search != nil {
		processorCount++
	}
	if c.Webhook != nil {
		processorCount++
	}
	if c.Stdout != nil {
		processorCount++
	}

	switch processorCount {
	case 0:
		return errors.New("need at least one processor configured")
	case 1:
		// Only one processor is configured, do nothing
		return nil
	default:
		// More than one processor is configured, return an error
		return fmt.Errorf("only one processor can be configured at a time, found %d", processorCount)
	}
}

func (c *Config) SourcePostgresURL() string {
	if c.Listener.Postgres != nil {
		return c.Listener.Postgres.URL
	}
	return ""
}

func (c *Config) PostgresReplicationSlot() string {
	if c.Listener.Postgres != nil {
		return c.Listener.Postgres.Replication.ReplicationSlotName
	}
	return ""
}

func (c *Config) isInjectorEnabled() bool {
	return c.Processor.Injector != nil && c.Processor.Injector.URL != ""
}

// restoreConflictTargetsBeforeData reports whether the schema snapshot must
// restore primary keys, unique constraints and unique indexes before the data
// snapshot runs. This is required when the postgres batch writer emits
// INSERT ... ON CONFLICT DO UPDATE, since the target table needs a matching
// conflict target at insert time.
func (c *Config) restoreConflictTargetsBeforeData() bool {
	if c.Processor.Postgres == nil {
		return false
	}
	bw := c.Processor.Postgres.BatchWriter
	return !bw.BulkIngestEnabled && strings.EqualFold(bw.OnConflictAction, "update")
}

func (c *Config) GetInitConfig(opts ...InitOption) *InitConfig {
	initConfig := &InitConfig{
		PostgresURL:               c.SourcePostgresURL(),
		ReplicationSlotName:       c.PostgresReplicationSlot(),
		InjectorMigrationsEnabled: c.isInjectorEnabled(),
	}

	for _, opt := range opts {
		opt(initConfig)
	}

	return initConfig
}

func (c *Config) RequiredTables() []string {
	requiredTables := []string{}
	if c.Listener.Postgres != nil {
		if c.Listener.Postgres.Snapshot != nil {
			requiredTables = append(requiredTables, c.Listener.Postgres.Snapshot.Adapter.Tables...)
		}
	}
	// Replication-side filtering is exposed via ReplicationTableSelection,
	// since include/exclude isn't a flat "required" list.
	return requiredTables
}

// TableSelection captures the include/exclude filter pgstream applies to the
// WAL stream. Empty Include means "every user table is in scope"; Exclude
// names tables to skip. Include and Exclude are mutually exclusive (validated
// at filter construction).
type TableSelection struct {
	Include []string
	Exclude []string
}

// IsUnfiltered reports whether the selection imposes no constraint, i.e. every
// user table the listener sees is in scope.
func (s TableSelection) IsUnfiltered() bool {
	return len(s.Include) == 0 && len(s.Exclude) == 0
}

// SnapshotTableSelection returns the table filter that will be applied to the
// snapshot path, sourced from the snapshot adapter's include/exclude lists.
// If no snapshot is configured every table the snapshot worker sees is in
// scope.
func (c *Config) SnapshotTableSelection() TableSelection {
	if c.Listener.Postgres == nil || c.Listener.Postgres.Snapshot == nil {
		return TableSelection{}
	}
	return TableSelection{
		Include: c.Listener.Postgres.Snapshot.Adapter.Tables,
		Exclude: c.Listener.Postgres.Snapshot.Adapter.ExcludedTables,
	}
}

// ReplicationTableSelection returns the table filter that will be applied to
// WAL events at replication time. Callers (e.g. preflight checks) can use it
// to know which tables to inspect on the source. The selection mirrors the
// filter processor's configuration; if no filter is configured every table the
// listener sees is in scope.
func (c *Config) ReplicationTableSelection() TableSelection {
	if c.Processor.Filter == nil {
		return TableSelection{}
	}
	return TableSelection{
		Include: c.Processor.Filter.IncludeTables,
		Exclude: c.Processor.Filter.ExcludeTables,
	}
}

// AccessTableSelection returns the table set the source role needs SELECT on,
// computed as the union of SnapshotTableSelection and ReplicationTableSelection
// — a table is in access scope if either path will read from it.
//
// Source-config rules: each underlying selection has Include XOR Exclude
// (mutually exclusive at config time), so the combinations are:
//   - either side unfiltered → access is unfiltered
//   - both Include-only       → access Include = union of the two lists
//   - both Exclude-only       → access Exclude = intersection (a table is only
//     excluded when both paths exclude it)
//   - mixed (one Include, one Exclude) → fall back to unfiltered. The exact
//     union can't be represented with one Include-XOR-Exclude TableSelection
//     without introducing a richer predicate; over-permissive is the safe
//     default since it can only produce extra (not missing) findings.
func (c *Config) AccessTableSelection() TableSelection {
	snap := c.SnapshotTableSelection()
	rep := c.ReplicationTableSelection()

	if snap.IsUnfiltered() || rep.IsUnfiltered() {
		return TableSelection{}
	}

	switch {
	case len(snap.Include) > 0 && len(rep.Include) > 0:
		return TableSelection{Include: dedupUnion(snap.Include, rep.Include)}
	case len(snap.Exclude) > 0 && len(rep.Exclude) > 0:
		return TableSelection{Exclude: intersection(snap.Exclude, rep.Exclude)}
	default:
		return TableSelection{}
	}
}

func dedupUnion(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	for _, s := range a {
		seen[s] = struct{}{}
	}
	for _, s := range b {
		seen[s] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	return out
}

func intersection(a, b []string) []string {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	aSet := make(map[string]struct{}, len(a))
	for _, s := range a {
		aSet[s] = struct{}{}
	}
	var out []string
	for _, s := range b {
		if _, ok := aSet[s]; ok {
			out = append(out, s)
		}
	}
	return out
}
