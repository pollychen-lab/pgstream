// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/wal/listener/snapshot/adapter"
	snapshotbuilder "github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
	"github.com/xataio/pgstream/pkg/wal/processor/filter"
)

func TestConfig_ReplicationTableSelection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  *Config
		want TableSelection
	}{
		{
			name: "no filter configured returns unfiltered selection",
			cfg:  &Config{},
			want: TableSelection{},
		},
		{
			name: "empty filter returns unfiltered selection",
			cfg: &Config{
				Processor: ProcessorConfig{Filter: &filter.Config{}},
			},
			want: TableSelection{},
		},
		{
			name: "include list propagates",
			cfg: &Config{
				Processor: ProcessorConfig{
					Filter: &filter.Config{IncludeTables: []string{"public.users", "public.orders"}},
				},
			},
			want: TableSelection{Include: []string{"public.users", "public.orders"}},
		},
		{
			name: "exclude list propagates",
			cfg: &Config{
				Processor: ProcessorConfig{
					Filter: &filter.Config{ExcludeTables: []string{"public.audit_log"}},
				},
			},
			want: TableSelection{Exclude: []string{"public.audit_log"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, tc.cfg.ReplicationTableSelection())
		})
	}
}

func TestTableSelection_IsUnfiltered(t *testing.T) {
	t.Parallel()

	require.True(t, TableSelection{}.IsUnfiltered())
	require.False(t, TableSelection{Include: []string{"public.x"}}.IsUnfiltered())
	require.False(t, TableSelection{Exclude: []string{"public.x"}}.IsUnfiltered())
}

func TestTableSelection_IsTableInScope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		selection TableSelection
		schema    string
		table     string
		want      bool
	}{
		{name: "unfiltered selection includes everything", schema: "public", table: "users", want: true},
		{
			name:      "include match",
			selection: TableSelection{Include: []string{"public.users"}},
			schema:    "public", table: "users", want: true,
		},
		{
			name:      "include miss",
			selection: TableSelection{Include: []string{"public.users"}},
			schema:    "public", table: "orders", want: false,
		},
		{
			name:      "exclude match",
			selection: TableSelection{Exclude: []string{"public.audit_log"}},
			schema:    "public", table: "audit_log", want: false,
		},
		{
			name:      "exclude miss",
			selection: TableSelection{Exclude: []string{"public.audit_log"}},
			schema:    "public", table: "users", want: true,
		},
		{
			name:      "exclude wins over include when both match",
			selection: TableSelection{Include: []string{"public.users"}, Exclude: []string{"public.users"}},
			schema:    "public", table: "users", want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, tc.selection.IsTableInScope(tc.schema, tc.table))
		})
	}
}

func TestTableSelection_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		selection TableSelection
		wantErr   bool
	}{
		{name: "empty selection is valid"},
		{
			name:      "valid include/exclude",
			selection: TableSelection{Include: []string{"public.users", "billing.invoices"}, Exclude: []string{"public.audit_log"}},
		},
		{
			name:      "malformed include surfaces error",
			selection: TableSelection{Include: []string{"too.many.parts"}},
			wantErr:   true,
		},
		{
			name:      "malformed exclude surfaces error",
			selection: TableSelection{Exclude: []string{"too.many.parts"}},
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.selection.Validate()
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestConfig_SnapshotTableSelection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  *Config
		want TableSelection
	}{
		{
			name: "no snapshot configured returns unfiltered selection",
			cfg:  &Config{},
		},
		{
			name: "snapshot adapter include/exclude propagates",
			cfg: &Config{
				Listener: ListenerConfig{
					Postgres: &PostgresListenerConfig{
						Snapshot: &snapshotbuilder.SnapshotListenerConfig{
							Adapter: adapter.SnapshotConfig{
								Tables:         []string{"public.users"},
								ExcludedTables: []string{"public.audit_log"},
							},
						},
					},
				},
			},
			want: TableSelection{
				Include: []string{"public.users"},
				Exclude: []string{"public.audit_log"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, tc.cfg.SnapshotTableSelection())
		})
	}
}

func TestConfig_AccessTableSelection(t *testing.T) {
	t.Parallel()

	snapInclude := func(tables ...string) *Config {
		return &Config{
			Listener: ListenerConfig{
				Postgres: &PostgresListenerConfig{
					Snapshot: &snapshotbuilder.SnapshotListenerConfig{
						Adapter: adapter.SnapshotConfig{Tables: tables},
					},
				},
			},
		}
	}
	snapExclude := func(tables ...string) *Config {
		return &Config{
			Listener: ListenerConfig{
				Postgres: &PostgresListenerConfig{
					Snapshot: &snapshotbuilder.SnapshotListenerConfig{
						Adapter: adapter.SnapshotConfig{ExcludedTables: tables},
					},
				},
			},
		}
	}
	withRepInclude := func(c *Config, tables ...string) *Config {
		c.Processor.Filter = &filter.Config{IncludeTables: tables}
		return c
	}
	withRepExclude := func(c *Config, tables ...string) *Config {
		c.Processor.Filter = &filter.Config{ExcludeTables: tables}
		return c
	}

	tests := []struct {
		name string
		cfg  *Config
		want TableSelection
	}{
		{
			name: "both unfiltered returns unfiltered",
			cfg:  &Config{},
		},
		{
			name: "only snapshot Include with no filter returns unfiltered (replication covers everything)",
			cfg:  snapInclude("public.users"),
		},
		{
			name: "only replication Include with no snapshot returns unfiltered (snapshot covers everything)",
			cfg:  withRepInclude(&Config{}, "public.users"),
		},
		{
			name: "both Include — access Include is the union",
			cfg:  withRepInclude(snapInclude("public.orders", "public.users"), "public.users", "public.events"),
			want: TableSelection{Include: []string{"public.orders", "public.users", "public.events"}},
		},
		{
			name: "both Exclude — access Exclude is the intersection",
			cfg:  withRepExclude(snapExclude("public.audit_log", "public.events"), "public.audit_log", "public.tmp"),
			want: TableSelection{Exclude: []string{"public.audit_log"}},
		},
		{
			name: "mixed Include + Exclude falls back to unfiltered",
			cfg:  withRepExclude(snapInclude("public.orders"), "public.audit_log"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.cfg.AccessTableSelection()
			require.ElementsMatch(t, tc.want.Include, got.Include, "Include lists differ")
			require.ElementsMatch(t, tc.want.Exclude, got.Exclude, "Exclude lists differ")
		})
	}
}
