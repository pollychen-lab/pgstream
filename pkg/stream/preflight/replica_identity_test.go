// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/postgres"
)

func TestAssessReplicaIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		row      replicaIdentityRow
		wantHit  bool
		wantSubs []string // substrings that must appear in the finding
	}{
		{
			name: "FULL is always OK",
			row:  replicaIdentityRow{Schema: "public", Name: "x", Relreplident: "f"},
		},
		{
			name: "default + PK is OK",
			row:  replicaIdentityRow{Schema: "public", Name: "users", Relreplident: "d", HasPK: true},
		},
		{
			name:     "default without PK is a finding",
			row:      replicaIdentityRow{Schema: "public", Name: "audit_log", Relreplident: "d"},
			wantHit:  true,
			wantSubs: []string{"public.audit_log", "REPLICA IDENTITY=default", "no PRIMARY KEY"},
		},
		{
			name:     "nothing is a finding regardless of PK",
			row:      replicaIdentityRow{Schema: "public", Name: "events", Relreplident: "n", HasPK: true},
			wantHit:  true,
			wantSubs: []string{"public.events", "REPLICA IDENTITY=nothing"},
		},
		{
			name: "index with a valid index is OK",
			row:  replicaIdentityRow{Schema: "public", Name: "t", Relreplident: "i", ReplidentOK: true},
		},
		{
			name:     "index with an invalid index is a finding",
			row:      replicaIdentityRow{Schema: "public", Name: "t", Relreplident: "i"},
			wantHit:  true,
			wantSubs: []string{"public.t", "REPLICA IDENTITY=index"},
		},
		{
			name:     "unknown relreplident value is a finding",
			row:      replicaIdentityRow{Schema: "public", Name: "t", Relreplident: "z"},
			wantHit:  true,
			wantSubs: []string{"unknown REPLICA IDENTITY"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := assessReplicaIdentity(tc.row)
			if !tc.wantHit {
				require.Empty(t, got)
				return
			}
			for _, sub := range tc.wantSubs {
				require.Contains(t, got, sub)
			}
		})
	}
}

func TestInScope(t *testing.T) {
	t.Parallel()

	mustMap := func(t *testing.T, ss []string) postgres.SchemaTableMap {
		t.Helper()
		if len(ss) == 0 {
			return nil
		}
		m, err := postgres.NewSchemaTableMap(ss)
		require.NoError(t, err)
		return m
	}

	tests := []struct {
		name    string
		schema  string
		table   string
		include []string
		exclude []string
		want    bool
	}{
		{
			name: "no selection includes everything", schema: "public", table: "users", want: true,
		},
		{
			name: "include match", schema: "public", table: "users",
			include: []string{"public.users"}, want: true,
		},
		{
			name: "include miss", schema: "public", table: "orders",
			include: []string{"public.users"}, want: false,
		},
		{
			name: "exclude match", schema: "public", table: "audit_log",
			exclude: []string{"public.audit_log"}, want: false,
		},
		{
			name: "exclude miss", schema: "public", table: "users",
			exclude: []string{"public.audit_log"}, want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, inScope(tc.schema, tc.table, mustMap(t, tc.include), mustMap(t, tc.exclude)))
		})
	}
}
