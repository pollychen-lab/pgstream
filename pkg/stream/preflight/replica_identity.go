// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"fmt"

	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/stream"
)

// ReplicaIdentityCheck verifies that every in-scope table has a REPLICA
// IDENTITY sufficient for logical replication of UPDATE/DELETE WAL events.
// Anything insufficient means those events would silently be skipped at run
// time. The check inspects only the tables that pass the user's
// include/exclude filter (TableSelection) so unrelated tables don't pollute
// the report.
type ReplicaIdentityCheck struct {
	Source    postgres.AcquireFunc
	Selection stream.TableSelection
}

func (c *ReplicaIdentityCheck) Name() string { return "replica_identity" }

const replicaIdentityQuery = `
SELECT
  n.nspname,
  c.relname,
  c.relreplident::text,
  EXISTS (
    SELECT 1 FROM pg_index i
    WHERE i.indrelid = c.oid AND i.indisprimary
  ) AS has_pk,
  COALESCE(
    (
      SELECT i.indisvalid AND i.indisunique AND i.indpred IS NULL
             AND NOT EXISTS (
               SELECT 1 FROM unnest(i.indkey) k
               JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k
               WHERE NOT a.attnotnull
             )
      FROM pg_index i WHERE i.indrelid = c.oid AND i.indisreplident
    ),
    false
  ) AS replident_index_ok
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pgstream')
ORDER BY n.nspname, c.relname
`

func (c *ReplicaIdentityCheck) Run(ctx context.Context) ([]Finding, error) {
	conn, err := c.Source(ctx)
	if err != nil {
		return nil, fmt.Errorf("connecting to source: %w", err)
	}

	include, exclude, err := buildScopeMaps(c.Selection)
	if err != nil {
		return nil, fmt.Errorf("parsing table selection: %w", err)
	}

	rows, err := conn.Query(ctx, replicaIdentityQuery)
	if err != nil {
		return nil, fmt.Errorf("querying pg_class for replica identity: %w", err)
	}
	defer rows.Close()

	var findings []Finding
	for rows.Next() {
		var t replicaIdentityRow
		if err := rows.Scan(&t.Schema, &t.Name, &t.Relreplident, &t.HasPK, &t.ReplidentOK); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		if !inScope(t.Schema, t.Name, include, exclude) {
			continue
		}
		if msg := assessReplicaIdentity(t); msg != "" {
			findings = append(findings, Finding{Message: msg})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}
	return findings, nil
}

type replicaIdentityRow struct {
	Schema       string
	Name         string
	Relreplident string // 'd' default, 'n' nothing, 'f' full, 'i' index
	HasPK        bool
	ReplidentOK  bool // only meaningful when Relreplident == 'i'
}

// assessReplicaIdentity returns a remediation message if the table's REPLICA
// IDENTITY is insufficient for UPDATE/DELETE replication, or "" if it's OK.
// Kept pure for cheap unit testing.
func assessReplicaIdentity(t replicaIdentityRow) string {
	switch t.Relreplident {
	case "f":
		return ""
	case "d":
		if t.HasPK {
			return ""
		}
		return fmt.Sprintf("%s.%s: REPLICA IDENTITY=default but no PRIMARY KEY; UPDATE/DELETE WAL events will be skipped — add a PRIMARY KEY, set REPLICA IDENTITY FULL, or REPLICA IDENTITY USING INDEX <unique non-partial NOT-NULL index>", t.Schema, t.Name)
	case "n":
		return fmt.Sprintf("%s.%s: REPLICA IDENTITY=nothing; UPDATE/DELETE WAL events will be skipped — set REPLICA IDENTITY DEFAULT / FULL / USING INDEX", t.Schema, t.Name)
	case "i":
		if t.ReplidentOK {
			return ""
		}
		return fmt.Sprintf("%s.%s: REPLICA IDENTITY=index but the chosen index is invalid, non-unique, partial, or includes nullable columns — pick a different index or use REPLICA IDENTITY FULL", t.Schema, t.Name)
	default:
		return fmt.Sprintf("%s.%s: unknown REPLICA IDENTITY=%q on this Postgres version", t.Schema, t.Name, t.Relreplident)
	}
}

func buildScopeMaps(sel stream.TableSelection) (include, exclude postgres.SchemaTableMap, err error) {
	if len(sel.Include) > 0 {
		include, err = postgres.NewSchemaTableMap(sel.Include)
		if err != nil {
			return nil, nil, fmt.Errorf("include: %w", err)
		}
	}
	if len(sel.Exclude) > 0 {
		exclude, err = postgres.NewSchemaTableMap(sel.Exclude)
		if err != nil {
			return nil, nil, fmt.Errorf("exclude: %w", err)
		}
	}
	return include, exclude, nil
}

func inScope(schema, table string, include, exclude postgres.SchemaTableMap) bool {
	if exclude != nil && exclude.ContainsSchemaTable(schema, table) {
		return false
	}
	if include != nil {
		return include.ContainsSchemaTable(schema, table)
	}
	return true
}
