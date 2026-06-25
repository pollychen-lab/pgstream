// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"fmt"

	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/stream"
)

// SourceTableSelectPrivilegesCheck verifies that the source Postgres role can
// read every in-scope table. Inspects only the tables that pass the user's
// include/exclude filter (TableSelection) so unrelated tables don't pollute
// the report.
type SourceTableSelectPrivilegesCheck struct {
	Source    postgres.AcquireFunc
	Selection stream.TableSelection
}

func (c *SourceTableSelectPrivilegesCheck) Name() string {
	return "source_table_select_privileges"
}

const sourceTableSelectPrivilegesQuery = `
SELECT
  current_user,
  n.nspname,
  c.relname,
  has_table_privilege(c.oid, 'SELECT') AS has_select
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pgstream')
  AND n.nspname NOT LIKE 'pg_toast%'
ORDER BY n.nspname, c.relname
`

func (c *SourceTableSelectPrivilegesCheck) Run(ctx context.Context) ([]Finding, error) {
	include, exclude, err := buildScopeMaps(c.Selection)
	if err != nil {
		return nil, fmt.Errorf("parsing table selection: %w", err)
	}

	conn, err := c.Source(ctx)
	if err != nil {
		return nil, fmt.Errorf("connecting to source: %w", err)
	}

	rows, err := conn.Query(ctx, sourceTableSelectPrivilegesQuery)
	if err != nil {
		return nil, fmt.Errorf("querying source table privileges: %w", err)
	}
	defer rows.Close()

	var findings []Finding
	for rows.Next() {
		var row sourceTableSelectPrivilegeRow
		if err := rows.Scan(&row.Role, &row.Schema, &row.Table, &row.HasSelect); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		if !inScope(row.Schema, row.Table, include, exclude) {
			continue
		}
		if !row.HasSelect {
			findings = append(findings, Finding{Message: sourceTableSelectPrivilegeMessage(row)})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}
	return findings, nil
}

type sourceTableSelectPrivilegeRow struct {
	Role      string
	Schema    string
	Table     string
	HasSelect bool
}

func sourceTableSelectPrivilegeMessage(row sourceTableSelectPrivilegeRow) string {
	table := fmt.Sprintf("%s.%s", row.Schema, row.Table)
	return fmt.Sprintf("source role %q lacks SELECT on %s; run GRANT SELECT ON TABLE %s TO %s", row.Role, table, table, row.Role)
}
