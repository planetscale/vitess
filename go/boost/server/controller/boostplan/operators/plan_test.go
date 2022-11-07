package operators

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestComputeColumnUsage(t *testing.T) {
	stmt, err := sqlparser.Parse("SELECT id, name FROM user")
	require.NoError(t, err)
	semTable, err := semantics.Analyze(stmt, "main", &semantics.FakeSI{
		Tables: map[string]*vindexes.Table{
			"`user`": {
				Name: sqlparser.NewIdentifierCS("user"),
				Keyspace: &vindexes.Keyspace{
					Name:    "main",
					Sharded: true,
				},
				Columns: []vindexes.Column{
					{Name: sqlparser.NewIdentifierCI("Id")},
					{Name: sqlparser.NewIdentifierCI("Name")},
					{Name: sqlparser.NewIdentifierCI("NotUsed")},
				},
			},
		},
	})
	require.NoError(t, err)
	usage := computeColumnUsage(semTable)
	tableName := sqlparser.TableName{
		Name:      sqlparser.NewIdentifierCS("user"),
		Qualifier: sqlparser.NewIdentifierCS("main"),
	}
	require.Len(t, usage.TableUsage, 1)
	assert.Equal(t, []string{"Id", "Name"}, usage.TableUsage[tableName])
}

func TestComputeColumnUsageWithExpandedColumns(t *testing.T) {
	stmt, err := sqlparser.Parse("SELECT * FROM user")
	require.NoError(t, err)
	columns := []*sqlparser.ColName{
		{Name: sqlparser.NewIdentifierCI("Id")},
		{Name: sqlparser.NewIdentifierCI("Name")},
		{Name: sqlparser.NewIdentifierCI("Used")},
	}
	semTable, err := semantics.Analyze(stmt, "main", &semantics.FakeSI{
		Tables: map[string]*vindexes.Table{
			"`user`": {
				Name: sqlparser.NewIdentifierCS("user"),
				Keyspace: &vindexes.Keyspace{
					Name:    "main",
					Sharded: true,
				},
				Columns: []vindexes.Column{
					{Name: columns[0].Name},
					{Name: columns[1].Name},
					{Name: columns[2].Name},
				},
				ColumnListAuthoritative: true,
			},
		},
	})
	require.NoError(t, err)
	usage := computeColumnUsage(semTable)
	tableName := sqlparser.TableName{
		Name:      sqlparser.NewIdentifierCS("user"),
		Qualifier: sqlparser.NewIdentifierCS("main"),
	}
	require.Len(t, usage.TableUsage, 1)
	assert.Equal(t, []string{"Id", "Name", "Used"}, usage.TableUsage[tableName])
	require.Len(t, usage.ExpandedColumns, 1)
	assert.Equal(t, columns, usage.ExpandedColumns[tableName])
}

func TestComputeColumnUsageWithMultipleTables(t *testing.T) {
	stmt, err := sqlparser.Parse("SELECT user.*, demo.id FROM demo, user")
	require.NoError(t, err)
	columnsUser := []*sqlparser.ColName{
		{Name: sqlparser.NewIdentifierCI("Id"), Qualifier: sqlparser.TableName{Name: sqlparser.NewIdentifierCS("user")}},
		{Name: sqlparser.NewIdentifierCI("Name"), Qualifier: sqlparser.TableName{Name: sqlparser.NewIdentifierCS("user")}},
		{Name: sqlparser.NewIdentifierCI("Used"), Qualifier: sqlparser.TableName{Name: sqlparser.NewIdentifierCS("user")}},
	}
	columnsDemo := []*sqlparser.ColName{
		{Name: sqlparser.NewIdentifierCI("Id"), Qualifier: sqlparser.TableName{Name: sqlparser.NewIdentifierCS("demo")}},
	}
	semTable, err := semantics.Analyze(stmt, "main", &semantics.FakeSI{
		Tables: map[string]*vindexes.Table{
			"`user`": {
				Name: sqlparser.NewIdentifierCS("user"),
				Keyspace: &vindexes.Keyspace{
					Name:    "main",
					Sharded: true,
				},
				Columns: []vindexes.Column{
					{Name: columnsUser[0].Name},
					{Name: columnsUser[1].Name},
					{Name: columnsUser[2].Name},
				},
				ColumnListAuthoritative: true,
			},
			"demo": {
				Name: sqlparser.NewIdentifierCS("demo"),
				Keyspace: &vindexes.Keyspace{
					Name:    "main",
					Sharded: true,
				},
				Columns: []vindexes.Column{
					{Name: columnsDemo[0].Name},
				},
			},
		},
	})
	require.NoError(t, err)
	usage := computeColumnUsage(semTable)

	require.Len(t, usage.TableUsage, 2)
	require.Len(t, usage.ExpandedColumns, 1)

	userTableName := sqlparser.TableName{
		Name:      sqlparser.NewIdentifierCS("user"),
		Qualifier: sqlparser.NewIdentifierCS("main"),
	}
	assert.Equal(t, []string{"Id", "Name", "Used"}, usage.TableUsage[userTableName])
	assert.Equal(t, columnsUser, usage.ExpandedColumns[userTableName])

	demoTableName := sqlparser.TableName{
		Name:      sqlparser.NewIdentifierCS("demo"),
		Qualifier: sqlparser.NewIdentifierCS("main"),
	}
	assert.Equal(t, []string{"Id"}, usage.TableUsage[demoTableName])
}
