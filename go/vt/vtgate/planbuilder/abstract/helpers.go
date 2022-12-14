package abstract

import (
	"fmt"
	"sort"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func QualifiedIdentifier(ks *vindexes.Keyspace, i sqlparser.IdentifierCS) string {
	return QualifiedString(ks, i.String())
}

func QualifiedString(ks *vindexes.Keyspace, s string) string {
	return fmt.Sprintf("%s.%s", ks.Name, s)
}

func QualifiedStrings(ks *vindexes.Keyspace, ss []string) []string {
	add, collect := CollectSortedUniqueStrings()
	for _, s := range ss {
		add(QualifiedString(ks, s))
	}
	return collect()
}

func QualifiedTableName(ks *vindexes.Keyspace, t sqlparser.TableName) string {
	return QualifiedIdentifier(ks, t.Name)
}

func QualifiedTableNames(ks *vindexes.Keyspace, ts []sqlparser.TableName) []string {
	add, collect := CollectSortedUniqueStrings()
	for _, t := range ts {
		add(QualifiedTableName(ks, t))
	}
	return collect()
}

func QualifiedTables(ks *vindexes.Keyspace, vts []*vindexes.Table) []string {
	add, collect := CollectSortedUniqueStrings()
	for _, vt := range vts {
		add(QualifiedIdentifier(ks, vt.Name))
	}
	return collect()
}

func SingleQualifiedIdentifier(ks *vindexes.Keyspace, i sqlparser.IdentifierCS) []string {
	return SingleQualifiedString(ks, i.String())
}

func SingleQualifiedString(ks *vindexes.Keyspace, s string) []string {
	return []string{QualifiedString(ks, s)}
}

func SingleQualifiedTableName(ks *vindexes.Keyspace, t sqlparser.TableName) []string {
	return SingleQualifiedIdentifier(ks, t.Name)
}

func CollectSortedUniqueStrings() (add func(string), collect func() []string) {
	uniq := make(map[string]any)
	add = func(v string) {
		uniq[v] = nil
	}
	collect = func() []string {
		sorted := make([]string, 0, len(uniq))
		for v := range uniq {
			sorted = append(sorted, v)
		}
		sort.Strings(sorted)
		return sorted
	}

	return add, collect
}
