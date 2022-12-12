package testrecipe

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"testing"

	"vitess.io/vitess/go/boost/server/controller/boostplan"
	"vitess.io/vitess/go/vt/proto/vtboost"
	"vitess.io/vitess/go/vt/sqlparser"
)

type Recipe struct {
	DDL      map[string][]string
	Queries  []*vtboost.CachedQuery
	External *boostplan.DDLSchema
}

func (recipe *Recipe) ToProto() *vtboost.Recipe {
	return &vtboost.Recipe{Queries: recipe.Queries}
}

func (recipe *Recipe) SchemaInformation() *boostplan.SchemaInformation {
	return &boostplan.SchemaInformation{
		Schema: recipe.External,
	}
}

func (recipe *Recipe) ToStringDDL(keyspace string) string {
	return strings.Join(recipe.DDL[keyspace], ";\n")
}

func (recipe *Recipe) Update(t testing.TB) {
	recipe.updateExternalSchema(t)
}

func (recipe *Recipe) updateExternalSchema(t testing.TB) {
	recipe.External = &boostplan.DDLSchema{
		Specs: map[string]map[string]*sqlparser.TableSpec{},
	}

	for keyspace, ddls := range recipe.DDL {
		specs := make(map[string]*sqlparser.TableSpec)
		for _, ddl := range ddls {
			t.Logf("[schema] %s", ddl)

			stmt, err := sqlparser.ParseStrictDDL(ddl)
			if err != nil {
				t.Fatalf("failed to parse DDL statement: %v", err)
			}
			if create, ok := stmt.(*sqlparser.CreateTable); ok {
				specs[create.Table.Name.String()] = create.TableSpec
			}
		}
		recipe.External.Specs[keyspace] = specs
	}
}

const DefaultKeyspace = "source"

func Load(t testing.TB, name string) *Recipe {
	return NewRecipeFromSQL(t, DefaultKeyspace, Schema(t, name))
}

func LoadSQL(t testing.TB, recipesql string) *Recipe {
	return NewRecipeFromSQL(t, DefaultKeyspace, recipesql)
}

func NewRecipeFromSQL(t testing.TB, keyspace, recipesql string) *Recipe {
	pieces, err := sqlparser.SplitStatementToPieces(recipesql)
	if err != nil {
		t.Fatalf("failed to parse recipe: %v", err)
	}

	var queries []*vtboost.CachedQuery
	var ddls []string
	for _, piece := range pieces {
		stmt, _, err := sqlparser.Parse2(piece)
		if err != nil {
			t.Fatalf("failed to parse query %d in recipe: %v\nquery: %s", len(queries), err, piece)
		}

		switch expr := stmt.(type) {
		case sqlparser.SelectStatement:
			query := &vtboost.CachedQuery{
				PublicId: fmt.Sprintf("q%d", len(queries)),
				Sql:      piece,
				Keyspace: keyspace,
			}

			dir := expr.GetParsedComments().Directives()
			if name, ok := dir.GetString("VIEW", ""); ok {
				query.PublicId = name
			}
			queries = append(queries, query)

		case *sqlparser.CreateTable:
			if !expr.IsFullyParsed() {
				t.Fatalf("statement %q is not fully parsed", piece)
			}
			ddls = append(ddls, piece)

		default:
			t.Fatalf("unsupported statement: %q", piece)
		}
	}

	return &Recipe{
		DDL: map[string][]string{
			keyspace: ddls,
		},
		Queries: queries,
	}
}

func Schema(t testing.TB, name string) string {
	path := fmt.Sprintf("testdata/schemas/%s.sql", name)

	var file *os.File
	for depth := 0; depth < 10; depth++ {
		var err error
		file, err = os.Open(strings.Repeat("../", depth) + path)
		if err == nil {
			break
		}
	}
	if file == nil {
		t.Fatalf("failed to find schema %q", name)
	}
	defer file.Close()

	var clean strings.Builder
	var scanner = bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "--") || strings.HasPrefix(line, "#") {
			continue
		}

		clean.WriteString(line)
		clean.WriteByte('\n')
	}
	return clean.String()
}
