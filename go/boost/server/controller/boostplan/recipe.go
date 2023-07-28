package boostplan

import (
	"errors"
	"fmt"
	"strings"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/hack"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
	"vitess.io/vitess/go/vt/sqlparser"
)

// QueryHash is a 64 bit hash that uniquely represents a query
type QueryHash uint64

type Recipe struct {
	expressions       map[QueryHash]*CachedQuery
	queriesByPublicID map[string]QueryHash
	sorted            []QueryHash
}

func (r *Recipe) AddQuery(query *vtboostpb.CachedQuery) error {
	stmt, _, err := sqlparser.Parse2(query.Sql)
	if err != nil {
		return &SyntaxError{Err: err, Query: query.Sql}
	}

	switch stmt.(type) {
	case sqlparser.SelectStatement:
	default:
		return &UnsupportedQueryTypeError{Query: stmt}
	}

	if query.PublicId == "" {
		return errors.New("missing query public id")
	}

	var newQuery = &CachedQuery{
		CachedQuery: query,
		Statement:   stmt,
	}

	qh := newQuery.hash()
	if _, ok := r.expressions[qh]; ok {
		return nil
	}
	if existingqid, ok := r.queriesByPublicID[newQuery.PublicId]; ok {
		if existingqid != qh {
			return fmt.Errorf("a query with public id %q already exists", newQuery.PublicId)
		}
	}
	r.queriesByPublicID[newQuery.PublicId] = qh

	r.expressions[qh] = newQuery
	r.sorted = append(r.sorted, qh)
	return nil
}

func (r *Recipe) Reset() {
	r.expressions = make(map[QueryHash]*CachedQuery)
	r.queriesByPublicID = make(map[string]QueryHash)
	r.sorted = nil
}

func (r *Recipe) ComputeDelta(other *Recipe) (added []QueryHash, removed []QueryHash, unchanged []QueryHash) {
	for _, qid := range r.sorted {
		if _, contains := other.expressions[qid]; !contains {
			added = append(added, qid)
		} else {
			unchanged = append(unchanged, qid)
		}
	}
	for _, qid := range other.sorted {
		if _, contains := r.expressions[qid]; !contains {
			removed = append(removed, qid)
		}
	}
	return
}

func (r *Recipe) GetQuery(qid QueryHash) *CachedQuery {
	return r.expressions[qid]
}

func (r *Recipe) GetAllPublicViews() []*CachedQuery {
	var recipes []*CachedQuery
	for _, expr := range r.expressions {
		if _, view := expr.Statement.(sqlparser.SelectStatement); view {
			recipes = append(recipes, expr)
		}
	}
	return recipes
}

func (r *Recipe) ToProto() []*vtboostpb.CachedQuery {
	var cached = make([]*vtboostpb.CachedQuery, 0, len(r.expressions))
	for _, expr := range r.expressions {
		cached = append(cached, expr.CachedQuery)
	}
	slices.SortFunc(cached, func(a, b *vtboostpb.CachedQuery) int {
		return strings.Compare(a.PublicId, b.PublicId)
	})
	return cached
}

func NewRecipeFromProto(queriespb []*vtboostpb.CachedQuery) (*Recipe, error) {
	recipe := newRecipe()
	for _, q := range queriespb {
		if err := recipe.AddQuery(q); err != nil {
			return nil, err
		}
	}
	return recipe, nil
}

func newRecipe() *Recipe {
	return &Recipe{
		expressions:       make(map[QueryHash]*CachedQuery),
		queriesByPublicID: make(map[string]QueryHash),
	}
}

type CachedQuery struct {
	*vtboostpb.CachedQuery
	Statement sqlparser.Statement
}

func (c *CachedQuery) hash() QueryHash {
	var buf = sqlparser.NewTrackedBuffer(nil)
	buf.WriteNode(c.Statement)
	return QueryHash(hack.RuntimeStrhash(buf.String(), 0x1234))
}
