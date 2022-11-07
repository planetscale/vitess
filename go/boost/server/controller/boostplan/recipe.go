package boostplan

import (
	"fmt"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/hack"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
	"vitess.io/vitess/go/vt/sqlparser"
)

type QueryID uint64

type Recipe struct {
	expressions map[QueryID]*CachedQuery
	aliases     map[string]QueryID
	sorted      []QueryID
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

	var newQuery = &CachedQuery{
		CachedQuery: query,
		Statement:   stmt,
	}

	qid := newQuery.hash()
	if _, ok := r.expressions[qid]; ok {
		return nil
	}
	if newQuery.Name != "" {
		if existingqid, ok := r.aliases[newQuery.Name]; ok {
			if existingqid != qid {
				return fmt.Errorf("a query named %q already exists", newQuery.Name)
			}
		}
		r.aliases[newQuery.Name] = qid
	}

	r.expressions[qid] = newQuery
	r.sorted = append(r.sorted, qid)
	return nil
}

func (r *Recipe) RemoveQueryByPublicID(public string) error {
	for qid, expr := range r.expressions {
		if expr.PublicId == public {
			delete(r.expressions, qid)
			if expr.Name != "" {
				delete(r.aliases, expr.Name)
			}
			if idx := slices.Index(r.sorted, qid); idx >= 0 {
				r.sorted = slices.Delete(r.sorted, idx, idx+1)
			}
			return nil
		}
	}
	return &UnknownPublicIDError{PublicID: public}
}

func (r *Recipe) RemoveQueryByName(name string) error {
	qid, ok := r.aliases[name]
	if !ok {
		return &UnknownQueryError{Name: name}
	}
	delete(r.aliases, name)
	delete(r.expressions, qid)
	if idx := slices.Index(r.sorted, qid); idx >= 0 {
		r.sorted = slices.Delete(r.sorted, idx, idx+1)
	}
	return nil
}

func (r *Recipe) Reset() {
	r.expressions = make(map[QueryID]*CachedQuery)
	r.aliases = make(map[string]QueryID)
	r.sorted = nil
}

func (r *Recipe) ComputeDelta(other *Recipe) (added []QueryID, removed []QueryID, unchanged []QueryID) {
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

func (r *Recipe) ResolveAlias(name string) (string, bool) {
	if qid, ok := r.aliases[name]; ok {
		rq := r.expressions[qid]
		return rq.Name, true
	}
	return "", false
}

func (r *Recipe) GetExpressionByName(name string) (sqlparser.Statement, bool) {
	if qid, ok := r.aliases[name]; ok {
		return r.expressions[qid].Statement, true
	}
	return nil, false
}

func (r *Recipe) GetQuery(qid QueryID) *CachedQuery {
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
	slices.SortFunc(cached, func(a, b *vtboostpb.CachedQuery) bool {
		return a.PublicId < b.PublicId
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
		expressions: make(map[QueryID]*CachedQuery),
		aliases:     make(map[string]QueryID),
	}
}

type CachedQuery struct {
	*vtboostpb.CachedQuery
	Statement sqlparser.Statement
}

func (c *CachedQuery) hash() QueryID {
	var buf = sqlparser.NewTrackedBuffer(nil)
	buf.WriteNode(c.Statement)
	return QueryID(hack.RuntimeStrhash(buf.String(), 0x1234))
}
