package boostplan

import (
	"errors"
	"fmt"

	"go.uber.org/multierr"

	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller/boostplan/operators"
	"vitess.io/vitess/go/boost/server/controller/config"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
	"vitess.io/vitess/go/vt/sqlparser"
)

type QFP interface {
	Leaf() graph.NodeIdx
	GetTableReport() []*operators.TableReport
}

type VersionedRecipe struct {
	*Recipe

	version int64
	prior   *VersionedRecipe
	inc     *Incorporator
}

func BlankRecipe() *VersionedRecipe {
	return &VersionedRecipe{
		Recipe:  newRecipe(),
		version: 0,
		prior:   nil,
		inc:     NewIncorporator(),
	}
}

func (r *VersionedRecipe) Activate(mig Migration, schema *SchemaInformation) (*ActivationResult, error) {
	var added []QueryHash
	var removed []QueryHash
	var unchanged []QueryHash

	if r.prior == nil {
		added, removed, unchanged = r.ComputeDelta(newRecipe())
	} else {
		added, removed, unchanged = r.ComputeDelta(r.prior.Recipe)
	}

	activation := &ActivationResult{
		NodesAdded:   make(map[string]graph.NodeIdx),
		NodesRemoved: nil,
	}

	for _, qq := range unchanged {
		activation.QueriesUnchanged = append(activation.QueriesUnchanged, r.GetQuery(qq))
	}

	// upgrade schema version *before* applying changes, so that new queries are correctly
	// tagged with the new version. If this recipe was just created, there is no need to
	// upgrade the schema version, as the SqlIncorporator's version will still be at zero.
	if r.version > 0 {
		err := r.inc.UpgradeSchema(r.version)
		if err != nil {
			return nil, err
		}
	}

	// add new queries to the Soup graph carried by `mig`, and reflect state in the
	// incorporator in `inc`. `NodeIndex`es for new nodes are collected in `new_nodes` to be
	// returned to the caller (who may use them to obtain mutators and getters)
	var errs []error
	for _, qid := range added {
		q := r.GetQuery(qid)

		if q.PublicId == "" {
			errs = append(errs, errors.New("query has no public id"))
			continue
		}
		stmt := sqlparser.CloneStatement(q.Statement)
		qfp, err := r.inc.AddParsedQuery(q.Keyspace, stmt, q.PublicId, mig, schema)
		if err != nil {
			errs = append(errs, &QueryScopedError{err, q})
			continue
		}

		if _, exists := activation.NodesAdded[q.PublicId]; exists {
			errs = append(errs, fmt.Errorf("query with public id %q already exists", q.PublicId))
			continue
		}
		activation.NodesAdded[q.PublicId] = qfp.Leaf()
		activation.QueriesAdded = append(activation.QueriesAdded, q)
	}

	for _, qid := range removed {
		oldq := r.prior.GetQuery(qid)

		switch oldq.Statement.(type) {
		case *sqlparser.CreateTable:
			return nil, errors.New("cannot remove create table statements")
		default:
			rm, err := r.inc.RemoveQuery(oldq.PublicId)
			if err != nil {
				errs = append(errs, &QueryScopedError{err, oldq})
				continue
			}
			activation.NodesRemoved = append(activation.NodesRemoved, rm)
			activation.QueriesRemoved = append(activation.QueriesRemoved, oldq)
		}
	}

	return activation, multierr.Combine(errs...)
}

func (r *VersionedRecipe) revert() *VersionedRecipe {
	if r.prior != nil {
		return r.prior
	}
	return BlankRecipe()
}

func (r *VersionedRecipe) EnableReuse(reuse config.ReuseType) {
	r.inc.EnableReuse(reuse)
}

func (r *VersionedRecipe) IsLeafAddress(ni graph.NodeIdx) bool {
	return r.inc.IsLeafAddress(ni)
}

func (r *VersionedRecipe) Version() int64 {
	return r.version
}

func NewVersionedRecipe(prior *VersionedRecipe, recipepb *vtboostpb.Recipe) (*VersionedRecipe, error) {
	r, err := NewRecipeFromProto(recipepb.Queries)
	if err != nil {
		return nil, err
	}
	vr := &VersionedRecipe{
		Recipe:  r,
		version: recipepb.Version,
	}
	if prior != nil {
		vr.prior = prior
		vr.inc = prior.inc.Clone()
	} else {
		vr.inc = NewIncorporator()
	}
	return vr, nil
}

type ActivationResult struct {
	NodesAdded   map[string]graph.NodeIdx
	NodesRemoved []graph.NodeIdx

	QueriesAdded     []*CachedQuery
	QueriesRemoved   []*CachedQuery
	QueriesUnchanged []*CachedQuery
}
