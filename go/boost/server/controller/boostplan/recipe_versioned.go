package boostplan

import (
	"context"

	"go.uber.org/multierr"
	"go.uber.org/zap"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/common"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller/boostplan/operators"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
	"vitess.io/vitess/go/vt/sqlparser"
)

type QFP interface {
	Leaf() graph.NodeIdx
	GetName() string
	GetTableReport() *operators.TableReport
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

func (r *VersionedRecipe) NodeAddrFor(name string) (graph.NodeIdx, bool) {
	if r.inc == nil {
		panic("recipe not applied")
	}
	if alias, ok := r.ResolveAlias(name); ok {
		return r.inc.GetQueryAddress(alias)
	}
	return r.inc.GetQueryAddress(name)
}

func (r *VersionedRecipe) Activate(ctx context.Context, mig Migration, schema *SchemaInformation) (*ActivationResult, error) {
	log := common.Logger(ctx)
	log.Info("activating new recipe",
		zap.Int64("version", r.version),
	)

	var added []QueryID
	var removed []QueryID
	var unchanged []QueryID

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
		r.inc.UpgradeSchema(r.version)
	}

	// add new queries to the Soup graph carried by `mig`, and reflect state in the
	// incorporator in `inc`. `NodeIndex`es for new nodes are collected in `new_nodes` to be
	// returned to the caller (who may use them to obtain mutators and getters)
	var errors []error
	for _, qid := range added {
		q := r.GetQuery(qid)
		stmt := sqlparser.CloneStatement(q.Statement)
		qfp, err := r.inc.AddParsedQuery(q.Keyspace, stmt, q.Name, true, mig, schema)
		if err != nil {
			errors = append(errors, &QueryScopedError{err, q})
			continue
		}

		// If the user provided us with a query name, use that.
		// If not, use the name internally used by the QFP.
		var queryName string
		if q.Name != "" {
			queryName = q.Name
		} else {
			queryName = qfp.GetName()
		}

		activation.NodesAdded[queryName] = qfp.Leaf()
		activation.QueriesAdded = append(activation.QueriesAdded, q)
	}

	for _, qid := range removed {
		oldq := r.prior.GetQuery(qid)

		switch oldq.Statement.(type) {
		case *sqlparser.CreateTable:
			panic("CreateTable statements are implicit and should never be removed")
		default:
			if rm := r.inc.RemoveQuery(oldq.Name); rm != graph.InvalidNode {
				activation.NodesRemoved = append(activation.NodesRemoved, rm)
				activation.QueriesRemoved = append(activation.QueriesRemoved, oldq)
			}
		}
	}

	return activation, multierr.Combine(errors...)
}

func (r *VersionedRecipe) revert() *VersionedRecipe {
	if r.prior != nil {
		return r.prior
	}
	return BlankRecipe()
}

func (r *VersionedRecipe) EnableReuse(reuse boostpb.ReuseType) {
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
