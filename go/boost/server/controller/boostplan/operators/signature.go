package operators

import (
	"sort"

	"github.com/segmentio/fasthash/fnv1"

	"vitess.io/vitess/go/vt/sqlparser"
)

type QuerySignature struct {
	Tables     []string
	Predicates []string
}

type Hash uint64

func (sig QuerySignature) Hash() Hash {
	sort.Strings(sig.Tables)
	var hash = fnv1.Init64
	for _, rel := range sig.Tables {
		hash = fnv1.AddString64(hash, rel)
	}
	sort.Strings(sig.Predicates)
	for _, rel := range sig.Predicates {
		hash = fnv1.AddString64(hash, rel)
	}
	return Hash(hash)
}

func (sig QuerySignature) Merge(other QuerySignature) QuerySignature {
outer:
	for _, otherTable := range other.Tables {
		for _, sigTable := range sig.Tables {
			if otherTable == sigTable {
				continue outer
			}
		}
		sig.Tables = append(sig.Tables, otherTable)
	}
	sig.Predicates = append(sig.Predicates, other.Predicates...)
	return sig
}

func (t *Table) Signature() QuerySignature {
	return QuerySignature{
		Tables:     []string{t.Keyspace + "." + t.TableName},
		Predicates: nil,
	}
}

func (j *Join) Signature() QuerySignature {
	return QuerySignature{}.AddPredicate(j.Predicates)
}

func (sig QuerySignature) AddPredicate(predicate sqlparser.Expr) QuerySignature {
	predicates := sqlparser.SplitAndExpression(nil, predicate)
	for _, predicate := range predicates {
		sig.Predicates = append(sig.Predicates, sqlparser.String(predicate))
	}
	return sig
}

func (f *Filter) Signature() QuerySignature {
	return QuerySignature{}.AddPredicate(f.Predicates)
}

func (n *NullFilter) Signature() QuerySignature {
	return QuerySignature{}.AddPredicate(n.Predicates)
}

func (g *GroupBy) Signature() QuerySignature {
	return QuerySignature{}
}

func (p *Project) Signature() QuerySignature {
	return QuerySignature{}
}

func (v *View) Signature() QuerySignature {
	return QuerySignature{}
}

func (n *NodeTableRef) Signature() QuerySignature {
	return n.Node.Signature()
}

func (u *Union) Signature() QuerySignature {
	return QuerySignature{}
}

func (t *TopK) Signature() QuerySignature {
	return QuerySignature{}
}

func (d *Distinct) Signature() QuerySignature {
	return QuerySignature{}
}
