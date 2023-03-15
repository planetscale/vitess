package flownode

import (
	"fmt"

	"vitess.io/vitess/go/boost/common/graphviz"
	"vitess.io/vitess/go/boost/dataflow"
)

type GraphvizOptions struct {
	Materialization dataflow.MaterializationStatus
	ShowSchema      bool
}

func (n *Node) RenderGraphviz(gvz *graphviz.Node, options GraphvizOptions) {
	gvz.Attr["tooltip"] = graphviz.JSON(n.ToProto())

	switch n.shardedBy.Mode {
	case dataflow.Sharding_ByColumn, dataflow.Sharding_Random:
		gvz.Attr["style"] = "filled,dashed"
	default:
		gvz.Attr["style"] = "filled"
	}

	var (
		materialized string
		sharding     string
		addr         = graphviz.Cell{
			Attr: map[string]string{"PORT": "node_addr"},
		}
	)

	if n.domain != dataflow.InvalidDomainIdx {
		gvz.Attr["fillcolor"] = fmt.Sprintf("/set312/%d", (n.domain%12)+1)
	} else {
		gvz.Attr["fillcolor"] = "white"
	}

	switch options.Materialization {
	case dataflow.MaterializationNone:
	case dataflow.MaterializationPartial:
		materialized = "◕"
	case dataflow.MaterializationFull:
		materialized = "●"
	}

	switch n.shardedBy.Mode {
	case dataflow.Sharding_ByColumn:
		sharding = fmt.Sprintf("shard ⚷: %s / %d-way", n.fields[n.shardedBy.Col], n.shardedBy.Shards)
	case dataflow.Sharding_Random:
		sharding = "shard randomly"
	case dataflow.Sharding_None:
		sharding = "unsharded"
	case dataflow.Sharding_ForcedNone:
		sharding = "desharded to avoid SS"
	}

	if n.index.IsEmpty() {
		addr.Txt = "???"
	} else {
		if n.index.HasLocal() {
			addr.Txt = graphviz.Fmt("%d<FONT COLOR=\"grey\">(%d)</FONT>", n.index.AsGlobal(), n.index.AsLocal())
		} else {
			addr.Txt = graphviz.Fmt("%d<FONT COLOR=\"grey\">(?)</FONT>", n.index.AsGlobal())
		}
	}

	switch impl := n.impl.(type) {
	case *Root:
		gvz.Row("(source)")
		return
	case *Dropped:
		gvz.Row(addr, "(dropped)")
		return
	case *Table:
		gvz.Row(addr, graphviz.Fmt("%s.<B>%s</B> <I>(external)</I>", impl.Keyspace(), impl.Name()))
		gvz.Row(impl.keyspace)
	case *Ingress:
		gvz.Row(addr, materialized)
		gvz.Row("(ingress)")
	case *Egress:
		gvz.Row(addr)
		gvz.Row("(egress)")
	case *Sharder:
		gvz.Row(addr)
		gvz.Row("shard by " + n.fields[impl.ShardedBy()])
	case *Reader:
		var key string
		if impl.Key() == nil {
			key = "none"
		} else {
			key = fmt.Sprintf("%v", impl.Key())
		}
		gvz.Row(addr, graphviz.Fmt("<B>%s</B>", impl.PublicID()), materialized)
		gvz.Row("(reader / ⚷: " + key + ")")
	case Internal:
		gvz.Row(addr, n.name, materialized)
		gvz.Row(graphviz.Fmt("<FONT POINT-SIZE=\"10\">%s</FONT>", impl.Description()))
	}

	gvz.Row(sharding)

	if options.ShowSchema {
		schema := n.Schema()
		fields := n.Fields()
		reader := n.AsReader()

		for i, f := range fields {
			var (
				collname  string
				fieldname graphviz.Escaped
				fieldn    graphviz.Cell
			)
			if coll := schema[i].Collation.Get(); coll != nil {
				collname = coll.Name()
			} else {
				collname = "???"
			}
			if reader != nil && i >= reader.columnsForUser {
				fieldname = graphviz.Fmt(`<I><FONT COLOR="grey">[%s]</FONT></I>`, f)
			} else {
				fieldname = graphviz.Fmt("%s", f)
			}
			fieldn = graphviz.Cell{
				Txt: graphviz.Fmt("%d", i),
				Attr: map[string]string{
					"PORT": fmt.Sprintf("field%d", i),
				},
			}
			gvz.Row(fieldn, fieldname, graphviz.Fmt("%s (%s)", schema[i].T.String(), collname))
		}
	}
}
