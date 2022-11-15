package flownode

import (
	"fmt"
	"strconv"
	"strings"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/common/graphviz"
	"vitess.io/vitess/go/mysql/collations"
)

type DescribeOptions struct {
	Materialization boostpb.MaterializationStatus
	ShowSchema      bool
}

func (n *Node) Describe(gvz *graphviz.Node, options DescribeOptions) {
	switch n.shardedBy.Mode {
	case boostpb.Sharding_ByColumn, boostpb.Sharding_Random:
		gvz.Attr["style"] = "filled,dashed"
	default:
		gvz.Attr["style"] = "filled"
	}

	var (
		materialized string
		sharding     string
		addr         graphviz.Escaped
	)

	if n.domain != boostpb.InvalidDomainIndex {
		gvz.Attr["fillcolor"] = fmt.Sprintf("/set312/%d", (n.domain%12)+1)
	} else {
		gvz.Attr["fillcolor"] = "white"
	}

	switch options.Materialization {
	case boostpb.MaterializationNone:
	case boostpb.MaterializationPartial:
		if n.Purge {
			materialized = "◔"
		} else {
			materialized = "◕"
		}
	case boostpb.MaterializationFull:
		materialized = "●"
	}

	switch n.shardedBy.Mode {
	case boostpb.Sharding_ByColumn:
		sharding = fmt.Sprintf("shard ⚷: %s / %d-way", n.fields[n.shardedBy.Col], n.shardedBy.Shards)
	case boostpb.Sharding_Random:
		sharding = "shard randomly"
	case boostpb.Sharding_None:
		sharding = "unsharded"
	case boostpb.Sharding_ForcedNone:
		sharding = "desharded to avoid SS"
	}

	if n.index.IsEmpty() {
		addr = "???"
	} else {
		if n.index.HasLocal() {
			addr = graphviz.Fmt("%d <FONT COLOR=\"grey\" POINT-SIZE=\"10\">(%d)</FONT>", n.index.AsGlobal(), n.index.AsLocal())
		} else {
			addr = graphviz.Fmt("%d <FONT COLOR=\"grey\" POINT-SIZE=\"10\">(?)</FONT>", n.index.AsGlobal())
		}
	}

	switch impl := n.impl.(type) {
	case *Source:
		gvz.Row("(source)")
		return
	case *Dropped:
		gvz.Row(addr, "dropped")
	case *Base:
		gvz.Row(addr, graphviz.Fmt("<B>%s</B>", n.Name), materialized)
		gvz.Row(sharding)
		if !options.ShowSchema {
			gvz.Row(strings.Join(n.fields, ", \n"))
		}
	case *ExternalBase:
		gvz.Row(addr, graphviz.Fmt("<B>%s</B> <I>(external)</I>", n.Name))
		gvz.Row(impl.keyspace)
		gvz.Row(sharding)
		if !options.ShowSchema {
			gvz.Row(strings.Join(n.fields, ", \n"))
		}
	case *Ingress:
		gvz.Row(addr, materialized)
		gvz.Row("(ingress)")
		gvz.Row(sharding)
	case *Egress:
		gvz.Row(addr)
		gvz.Row("(egress)")
		gvz.Row(sharding)
	case *Sharder:
		gvz.Row(addr)
		gvz.Row("shard by " + n.fields[impl.ShardedBy()])
		gvz.Row(sharding)
	case *Reader:
		var key string
		if impl.Key() == nil {
			key = "none"
		} else {
			key = fmt.Sprintf("%v", impl.Key())
		}
		gvz.Row(addr, graphviz.Fmt("<B>%s</B>", n.Name), materialized)
		gvz.Row("(reader / ⚷: " + key + ")")
		gvz.Row(sharding)

		if !options.ShowSchema {
			var fields string
			if len(n.fields) > impl.columnsForUser {
				fields += strings.Join(n.fields[:impl.columnsForUser], ", ")
				fields += ` <I><FONT COLOR="grey">[, `
				fields += strings.Join(n.fields[impl.columnsForUser:], ", ")
				fields += `]</FONT></I>`
			} else {
				fields = strings.Join(n.fields, ", ")
			}
			gvz.Row(graphviz.Escaped(fields))
		}
	case Internal:
		gvz.Row(addr, n.Name, materialized)
		gvz.Row(graphviz.Fmt("<FONT POINT-SIZE=\"8\">%s</FONT>", impl.Description(true)))
		gvz.Row(sharding)
		if !options.ShowSchema {
			gvz.Row(strings.Join(n.fields, ", "))
		}
	}

	if options.ShowSchema {
		schema := n.Schema()
		fields := n.Fields()

		for i, f := range fields {
			collname := "???"
			if coll := collations.Local().LookupByID(schema[i].Collation); coll != nil {
				collname = coll.Name()
			}
			gvz.Row(strconv.Itoa(i), f, graphviz.Fmt("%s (%s)", schema[i].T.String(), collname))
		}
	}
}
