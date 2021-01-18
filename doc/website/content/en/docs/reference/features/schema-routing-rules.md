---
title: Schema Routing Rules
aliases: ['/docs/schema-management/routing-rules/','/docs/reference/schema-routing-rules/']
---

The Vitess routing rules feature is a powerful mechanism for directing traffic to the right keyspaces, shards or tablet types.
It fulfils the following use cases:

* **Routing traffic during resharding**: During resharding, you can specify rules that decide where to send reads and writes. For example,
  you can move traffic from the source shard to the destination shards, but only for the `rdonly` or `replica` types. This gives you
  the option to try out the new shards and make sure they will work as intended before committing to move the rest of the traffic.
* **Table equivalence**: The new VReplication feature allows you to materialize tables in different keyspaces. In this situation,
  you can specify that two tables are 'equivalent'. This will allow VTGate to use the best possible plan depending on the input
  query.

## ApplyRoutingRules

You can use the vtctlclient command to apply routing rules:

```
ApplyRoutingRules {-rules=<rules> || -rules_file=<rules_file=<sql file>} [-cells=c1,c2,...] [-skip_rebuild] [-dry-run]
```

## Syntax

### Resharding 

Routing rules can be specified using JSON format. Here's an example:

``` json
{"rules": [
  {
    "from_table": "t@rdonly",
    "to_tables": ["target.t"]
  }, {
    "from_table": "target.t",
    "to_tables": ["source.t"]
  }, {
    "from_table": "t",
    "to_tables": ["source.t"]
  }
]}
```

The above JSON specifies the following rules:
* If you sent a query accessing `t` for an `rdonly` instance, then it would be sent to table `t` in the `target` keyspace.
* If you sent a query accessing `target.t` for anything other than `rdonly`, it would be sent `t` in the `source` keyspace.
* If you sent a query accessing `t` without any qualification, it would be sent to `t` in the `source` keyspace.

These rules are an example of how they can be used to shift traffic for a table during a vertical resharding process.
In this case, the assumption is that we are moving `t` from `source` to `target`, and so far, we've shifted traffic
for just the `rdonly` tablet types.

By updating these rules, you can eventually move all traffic to `target.t`

The rules are applied only once. The resulting targets need to specify fully qualified table names.

### Table equivalence

The routing rules allow you to specify table equivalence. Here's an example:

``` json
{"rules": [
  {
    "from_table": "product",
    "to_tables": ["lookup.product", "user.uproduct"]
  }
]}
```

In the above case, we are declaring that the `product` table is present in both `lookup` and `user`. If a query is issued
using the unqualified `product` table, then VTGate will consider sending the query to both `lookup.product` as well
as `user.uproduct` (note the name change).

For example, if `user` was a sharded keyspace, and the query joined a `user` table with `product`, then vtgate will
know that it's better to send the query to the `user` keyspace instead of `lookup`.

Typically, table equivalence makes sense when a view table is materialized from a source table using VReplication.

### Orthogonality

The tablet type targeting and table equivalence features are orthogonal to each other and can be combined. Although
there's no immediate use case for this, it's a possibility we can consider if the use case arises.
