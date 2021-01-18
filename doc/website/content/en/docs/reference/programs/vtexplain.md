---
title: vtexplain
aliases: ['/docs/reference/vtexplain/']
---

`vtexplain` is a command line tool which provides information on how Vitess plans to execute a particular query. It can be used to validate queries for compatibility with Vitess.

For a user guide that describes how to use the `vtexplain` tool to explain how Vitess executes a particular SQL statement, see [Analyzing a SQL statement](../../../user-guides/sql/vtexplain/).

## Example Usage

Explain how Vitess will execute the query `SELECT * FROM users` using the VSchema contained in `vschemas.json` and the database schema `schema.sql`:

```bash
vtexplain -vschema-file vschema.json -schema-file schema.sql  -sql "SELECT * FROM users"
```

Explain how the example will execute on 128 shards using Row-based replication:

```bash
vtexplain -shards 128 -vschema-file vschema.json -schema-file schema.sql -replication-mode "ROW" -output-mode text -sql "INSERT INTO users (user_id, name) VALUES(1, 'john')"
```


## Options

The following parameters apply to `mysqlctl`:

| Name | Type | Definition |
| :-------------------- | :--------- | :---------------------------------------------------- |
| -output-mode | string | Output in human-friendly text or json (default "text") |
| -normalize |  | Whether to enable vtgate normalization (default false) |
| -shards | int | Number of shards per keyspace (default 2) |
| -replication-mode | string | The replication mode to simulate -- must be set to either ROW or STATEMENT (default "ROW") |
| -schema | string | The SQL table schema (default "") |
| -schema-file | string | Identifies the file that contains the SQL table schema (default "") |
| -sql | string | A list of semicolon-delimited SQL commands to analyze (default "") |
| -sql-file | string | Identifies the file that contains the SQL commands to analyze (default "") |
| -vschema | string | Identifies the VTGate routing schema (default "") |
| -vschema-file | string | Identifies the VTGate routing schema file (default "") |
| -ks-shard-map | string | Identifies the shard keyranges for unevenly-sharded keyspaces (default "") |
| -ks-shard-map-file | string | Identifies the shard keyranges file (default "") |
| -dbname | string | Optional database target to override normal routing (default "") |
| -queryserver-config-passthrough-dmls |  | query server pass through all dml statements without rewriting (default false) |

<br>

Please note that `-ks-shard-map` and `ks-shard-map-file` will supercede `-shards`.
If you attempt to `vtexplain` on a keyspace that is included in the keyspace shard map, the shards as defined in the mapping will be used and `-shards` will be ignored.

## Limitations

### The VSchema must use a keyspace name.

VTExplain requires a keyspace name for each keyspace in an input VSChema:

```
"keyspace_name": {
    "_comment": "Keyspace definition goes here."
}
```

If no keyspace name is present, VTExplain will return the following error:

```
ERROR: initVtgateExecutor: json: cannot unmarshal bool into Go value of type map[string]json.RawMessage
```  