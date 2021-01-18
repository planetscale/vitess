---
title: Analyzing a SQL statement
weight: 1
aliases: ['/docs/user-guides/vtexplain/'] 
---

# Introduction 

This document covers the way Vitess executes a particular SQL statement using the [VTExplain tool](../../../reference/vtexplain). This tool works similarly to the MySQL `EXPLAIN` statement.

## Prerequisites

You can find a prebuilt binary version of the VTExplain tool in [the most recent release of Vitess](https://github.com/vitessio/vitess/releases/).

You can also build the `vtexplain` binary in your environment. To build this binary, refer to the [Build From Source](../../../contributing/build-from-source) guide.

## Overview

To successfully analyze your SQL queries and determine how Vitess executes each statement, follow these steps:

1. Identify a SQL schema for the statement's source tables
1. Identify a VSchema for the statement's source tables 
1. Run the VTExplain tool

If you have a large number of queries you want to analyze for issues, based on a Vschema you’ve created for your database, you can read through a detailed scripted example [here](../vtexplain-in-bulk).

## 1. Identify a SQL schema for tables in the statement

In order to explain a statement, first identify the SQL schema for the tables that the statement uses. This includes tables that a query targets and tables that a DML statement modifies.

### Example SQL Schema

The following example SQL schema creates two tables, `users` and `users_name_idx`, each of which contain the columns `user_id` and `name`, and define both columns as a composite primary key. The example statements in step 3 include these tables.

```
CREATE TABLE users(
  user_id bigint,
  name varchar(128),
  primary key(user_id)
);

CREATE TABLE users_name_idx(
  user_id bigint,
  name varchar(128),
  primary key(name, user_id)
);
```

## 2. Identify a VSchema for the statement's source tables

Next, identify a [VSchema](../../../concepts/vschema) that contains the [Vindexes](../../../reference/vindexes) for the tables in the statement.

### The VSchema must use a keyspace name.

VTExplain requires a keyspace name for each keyspace in an input VSchema:

```
"keyspace_name": {
    "_comment": "Keyspace definition goes here."
}
```

If no keyspace name is present, VTExplain will return the following error:

```
ERROR: initVtgateExecutor: json: cannot unmarshal bool into Go value of type map[string]json.RawMessage
```

### Example VSchema

The following example VSchema defines a single keyspace `mainkeyspace` and three Vindexes, and specifies vindexes for each column in the two tables `users` and `users_name_idx`. The keyspace name `"mainkeyspace"` precedes the keyspace definition object.

```
{
  "mainkeyspace": {
    "sharded": true,
    "vindexes": {
      "hash": {
        "type": "hash"
      },
      "md5": {
        "type": "unicode_loose_md5",
        "params": {},
        "owner": ""
      },
      "users_name_idx": {
        "type": "lookup_hash",
        "params": {
          "from": "name",
          "table": "users_name_idx",
          "to": "user_id"
        },
        "owner": "users"
      }
    },
    "tables": {
      "users": {
        "column_vindexes": [
          {
            "column": "user_id",
            "name": "hash"
          },
          {
            "column": "name",
            "name": "users_name_idx"
          }
        ],
        "auto_increment": null
      },
      "users_name_idx": {
        "type": "",
        "column_vindexes": [
          {
            "column": "name",
            "name": "md5"
          }
        ],
        "auto_increment": null
      }
    }
  }
}
```

## 3. Run the VTExplain tool

To explain a query, pass the SQL schema and VSchema files as arguments to the `VTExplain` command.

### Example: Explaining a SELECT query

In the following example, the `VTExplain` command takes a `SELECT` query and returns the sequence of queries that Vitess runs in order to execute the query:

```
vtexplain -shards 8 -vschema-file vschema.json -schema-file schema.sql -replication-mode "ROW" -output-mode text -sql "SELECT * from users"
----------------------------------------------------------------------
SELECT * from users

1 mainkeyspace/-20: select * from users limit 10001
1 mainkeyspace/20-40: select * from users limit 10001
1 mainkeyspace/40-60: select * from users limit 10001
1 mainkeyspace/60-80: select * from users limit 10001
1 mainkeyspace/80-a0: select * from users limit 10001
1 mainkeyspace/a0-c0: select * from users limit 10001
1 mainkeyspace/c0-e0: select * from users limit 10001
1 mainkeyspace/e0-: select * from users limit 10001

----------------------------------------------------------------------
```

In the example above, the output of `VTExplain` shows the sequence of queries that Vitess runs in order to execute the query. Each line shows the logical sequence of the query, the keyspace where the query executes, the shard where the query executes, and the query that executes, in the following format:

```
[Sequence number] [keyspace]/[shard]: [query]
```

In this example, each query has sequence number `1`, which shows that Vitess executes these in parallel. Vitess automatically adds the `LIMIT 10001` clause to protect against large results.

### Example: Explaining an INSERT query

In the following example, the `VTExplain` command takes an `INSERT` query and returns the sequence of queries that Vitess runs in order to execute the query:

```
vtexplain -shards 128 -vschema-file vschema.json -schema-file schema.sql -replication-mode "ROW" -output-mode text -sql "INSERT INTO users (user_id, name) VALUES(1, 'john')"

----------------------------------------------------------------------
INSERT INTO users (user_id, name) VALUES(1, 'john')

1 mainkeyspace/22-24: begin
1 mainkeyspace/22-24: insert into users_name_idx(name, user_id) values ('john', 1) /* vtgate:: keyspace_id:22c0c31d7a0b489a16332a5b32b028bc */
2 mainkeyspace/16-18: begin
2 mainkeyspace/16-18: insert into users(user_id, name) values (1, 'john') /* vtgate:: keyspace_id:166b40b44aba4bd6 */
3 mainkeyspace/22-24: commit
4 mainkeyspace/16-18: commit

----------------------------------------------------------------------
```

The example above shows how Vitess handles an insert into a table with a secondary lookup Vindex:

* At sequence number `1`, Vitess opens a transaction on shard `22-24` to insert the row into the `users_name_idx` table.
* At sequence number `2`, Vitess opens a second transaction on shard `16-18` to perform the actual insert into the `users` table.
* At sequence number `3`, the first transaction commits.
* At sequence number `4`, the second transaction commits.

### Example: Explaining an uneven keyspace

In previous examples, we used the `-shards` flag to set up an evenly-sharded keyspace, where each shard covers the same fraction of the keyrange.
`VTExplain` also supports receiving a JSON mapping of shard ranges to see how Vitess would handle a query against an arbitrarly-sharded keyspace.

To do this, we first create a JSON file containing a mapping of keyspace names to shardrange maps.
The shardrange map has the same structure as the output of running `vtctl FindAllShardsInKeyspace <keyspace>`.

```
{
  "mainkeyspace": {
    "-80": {
      "master_alias": {
        "cell": "test",
        "uid": 00000000100
      },
      "master_term_start_time": {
        "seconds": 1599828375,
        "nanoseconds": 664404881
      },
      "key_range": {
        "end": "gA=="
      },
      "is_master_serving": true
    },
    "80-90": {
      "master_alias": {
        "cell": "test",
        "uid": 00000000200
      },
      "master_term_start_time": {
        "seconds": 1599828344,
        "nanoseconds": 868327074
      },
      "key_range": {
        "start": "gA==",
        "end": "kA=="
      },
      "is_master_serving": true
    },
    "90-a0": {
      "master_alias": {
        "cell": "test",
        "uid": 00000000300
      },
      "master_term_start_time": {
        "seconds": 1599828405,
        "nanoseconds": 152120945
      },
      "key_range": {
        "start": "kA==",
        "end": "oA=="
      },
      "is_master_serving": true
    },
    "a0-e8": {
      "master_alias": {
        "cell": "test",
        "uid": 00000000400
      },
      "master_term_start_time": {
        "seconds": 1599828183,
        "nanoseconds": 911677983
      },
      "key_range": {
        "start": "oA==",
        "end": "6A=="
      },
      "is_master_serving": true
    },
    "e8-": {
      "master_alias": {
        "cell": "test",
        "uid": 00000000500
      },
      "master_term_start_time": {
        "seconds": 1599827865,
        "nanoseconds": 770606551
      },
      "key_range": {
        "start": "6A=="
      },
      "is_master_serving": true
    }
}
```

After having saved that to a file called `shardmaps.json`:

```
vtexplain -vschema-file vschema.json -schema-file schema.sql -ks-shard-map shardmaps.json -replication-mode "ROW" -output-mode text -sql "SELECT * FROM users; SELECT * FROM users WHERE id IN (10, 17, 42, 1000);"
----------------------------------------------------------------------
SELECT * FROM users

1 mainkeyspace/-80: select * from users limit 10001
1 mainkeyspace/80-90: select * from users limit 10001
1 mainkeyspace/90-a0: select * from users limit 10001
1 mainkeyspace/a0-e8: select * from users limit 10001
1 mainkeyspace/e8-: select * from users limit 10001

----------------------------------------------------------------------
SELECT * FROM users WHERE id IN (10, 17, 42, 100000)

1 mainkeyspace/-80: select * from users where id in (10, 17, 42) limit 10001
1 mainkeyspace/80-90: select * from users where id in (100000) limit 10001

----------------------------------------------------------------------
```


## See also

* For detailed configuration options for VTExplain, see the [VTExplain syntax reference](../../../reference/vtexplain).

