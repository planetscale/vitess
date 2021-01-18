---
title: Overview
description: VReplication features, design and options in a nutshell
weight: 2
---

VReplication is a core component of Vitess that can be used to compose
many features. It can be used for the following use cases:

* **Resharding**: Legacy workflows of vertical and horizontal resharding.
  New workflows of resharding from an unsharded to a sharded keyspace and
  vice-versa. Resharding from an unsharded to an unsharded keyspace using
  a different vindex than the source keyspace.
* **Materialized Views**: You can specify a materialization rule that creates
  a view of the source table into a target keyspace. This materialization
  can use a different primary vindex than the source. It can also materialize
  a subset of the source columns, or add new expressions from the source.
  This view will be kept up-to-date in real time. One can also materialize
  reference tables onto all shards and have Vitess perform efficient
  local joins with those materialized tables.
* **Realtime rollups**: The materialization expression can include aggregation
  expressions in which case, Vitess will create a rolled up version of the
  source table which can be used for realtime analytics.
* **Backfilling lookup vindexes**: VReplication can be used to backfill a
  newly created lookup vindex. Workflows can be built to manage the switching
  from a backfill mode to the vindex itself keeping it up-to-date.
* **Schema deployment**: VReplication can be used to recreate the workflow
  performed by gh-ost and thereby support zero-downtime schema deployments
  in Vitess natively.
* **Data migration**: VReplication can be setup to migrate data from an
  existing system into Vitess. The replication could also be reversed after
  a cutover giving you the option to rollback a migration cutover if something
  went wrong, without losing the writes to the migration target.
* **Change notification**: The streamer component of VReplication can be
  used for the application or a systems operator to subscribe to change
  notification and use it to keep downstream systems up-to-date with the
  source.

The VReplication feature itself is a fairly low level one that is
expected to be used as a building block for the above use cases. However,
it is still possible to directly issue commands to perform some of the
activities.

## Feature description

VReplication works as a stream or set of streams. Each stream
establishes a replication from a source keyspace/shard into a target
keyspace/shard.

A given stream can replicate multiple tables. For each table, you can
specify a `SELECT` statement that represents both the transformation
rule and the filtering rule. The `SELECT` expressions specify the
transformation, and the `WHERE` clause specifies the filtering.

The `SELECT` expressions can be any non-aggregate MySQL expression, or
they can also be `COUNT` or `SUM` as aggregate expressions. Aggregate
expressions combined with the corresponding `GROUP BY` clauses will
allow you to materialize real-time rollups of the source table, which
can be used for analytics. The target table can have a different name
from the source.

For a sharded system like Vitess, multiple VReplication streams
may be needed to achieve the objective. This is because there
can be multiple source shards and multiple destination shards, and
the relationship between them may not be one to one.

VReplication performs the following essential functions:

* Copy data from the source to the destination table in a consistent
  fashion. For a large table, this copy can be long-running. It can be
  interrupted and resumed. If interrupted, VReplication can keep
  the copied portion up-to-date with respect to the source, and it can
  resume the copy process at a point that is consistent with the
  current replication position.
* After copying is finished, it can continuously replicate the data
  from the source to destination.
* The copying rule can be expressed as a `SELECT` statement. The
  statement should be simple enough that the materialized table can
  be kept up-to-date from the data coming from the binlog. For
  example, joins in the `SELECT` statement are not supported.
* Correctness verification (to be implemented): VReplication can
  verify that the target table is an exact representation of
  the `SELECT` statement from the source by capturing consistent
  snapshots of the source and target and comparing them against each
  other. This step can be done without the need to create special
  snapshot replicas.
* Journaling: If there is any kind of traffic cut-over where we
  start writing to a different table than we used
  to before, VReplication will save the current binlog positions
  into a journal table. This can be used by other streams to resume
  replication from the new source.
* Routing rules: Although this feature is itself not a direct
  functionality of VReplication, it works hand in hand with it. It allows
  you to specify sophisticated rules about where to route queries
  depending on the type of workflow being performed. For example,
  it can be used to control the cut-over during resharding. In
  the case of materialized views, it can be used to establish
  equivalence of tables, which will allow VTGate to compute the most optimal
  plans given the available options.

<a name="exec"></a>

## VReplicationExec

The `VReplicationExec` command is a low-level command used to manage
VReplication streams.  The commands are issued as SQL statements. For
example, a `SELECT` can be used to see the current list of streams. An
`INSERT` can be used to create one, etc. By design, the metadata for
vreplication streams are stored in a table called `vreplication` in the `_vt`
sidecar database. VReplication uses a 'pull' model. This means that a stream is
created on the target side, and the target pulls the data by finding
the appropriate source. As a result, this metadata is stored on the
target shard.

The table schema is as follows:

```
CREATE TABLE _vt.vreplication (
  id INT AUTO_INCREMENT,
  workflow VARBINARY(1000),
  source VARBINARY(10000) NOT NULL,
  pos VARBINARY(10000) NOT NULL,
  stop_pos VARBINARY(10000) DEFAULT NULL,
  max_tps BIGINT(20) NOT NULL,
  max_replication_lag BIGINT(20) NOT NULL,
  cell VARBINARY(1000) DEFAULT NULL,
  tablet_types VARBINARY(100) DEFAULT NULL,
  time_updated BIGINT(20) NOT NULL,
  transaction_timestamp BIGINT(20) NOT NULL,
  state VARBINARY(100) NOT NULL,
  message VARBINARY(1000) DEFAULT NULL,
  db_name VARBINARY(255) NOT NULL,
  PRIMARY KEY (id)
)
```

The fields are explained in the following section.

This is the syntax of the command:

```
VReplicationExec [-json] <tablet alias> <sql command>
```

Here's an example of the command to list all existing streams for
a given tablet.

```
vtctlclient -server localhost:15999 VReplicationExec 'tablet-100' 'select * from _vt.vreplication'
```

### Creating a stream

It's generally easier to send the VReplication command programmatically
instead of a bash script. This is because of the number of nested encodings
involved:

* One of the arguments is an SQL statement, which can contain quoted
  strings as values.
* One of the strings in the SQL statement is a string encoded protobuf,
  which can contain quotes.
* One of the parameters within the protobuf is an SQL `SELECT` expression
  for the materialized view.

However, you can use [vreplgen.go](https://github.com/vitessio/contrib/blob/master/vreplgen/vreplgen.go) to generate a fully escaped bash command.

Alternately, you can use a python program. Here's an example:

```python
cmd = [
  'vtctlclient',
  '-server',
  'localhost:15999',
  'VReplicationExec',
  'test-200',
  """insert into _vt.vreplication
  (db_name, source, pos, max_tps, max_replication_lag, tablet_types, time_updated, transaction_timestamp, state) values
  ('vt_keyspace', 'keyspace:"lookup" shard:"0" filter:<rules:<match:"uproduct" filter:"select * from product" > >', '', 99999, 99999, 'master', 0, 0, 'Running')""",
]
```

The first argument to the command is the master tablet id of the target
keyspace/shard for the VReplication stream.

The second argument is the SQL command. To start a new stream, you need
an insert statement.  The parameters are as follows:

* `db_name`: This name must match the name of the MySQL database. In the future, this
  will not be required, and will be automatically filled in by the vttablet.
* `source`: The protobuf representation of the stream source, explained below.
* `pos`: For a brand new stream, this should be empty. To start
  from a specific position, a flavor-encoded position must be specified. A
  typical position would look like this `MySQL56/ac6c45eb-71c2-11e9-92ea-0a580a1c1026:1-1296`.
* `max_tps`: 99999, reserved.
* `max_replication_lag`: 99999, reserved.
* `tablet_types`: specifies a comma separated list of tablet types to replicate from.
  If empty, the default tablet type specified by the `-vreplication_tablet_type`
  command line flag is used, which in turn defaults to 'REPLICA'.
* `time_updated`: 0, reserved.
* `transaction_timestamp`: 0, reserved.
* `state`: 'Init', 'Copying', 'Running', 'Stopped', 'Error'.
* `cell`: is an optional parameter that specifies the cell from which the stream
  can be sourced. If no cell is specified, the default is the local/current cell.

#### The source field

The source field is a proto-encoding of the following structure:

```
message BinlogSource {
  // the source keyspace
  string keyspace = 1;
  // the source shard
  string shard = 2;
  // list of filtering rules
  Filter filter = 6;
  // what to do if a DDL is encountered
  OnDDLAction on_ddl = 7;
}

message Filter {
  repeated Rule rules = 1;
}

message Rule {
  // match can be a table name or a regular expression
  // delineated by '/' and '/'.
  string match = 1;
  // filter can be an empty string or keyrange if the match
  // is a regular expression. Otherwise, it must be a select
  // query.
  string filter = 2;
}

enum OnDDLAction {
  IGNORE = 0;
  STOP = 1;
  EXEC = 2;
  EXEC_IGNORE = 3;
}
```

Here are some examples of proto encodings:

```
keyspace:"lookup" shard:"0" filter:<rules:<match:"uproduct" filter:"select * from product" > >
```

Meaning: copy and replicate all columns and rows of product from the source
table `lookup/0.product` to the `uproduct` table in target keyspace.

```
keyspace:"user" shard:"-80" filter:<rules:<match:"morder" filter:"select * from uorder where in_keyrange(mname, \\'unicode_loose_md5\\', \\'-80\\')" > >
```

The double-backslash for the strings inside the select will first be escaped by the python script,
which will cause the expression to internally be `\'unicode_loose_md5\'`. Since the entire
source is surrounded by single quotes when being sent as a value inside the outer insert statement,
the single `\` will escape the single quotes that follow. The final value in the source will
therefore be:


```
keyspace:"user" shard:"-80" filter:<rules:<match:"morder" filter:"select * from uorder where in_keyrange(mname, 'unicode_loose_md5', '-80')" > >
```

Meaning: copy and replicate all columns of the source table `user/-80.uorder`
where `unicode_loose_md5(mname)` is within `-80` keyrange, to the `morder`
table in the the target keyspace.

This particular stream generally wouldn't make sense in isolation. This would typically
be one of a set of four streams that combine to create a materialized view of `uorder`
from the `user` keyspace into the target (`merchant`) keyspace, but sharded by using
`mname` as the primary vindex. The vindex used would be `unicode_loose_md5` which should
also match the primary vindex of other tables in the target keyspace.

```
keyspace:"user" shard:"-80" filter:<rules:<match:"sales" filter:"select pid, count(*) as kount, sum(price) as amount from uorder group by pid" > >
```

Meaning: create a materialized view of `user/-80.uorder` into `sales` of the target
keyspace using the expression: `select pid, count(*) as kount, sum(price) as amount from uorder group by pid`.

This represents only one stream from source shard `-80`. Presumably, there will be one
more for the other `-80` shard.

#### The 'SELECT' features

The `SELECT` statement has the following features (and restrictions):

* The `SELECT` expressions can be any deterministic MySQL expression.
  Subqueries and joins are not supported. Among aggregate expressions, only
  `count(*)` and `sum(col)` are supported.
* The where clause can only contain the `in_keyrange` construct. It
  has two forms:
  * `in_keyrange('-80')`: The row's source keyrange matched against `-80`.
  * `in_keyrange(col, 'vindex_func', '-80')`: The keyrange is computed using
    the specified Vindex function as `vindex_func(col)` and matched against
    `-80`.
* `GROUP BY`: can be specified if using aggregations. The `GROUP BY`
  expressions are expected to cover the non-aggregated columns just
  like regular SQL requires.
* No other constructs like `ORDER BY`, `LIMIT`, etc. are allowed.

#### The pos field

For starting a brand new vreplication stream, the `pos` field must be empty.
The empty string signifies that there's no starting point for the vreplication.
This causes VReplication to copy the contents of the source table first, and then
start the replication.

For large tables, this is done in chunks. After each chunk is copied, replication
is resumed until it's caught up. VReplication ensures that only changes that affect
existing rows are applied. Following this another chunk is copied, and so on,
until all tables are completed. After that, replication runs indefinitely until
the VReplication stream is stopped or deleted.

#### It is a shared row

The `vreplication` table row is shared between the operator and Vreplication
itself.  Once the row is created, the VReplication stream
updates various fields of the row to save and report on its own status. For
example, the `pos` field is continuously updated as it makes forward progress.

While copying, the `state` field will be `Init` or `Copying`.

### Updating a stream

You can change any field of the stream by issuing a `VReplicationExec` with an
SQL `UPDATE` statement. You are required to specify the id of the row you
intend to update. You can only update one row at a time.

Typically, you can update the row and change the state to `Stopped` to stop a
stream, or to `Running` to restart a stopped stream.

You can also update the row to set a `stop_pos`, which will make the replication
stop once it reaches the specified position.

### Deleting a stream

You can delete a stream by issuing a `DELETE` statement. This will stop the replication
and delete the row. This statement is destructive. All data about the replication
state will be permanently deleted. Note that the target table will be left as-is,
potentially partially copied, and needs to be cleaned up separately, if desired.

## Other properties of VReplication

### Fast replay

VReplication has the capability to batch transactions if the send rate of the source
exceeds the replay rate of the destination.  This allows it to catch up very quickly
when there is a backlog. Load tests have shown a 3-20X improvement over traditional
MySQL replication depending on the workload.

### Accurate lag tracking

The source vttablet sends its current time along with every event. This allows the
target to correct for clock skew while estimating replication lag. Additionally,
the source starts sending heartbeats if there is nothing to send. If the target
receives no events from the source at all, it knows that it's definitely lagged
and starts reporting itself accordingly.

### Self-replication

VReplication allows you to set the source keyspace/shard to be the same as the target.
This is especially useful for performing schema rollouts: you can create the target
table with the intended schema and vreplicate from the source table to the new
target. Once caught up, you can cutover to write to the target table.
In this situation, an apply on
the target generates a binlog event that will be picked up by the source and
sent to the target. Typically, it will be an empty transaction. In such cases,
the target does not generally apply these transactions, because such an application
will generate yet another event. However, there are situations where one needs
to apply empty transactions, especially if it's a required stopping point.
VReplication can differentiate between these situations and apply events
only as needed.

### Deadlocks and lock wait timeouts

It is possible that multiple streams can conflict with each other and cause
deadlocks or lock waits. When such things happen, VReplication silently retries
such transactions without reporting an error. It does increment a counter so
that the frequency of such occurrences can be tracked.

### Automatic retries

If any other error is encountered, the replication is retried after a short wait.
Each time, the stream searches from the full list of available sources and picks
one at random.

### on\_ddl

The source specification allows you to specify a value for `on_ddl`. This allows
you to specify what to do with DDL SQL statements when they are encountered
in the replication stream from the source. The values can be as follows:

* `IGNORE`: Ignore all DDLs (this is also the default, if a value for `on_ddl`
  is not provided).
* `STOP`: Stop when DDL is encountered. This allows you to make any necessary
  changes to the target. Once changes are made, updating the state to `Running`
  will cause VReplication to continue from just after the point where it
  encountered the DDL.
* `EXEC`: Apply the DDL, but stop if an error is encountered while applying it.
* `EXEC_IGNORE`: Apply the DDL, but ignore any errors and continue replicating.

### Failover continuation

If a failover is performed on the target keyspace/shard, the new master will
automatically resume VReplication from where the previous master left off.

## Monitoring and troubleshooting

### VTTablet /debug/status

The first place to look at is the `/debug/status` page of the target master
vttablet. The bottom of the page shows the status of all the VReplication
streams.

Typically, if there is a problem, the `Last Message` column will display the
error. Sometimes, it's possible that the stream cannot find a source. If so,
the `Source Tablet` would be empty.

### VTTablet logfile

If the errors are not clear or if they keep disappearing, the VTTablet logfile
will contain information about what it's been doing with each stream.

### VReplicationExec select

The current status of the streams can also be fetched by issuing a
VReplicationExec command with `select * from _vt.vreplication`.

### Monitoring variables

VReplication also reports the following variables that can be scraped by
monitoring tools like prometheus:

* VReplicationStreamCount: Number of VReplication streams.
* VReplicationSecondsBehindMasterMax: Max vreplication seconds behind master.
* VReplicationSecondsBehindMaster: vreplication seconds behind master per stream.
* VReplicationSource: The source for each VReplication stream.
* VReplicationSourceTablet: The source tablet for each VReplication stream.

Thresholds and alerts can be set to draw attention to potential problems.

