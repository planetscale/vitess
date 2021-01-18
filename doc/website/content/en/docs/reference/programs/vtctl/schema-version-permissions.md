---
title: vtctl Schema, Version, Permissions Command Reference
series: vtctl
docs_nav_title: Schema Versions & Permissions
---

The following `vtctl` commands are available for administering Schema, Versions and Permissions.

## Commands

### GetSchema

Displays the full schema for a tablet, or just the schema for the specified tables in that tablet.

#### Example

<pre class="command-example">GetSchema [-tables=&lt;table1&gt;,&lt;table2&gt;,...] [-exclude_tables=&lt;table1&gt;,&lt;table2&gt;,...] [-include-views] &lt;tablet alias&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| exclude_tables | string | Specifies a comma-separated list of tables to exclude. Each is either an exact match, or a regular expression of the form /regexp/ |
| include-views | Boolean | Includes views in the output |
| table_names_only | Boolean | Only displays table names that match |
| tables | string | Specifies a comma-separated list of tables for which we should gather information. Each is either an exact match, or a regular expression of the form /regexp/ |


#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;GetSchema&gt;</code> command This error occurs if the command is not called with exactly one argument.


### ReloadSchema

Reloads the schema on a remote tablet.

#### Example

<pre class="command-example">ReloadSchema &lt;tablet alias&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;ReloadSchema&gt;</code> command This error occurs if the command is not called with exactly one argument.

### ReloadSchemaShard

Reloads the schema on all the tablets in a shard.

#### Example

<pre class="command-example">ReloadSchemaShard [-concurrency=10] [-include_master=false] &lt;keyspace/shard&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| concurrency | Int | How many tablets to reload in parallel |
| include_master | Boolean | Include the master tablet |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> argument is required for the <code>&lt;ReloadSchemaShard&gt;</code> command This error occurs if the command is not called with exactly one argument.

### ReloadSchemaKeyspace

Reloads the schema on all the tablets in a keyspace.

#### Example

<pre class="command-example">ReloadSchemaKeyspace [-concurrency=10] [-include_master=false] &lt;keyspace&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| concurrency | Int | How many tablets to reload in parallel |
| include_master | Boolean | Include the master tablet(s) |


#### Arguments

* <code>&lt;keyspace&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace&gt;</code> argument is required for the <code>&lt;ReloadSchemaKeyspace&gt;</code> command This error occurs if the command is not called with exactly one argument.

### ValidateSchemaShard

Validates that the master schema matches all of the replicas.

#### Example

<pre class="command-example">ValidateSchemaShard [-exclude_tables=''] [-include-views] &lt;keyspace/shard&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| exclude_tables | string | Specifies a comma-separated list of tables to exclude. Each is either an exact match, or a regular expression of the form /regexp/ |
| include-views | Boolean | Includes views in the validation |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> argument is required for the <code>&lt;ValidateSchemaShard&gt;</code> command This error occurs if the command is not called with exactly one argument.

### ValidateSchemaKeyspace

Validates that the master schema from shard 0 matches the schema on all of the other tablets in the keyspace.

#### Example

<pre class="command-example">ValidateSchemaKeyspace [-exclude_tables=''] [-include-views] &lt;keyspace name&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| exclude_tables | string | Specifies a comma-separated list of tables to exclude. Each is either an exact match, or a regular expression of the form /regexp/ |
| include-views | Boolean | Includes views in the validation |


#### Arguments

* <code>&lt;keyspace name&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace name&gt;</code> argument is required for the <code>&lt;ValidateSchemaKeyspace&gt;</code> command This error occurs if the command is not called with exactly one argument.

### ApplySchema

Applies the schema change to the specified keyspace on every master, running in parallel on all shards. The changes are then propagated to replicas via replication. If -allow_long_unavailability is set, schema changes affecting a large number of rows (and possibly incurring a longer period of unavailability) will not be rejected.

#### Example

<pre class="command-example">ApplySchema [-allow_long_unavailability] [-wait_replicas_timeout=10s] {-sql=&lt;sql&gt; || -sql-file=&lt;filename&gt;} &lt;keyspace&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| allow_long_unavailability | Boolean | Allow large schema changes which incur a longer unavailability of the database. |
| sql | string | A list of semicolon-delimited SQL commands |
| sql-file | string | Identifies the file that contains the SQL commands |
| wait_replicas_timeout | Duration | The amount of time to wait for replicas to receive the schema change via replication. |


#### Arguments

* <code>&lt;keyspace&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace&gt;</code> argument is required for the command<code>&lt;ApplySchema&gt;</code> command This error occurs if the command is not called with exactly one argument.

### CopySchemaShard

Copies the schema from a source shard's master (or a specific tablet) to a destination shard. The schema is applied directly on the master of the destination shard, and it is propagated to the replicas through binlogs.

#### Example

<pre class="command-example">CopySchemaShard [-tables=&lt;table1&gt;,&lt;table2&gt;,...] [-exclude_tables=&lt;table1&gt;,&lt;table2&gt;,...] [-include-views] [-wait_replicas_timeout=10s] {&lt;source keyspace/shard&gt; || &lt;source tablet alias&gt;} &lt;destination keyspace/shard&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| exclude_tables | string | Specifies a comma-separated list of tables to exclude. Each is either an exact match, or a regular expression of the form /regexp/ |
| include-views | Boolean | Includes views in the output |
| tables | string | Specifies a comma-separated list of tables to copy. Each is either an exact match, or a regular expression of the form /regexp/ |
| wait_replicas_timeout | Duration | The amount of time to wait for replicas to receive the schema change via replication. |


#### Arguments

* <code>&lt;source tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.
* <code>&lt;destination keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;source keyspace/shard&gt;</code> and <code>&lt;destination keyspace/shard&gt;</code> arguments are both required for the <code>&lt;CopySchemaShard&gt;</code> command. Instead of the <code>&lt;source keyspace/shard&gt;</code> argument, you can also specify <code>&lt;tablet alias&gt;</code> which refers to a specific tablet of the shard in the source keyspace This error occurs if the command is not called with exactly 2 arguments.

### ValidateVersionShard

Validates that the master version matches all of the replicas.

#### Example

<pre class="command-example">ValidateVersionShard &lt;keyspace/shard&gt;</pre>

#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> argument is required for the <code>&lt;ValidateVersionShard&gt;</code> command This error occurs if the command is not called with exactly one argument.

### ValidateVersionKeyspace

Validates that the master version from shard 0 matches all of the other tablets in the keyspace.

#### Example

<pre class="command-example">ValidateVersionKeyspace &lt;keyspace name&gt;</pre>

#### Arguments

* <code>&lt;keyspace name&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace name&gt;</code> argument is required for the <code>&lt;ValidateVersionKeyspace&gt;</code> command This error occurs if the command is not called with exactly one argument.

### GetPermissions

Displays the permissions for a tablet.

#### Example

<pre class="command-example">GetPermissions &lt;tablet alias&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;GetPermissions&gt;</code> command This error occurs if the command is not called with exactly one argument.

### ValidatePermissionsShard

Validates that the master permissions match all the replicas.

#### Example

<pre class="command-example">ValidatePermissionsShard &lt;keyspace/shard&gt;</pre>

#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> argument is required for the <code>&lt;ValidatePermissionsShard&gt;</code> command This error occurs if the command is not called with exactly one argument.

### ValidatePermissionsKeyspace

Validates that the master permissions from shard 0 match those of all of the other tablets in the keyspace.

#### Example

<pre class="command-example">ValidatePermissionsKeyspace &lt;keyspace name&gt;</pre>

#### Arguments

* <code>&lt;keyspace name&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace name&gt;</code> argument is required for the <code>&lt;ValidatePermissionsKeyspace&gt;</code> command This error occurs if the command is not called with exactly one argument.

### GetVSchema

Displays the VTGate routing schema.

#### Example

<pre class="command-example">GetVSchema &lt;keyspace&gt;</pre>

#### Arguments

* <code>&lt;keyspace&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace&gt;</code> argument is required for the <code>&lt;GetVSchema&gt;</code> command This error occurs if the command is not called with exactly one argument.

### ApplyVSchema

Applies the VTGate routing schema to the provided keyspace. Shows the result after application.

#### Example

<pre class="command-example">ApplyVSchema {-vschema=&lt;vschema&gt; || -vschema_file=&lt;vschema file&gt;} [-cells=c1,c2,...] [-skip_rebuild] &lt;keyspace&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| cells | string | If specified, limits the rebuild to the cells, after upload. Ignored if skipRebuild is set. |
| skip_rebuild | Boolean | If set, do no rebuild the SrvSchema objects. |
| vschema | string | Identifies the VTGate routing schema |
| vschema_file | string | Identifies the VTGate routing schema file |


#### Arguments

* <code>&lt;keyspace&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace&gt;</code> argument is required for the <code>&lt;ApplyVSchema&gt;</code> command This error occurs if the command is not called with exactly one argument.
* either the <code>&lt;vschema&gt;</code> or <code>&lt;vschema&gt;</code>File flag must be specified when calling the <code>&lt;ApplyVSchema&gt;</code> command

### GetRoutingRules

```
GetRoutingRules  
```

### ApplyRoutingRules

```
ApplyRoutingRules  {-rules=<rules>
| -rules_file=<rules_file>} [-cells=c1,c2,...] [-skip_rebuild] [-dry-run]
```

### RebuildVSchemaGraph

Rebuilds the cell-specific SrvVSchema from the global VSchema objects in the provided cells (or all cells if none provided).

#### Example

<pre class="command-example">RebuildVSchemaGraph [-cells=c1,c2,...]</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| cells | string | Specifies a comma-separated list of cells to look for tablets |


#### Errors

* <code>&lt;RebuildVSchemaGraph&gt;</code> doesn't take any arguments This error occurs if the command is not called with exactly 0 arguments.

## See Also

* [vtctl command index](../../vtctl)
