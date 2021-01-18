---
title: vtctl Keyspace Command Reference
series: vtctl
docs_nav_title: Keyspaces
---

The following `vtctl` commands are available for administering Keyspaces.

## Commands

### CreateKeyspace

Creates the specified keyspace.

#### Example

<pre class="command-example">CreateKeyspace [-sharding_column_name=name] [-sharding_column_type=type] [-served_from=tablettype1:ks1,tablettype2,ks2,...] [-force] &lt;keyspace name&gt;
Creates the specified keyspace. keyspace_type can be NORMAL or SNAPSHOT. For a SNAPSHOT keyspace you must specify the name of a base_keyspace, and a snapshot_time in UTC, in RFC3339 time format, e.g. 2006-01-02T15:04:05+00:00</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| force | Boolean | Proceeds even if the keyspace already exists |
| served_from | string | Specifies a comma-separated list of dbtype:keyspace pairs used to serve traffic |
| sharding_column_name | string | Specifies the column to use for sharding operations |
| sharding_column_type | string | Specifies the type of the column to use for sharding operations |


#### Arguments

* <code>&lt;keyspace name&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace name&gt;</code> argument is required for the <code>&lt;CreateKeyspace&gt;</code> command This error occurs if the command is not called with exactly one argument.

### DeleteKeyspace

Deletes the specified keyspace. In recursive mode, it also recursively deletes all shards in the keyspace. Otherwise, there must be no shards left in the keyspace.

#### Example

<pre class="command-example">DeleteKeyspace [-recursive] &lt;keyspace&gt;
Deletes the specified keyspace. In recursive mode, it also recursively deletes all shards in the keyspace. Otherwise, there must be no shards left in the keyspace.</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| recursive | Boolean | Also recursively delete all shards in the keyspace. |


#### Arguments

* <code>&lt;keyspace&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* must specify the <code>&lt;keyspace&gt;</code> argument for <code>&lt;DeleteKeyspace&gt;</code> This error occurs if the command is not called with exactly one argument.

### RemoveKeyspaceCell

Removes the cell from the Cells list for all shards in the keyspace, and the SrvKeyspace for that keyspace in that cell.

#### Example

<pre class="command-example">RemoveKeyspaceCell [-force] [-recursive] &lt;keyspace&gt; &lt;cell&gt;
Removes the cell from the Cells list for all shards in the keyspace, and the SrvKeyspace for that keyspace in that cell.</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| force | Boolean | Proceeds even if the cell's topology service cannot be reached. The assumption is that you turned down the entire cell, and just need to update the global topo data. |
| recursive | Boolean | Also delete all tablets in that cell belonging to the specified keyspace. |


#### Arguments

* <code>&lt;keyspace&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.
* <code>&lt;cell&gt;</code> &ndash; Required. A cell is a location for a service. Generally, a cell resides in only one cluster. In Vitess, the terms "cell" and "data center" are interchangeable. The argument value is a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace&gt;</code> and <code>&lt;cell&gt;</code> arguments are required for the <code>&lt;RemoveKeyspaceCell&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.

### GetKeyspace

Outputs a JSON structure that contains information about the Keyspace.

#### Example

<pre class="command-example">GetKeyspace &lt;keyspace&gt;
Outputs a JSON structure that contains information about the Keyspace.</pre>

#### Arguments

* <code>&lt;keyspace&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace&gt;</code> argument is required for the <code>&lt;GetKeyspace&gt;</code> command This error occurs if the command is not called with exactly one argument.

### GetKeyspaces

Outputs a sorted list of all keyspaces.

#### Errors

* the <code>&lt;destination keyspace/shard&gt;</code> and <code>&lt;served tablet type&gt;</code> arguments are both required for the <code>&lt;MigrateServedFrom&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.

### SetKeyspaceShardingInfo

Updates the sharding information for a keyspace.

#### Example

<pre class="command-example">SetKeyspaceShardingInfo [-force] &lt;keyspace name&gt; [&lt;column name&gt;] [&lt;column type&gt;]
Updates the sharding information for a keyspace.</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| force | Boolean | Updates fields even if they are already set. Use caution before calling this command. |


#### Arguments

* <code>&lt;keyspace name&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.
* <code>&lt;column name&gt;</code> &ndash; Optional.
* <code>&lt;column type&gt;</code> &ndash; Optional.

#### Errors

* the <code>&lt;keyspace name&gt;</code> argument is required for the <code>&lt;SetKeyspaceShardingInfo&gt;</code> command. The <code>&lt;column name&gt;</code> and <code>&lt;column type&gt;</code> arguments are both optional This error occurs if the command is not called with between 1 and 3 arguments.
* both <code>&lt;column name&gt;</code> and <code>&lt;column type&gt;</code> must be set, or both must be unset


### SetKeyspaceServedFrom

Changes the ServedFromMap manually. This command is intended for emergency fixes. This field is automatically set when you call the [*MigrateServedFrom*](https://vitess.io/docs/reference/programs/vtctl/keyspaces/#migrateservedfrom) command. This command does not rebuild the serving graph.

#### Example

<pre class="command-example">SetKeyspaceServedFrom [-source=&lt;source keyspace name&gt;] [-remove] [-cells=c1,c2,...] &lt;keyspace name&gt; &lt;tablet type&gt;
Changes the ServedFromMap manually. This command is intended for emergency fixes. This field is automatically set when you call the *MigrateServedFrom* command. This command does not rebuild the serving graph.</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| cells | string | Specifies a comma-separated list of cells to affect |
| remove | Boolean | Indicates whether to add (default) or remove the served from record |
| source | string | Specifies the source keyspace name |


#### Arguments

* <code>&lt;keyspace name&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.
* <code>&lt;tablet type&gt;</code> &ndash; Required. The vttablet's role. Valid values are:

    * <code>backup</code> &ndash; A replicated copy of data that is offline to queries other than for backup purposes
    * <code>batch</code> &ndash; A replicated copy of data for OLAP load patterns (typically for MapReduce jobs)
    * <code>drained</code> &ndash; A tablet that is reserved for a background process. For example, a tablet used by a vtworker process, where the tablet is likely lagging in replication.
    * <code>experimental</code> &ndash; A replicated copy of data that is ready but not serving query traffic. The value indicates a special characteristic of the tablet that indicates the tablet should not be considered a potential master. Vitess also does not worry about lag for experimental tablets when reparenting.
    * <code>master</code> &ndash; A primary copy of data
    * <code>rdonly</code> &ndash; A replicated copy of data for OLAP load patterns
    * <code>replica</code> &ndash; A replicated copy of data ready to be promoted to master
    * <code>restore</code> &ndash; A tablet that is restoring from a snapshot. Typically, this happens at tablet startup, then it goes to its right state.
    * <code>spare</code> &ndash; A replicated copy of data that is ready but not serving query traffic. The data could be a potential master tablet.


#### Errors

* the <code>&lt;keyspace name&gt;</code> and <code>&lt;tablet type&gt;</code> arguments are required for the <code>&lt;SetKeyspaceServedFrom&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.

### RebuildKeyspaceGraph

Rebuilds the serving data for the keyspace. This command may trigger an update to all connected clients.

#### Example

<pre class="command-example">RebuildKeyspaceGraph [-cells=c1,c2,...] &lt;keyspace&gt; ...
Rebuilds the serving data for the keyspace. This command may trigger an update to all connected clients.</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| cells | string | Specifies a comma-separated list of cells to update |


#### Arguments

* <code>&lt;keyspace&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace. To specify multiple values for this argument, separate individual values with a space.

#### Errors

* the <code>&lt;keyspace&gt;</code> argument must be used to specify at least one keyspace when calling the <code>&lt;RebuildKeyspaceGraph&gt;</code> command This error occurs if the command is not called with at least one argument.


### ValidateKeyspace


Validates that all nodes reachable from the specified keyspace are consistent.

#### Example

<pre class="command-example">ValidateKeyspace [-ping-tablets] &lt;keyspace name&gt;
Validates that all nodes reachable from the specified keyspace are consistent.</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| ping-tablets | Boolean | Specifies whether all tablets will be pinged during the validation process |


#### Arguments

* <code>&lt;keyspace name&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace name&gt;</code> argument is required for the <code>&lt;ValidateKeyspace&gt;</code> command This error occurs if the command is not called with exactly one argument.


### Reshard
```shell
Reshard  [-skip_schema_copy] <keyspace.workflow> <source_shards> <target_shards>
Start a Resharding process. Example: Reshard -cells='zone1,alias1' -tablet_types='master,replica,rdonly'  ks.workflow001 '0' '-80,80-'.
```

### MoveTables
```shell
MoveTables  [-cell=<cell>] [-tablet_types=<source_tablet_types>] -workflow=<workflow> <source_keyspace> <target_keyspace> <table_specs>
Move table(s) to another keyspace, table_specs is a list of tables or the tables section of the vschema for the target keyspace. Example: '{"t1":{"column_vindexes": [{"column": "id1", "name": "hash"}]}, "t2":{"column_vindexes": [{"column": "id2", "name": "hash"}]}}'.  In the case of an unsharded target keyspace the vschema for each table may be empty. Example: '{"t1":{}, "t2":{}}'.
```

### DropSources
```shell
DropSources  [-dry_run] <keyspace.workflow>
After a MoveTables or Resharding workflow cleanup unused artifacts like source tables, source shards and blacklists.
```

### CreateLookupVindex
```shell
CreateLookupVindex  [-cell=<cell>] [-tablet_types=<source_tablet_types>] <keyspace> <json_spec>
Create and backfill a lookup vindex. the json_spec must contain the vindex and colvindex specs for the new lookup.
```

### ExternalizeVindex
```shell
ExternalizeVindex  <keyspace>.<vindex>
Externalize a backfilled vindex.
```

### Materialize
```shell
Materialize  <json_spec>, example : '{"workflow": "aaa", "source_keyspace": "source", "target_keyspace": "target", "table_settings": [{"target_table": "customer", "source_expression": "select * from customer", "create_ddl": "copy"}]}'
Performs materialization based on the json spec. Is used directly to form VReplication rules, with an optional step to copy table structure/DDL.
```

### SplitClone
```shell
SplitClone  <keyspace> <from_shards> <to_shards>
Start the SplitClone process to perform horizontal resharding. Example: SplitClone ks '0' '-80,80-'
```

### VerticalSplitClone
```shell
VerticalSplitClone  <from_keyspace> <to_keyspace> <tables>
Start the VerticalSplitClone process to perform vertical resharding. Example: SplitClone from_ks to_ks 'a,/b.*/'
```

### VDiff
```shell
VDiff  [-source_cell=<cell>] [-target_cell=<cell>] [-tablet_types=replica] [-filtered_replication_wait_time=30s] <keyspace.workflow>
Perform a diff of all tables in the workflow
```

### MigrateServedTypes

Migrates a serving type from the source shard to the shards that it replicates to. This command also rebuilds the serving graph. The &lt;keyspace/shard&gt; argument can specify any of the shards involved in the migration.

#### Example

<pre class="command-example">MigrateServedTypes [-cells=c1,c2,...] [-reverse] [-skip-refresh-state] &lt;keyspace/shard&gt; &lt;served tablet type&gt;
Migrates a serving type from the source shard to the shards that it replicates to. This command also rebuilds the serving graph. The <keyspace/shard> argument can specify any of the shards involved in the migration.</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| cells | string | Specifies a comma-separated list of cells to update |
| filtered\_replication\_wait\_time | Duration | Specifies the maximum time to wait, in seconds, for filtered replication to catch up on master migrations |
| reverse | Boolean | Moves the served tablet type backward instead of forward. Use in case of trouble |
| skip-refresh-state | Boolean | Skips refreshing the state of the source tablets after the migration, meaning that the refresh will need to be done manually, replica and rdonly only) |
| reverse\_replication | Boolean | For master migration, enabling this flag reverses replication which allows you to rollback |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.
* <code>&lt;served tablet type&gt;</code> &ndash; Required. The vttablet's role. Valid values are:

    * <code>backup</code> &ndash; A replicated copy of data that is offline to queries other than for backup purposes
    * <code>batch</code> &ndash; A replicated copy of data for OLAP load patterns (typically for MapReduce jobs)
    * <code>drained</code> &ndash; A tablet that is reserved for a background process. For example, a tablet used by a vtworker process, where the tablet is likely lagging in replication.
    * <code>experimental</code> &ndash; A replicated copy of data that is ready but not serving query traffic. The value indicates a special characteristic of the tablet that indicates the tablet should not be considered a potential master. Vitess also does not worry about lag for experimental tablets when reparenting.
    * <code>master</code> &ndash; A primary copy of data
    * <code>rdonly</code> &ndash; A replicated copy of data for OLAP load patterns
    * <code>replica</code> &ndash; A replicated copy of data ready to be promoted to master
    * <code>restore</code> &ndash; A tablet that is restoring from a snapshot. Typically, this happens at tablet startup, then it goes to its right state.
    * <code>spare</code> &ndash; A replicated copy of data that is ready but not serving query traffic. The data could be a potential master tablet.


#### Errors

* the <code>&lt;source keyspace/shard&gt;</code> and <code>&lt;served tablet type&gt;</code> arguments are both required for the <code>&lt;MigrateServedTypes&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.
* the <code>&lt;skip-refresh-state&gt;</code> flag can only be specified for non-master migrations


### MigrateServedFrom

Makes the &lt;destination keyspace/shard&gt; serve the given type. This command also rebuilds the serving graph.

#### Example

<pre class="command-example">MigrateServedFrom [-cells=c1,c2,...] [-reverse] &lt;destination keyspace/shard&gt; &lt;served tablet type&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| cells | string | Specifies a comma-separated list of cells to update |
| filtered_replication_wait_time | Duration | Specifies the maximum time to wait, in seconds, for filtered replication to catch up on master migrations |
| reverse | Boolean | Moves the served tablet type backward instead of forward. Use in case of trouble |


#### Arguments

* <code>&lt;destination keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.
* <code>&lt;served tablet type&gt;</code> &ndash; Required. The vttablet's role. Valid values are:

    * <code>backup</code> &ndash; A replicated copy of data that is offline to queries other than for backup purposes
    * <code>batch</code> &ndash; A replicated copy of data for OLAP load patterns (typically for MapReduce jobs)
    * <code>drained</code> &ndash; A tablet that is reserved for a background process. For example, a tablet used by a vtworker process, where the tablet is likely lagging in replication.
    * <code>experimental</code> &ndash; A replicated copy of data that is ready but not serving query traffic. The value indicates a special characteristic of the tablet that indicates the tablet should not be considered a potential master. Vitess also does not worry about lag for experimental tablets when reparenting.
    * <code>master</code> &ndash; A primary copy of data
    * <code>rdonly</code> &ndash; A replicated copy of data for OLAP load patterns
    * <code>replica</code> &ndash; A replicated copy of data ready to be promoted to master
    * <code>restore</code> &ndash; A tablet that is restoring from a snapshot. Typically, this happens at tablet startup, then it goes to its right state.
    * <code>spare</code> &ndash; A replicated copy of data that is ready but not serving query traffic. The data could be a potential master tablet.


#### Errors

* the <code>&lt;destination keyspace/shard&gt;</code> and <code>&lt;served tablet type&gt;</code> arguments are both required for the <code>&lt;MigrateServedFrom&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.

### SwitchReads
```shell
SwitchReads  [-cells=c1,c2,...] [-reverse] -tablet_type={replica|rdonly} [-dry-run] <keyspace.workflow>
Switch read traffic for the specified workflow.
```

### SwitchWrites
```shell
SwitchWrites  [-filtered_replication_wait_time=30s] [-cancel] [-reverse_replication=false] [-dry-run] <keyspace.workflow>
Switch write traffic for the specified workflow.
```

### CancelResharding

Permanently cancels a resharding in progress. All resharding related metadata will be deleted.

#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

### ShowResharding

Displays all metadata about a resharding in progress.

#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.


### FindAllShardsInKeyspace

Displays all of the shards in the specified keyspace.

#### Example

<pre class="command-example">FindAllShardsInKeyspace &lt;keyspace&gt;
Displays all of the shards in the specified keyspace.</pre>

#### Arguments

* <code>&lt;keyspace&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace&gt;</code> argument is required for the <code>&lt;FindAllShardsInKeyspace&gt;</code> command This error occurs if the command is not called with exactly one argument.


### WaitForDrain

Blocks until no new queries were observed on all tablets with the given tablet type in the specified keyspace.  This can be used as sanity check to ensure that the tablets were drained after running vtctl MigrateServedTypes  and vtgate is no longer using them. If -timeout is set, it fails when the timeout is reached.

#### Example

<pre class="command-example">WaitForDrain [-timeout &lt;duration&gt;] [-retry_delay &lt;duration&gt;] [-initial_wait &lt;duration&gt;] &lt;keyspace/shard&gt; &lt;served tablet type&gt;
Blocks until no new queries were observed on all tablets with the given tablet type in the specified keyspace. This can be used as sanity check to ensure that the tablets were drained after running vtctl MigrateServedTypes and vtgate is no longer using them. If -timeout is set, it fails when the timeout is reached."</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| cells | string | Specifies a comma-separated list of cells to look for tablets |
| initial_wait | Duration | Time to wait for all tablets to check in |
| retry_delay | Duration | Time to wait between two checks |
| timeout | Duration | Timeout after which the command fails |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.
* <code>&lt;served tablet type&gt;</code> &ndash; Required. The vttablet's role. Valid values are:

    * <code>backup</code> &ndash; A replicated copy of data that is offline to queries other than for backup purposes
    * <code>batch</code> &ndash; A replicated copy of data for OLAP load patterns (typically for MapReduce jobs)
    * <code>drained</code> &ndash; A tablet that is reserved for a background process. For example, a tablet used by a vtworker process, where the tablet is likely lagging in replication.
    * <code>experimental</code> &ndash; A replicated copy of data that is ready but not serving query traffic. The value indicates a special characteristic of the tablet that indicates the tablet should not be considered a potential master. Vitess also does not worry about lag for experimental tablets when reparenting.
    * <code>master</code> &ndash; A primary copy of data
    * <code>rdonly</code> &ndash; A replicated copy of data for OLAP load patterns
    * <code>replica</code> &ndash; A replicated copy of data ready to be promoted to master
    * <code>restore</code> &ndash; A tablet that is restoring from a snapshot. Typically, this happens at tablet startup, then it goes to its right state.
    * <code>spare</code> &ndash; A replicated copy of data that is ready but not serving query traffic. The data could be a potential master tablet.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> and <code>&lt;tablet type&gt;</code> arguments are both required for the <code>&lt;WaitForDrain&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.


## See Also

* [vtctl command index](../../vtctl)
