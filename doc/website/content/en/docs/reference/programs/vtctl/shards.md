---
title: vtctl Shard Command Reference
series: vtctl
docs_nav_title: Shards
---

The following `vtctl` commands are available for administering shards.

## Commands

### CreateShard

Creates the specified shard.

#### Example

<pre class="command-example">CreateShard [-force] [-parent] &lt;keyspace/shard&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| force | Boolean | Proceeds with the command even if the keyspace already exists |
| parent | Boolean | Creates the parent keyspace if it doesn't already exist |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> argument is required for the <code>&lt;CreateShard&gt;</code> command This error occurs if the command is not called with exactly one argument.

### GetShard

Outputs a JSON structure that contains information about the Shard.

#### Example

<pre class="command-example">GetShard &lt;keyspace/shard&gt;</pre>

#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> argument is required for the <code>&lt;GetShard&gt;</code> command This error occurs if the command is not called with exactly one argument.

### ValidateShard

Validates that all nodes that are reachable from this shard are consistent.

#### Example

<pre class="command-example">ValidateShard [-ping-tablets] &lt;keyspace/shard&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| ping-tablets | Boolean | Indicates whether all tablets should be pinged during the validation process |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> argument is required for the <code>&lt;ValidateShard&gt;</code> command This error occurs if the command is not called with exactly one argument.

### ShardReplicationPositions

Shows the replication status of each replica machine in the shard graph. In this case, the status refers to the replication lag between the master vttablet and the replica vttablet. In Vitess, data is always written to the master vttablet first and then replicated to all replica vttablets. Output is sorted by tablet type, then replication position. Use ctrl-C to interrupt command and see partial result if needed.

#### Example

<pre class="command-example">ShardReplicationPositions &lt;keyspace/shard&gt;</pre>

#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> argument is required for the <code>&lt;ShardReplicationPositions&gt;</code> command This error occurs if the command is not called with exactly one argument.


### ListShardTablets

Lists all tablets in the specified shard.

#### Example

<pre class="command-example">ListShardTablets &lt;keyspace/shard&gt;</pre>

#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> argument is required for the <code>&lt;ListShardTablets&gt;</code> command This error occurs if the command is not called with exactly one argument.

### SetShardIsMasterServing

```
SetShardIsMasterServing <keyspace/shard> <is_master_serving>
```

### SetShardTabletControl

Sets the TabletControl record for a shard and type. Only use this for an emergency fix or after a finished vertical split. The *MigrateServedFrom* and *MigrateServedType* commands set this field appropriately already. Always specify the blacklisted_tables flag for vertical splits, but never for horizontal splits.<br><br>To set the DisableQueryServiceFlag, keep 'blacklisted_tables' empty, and set 'disable_query_service' to true or false. Useful to fix horizontal splits gone wrong.<br><br>To change the blacklisted tables list, specify the 'blacklisted_tables' parameter with the new list. Useful to fix tables that are being blocked after a vertical split.<br><br>To just remove the ShardTabletControl entirely, use the 'remove' flag, useful after a vertical split is finished to remove serving restrictions.

#### Example

<pre class="command-example">SetShardTabletControl [--cells=c1,c2,...] [--blacklisted_tables=t1,t2,...] [--remove] [--disable_query_service] &lt;keyspace/shard&gt; &lt;tablet type&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| blacklisted_tables | string | Specifies a comma-separated list of tables to blacklist (used for vertical split). Each is either an exact match, or a regular expression of the form '/regexp/'. |
| cells | string | Specifies a comma-separated list of cells to update |
| disable_query_service | Boolean | Disables query service on the provided nodes. This flag requires 'blacklisted_tables' and 'remove' to be unset, otherwise it's ignored. |
| remove | Boolean | Removes cells for vertical splits. |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.
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

* the <code>&lt;keyspace/shard&gt;</code> and <code>&lt;tablet type&gt;</code> arguments are both required for the <code>&lt;SetShardTabletControl&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.

### UpdateSrvKeyspacePartition

```
UpdateSrvKeyspacePartition [--cells=c1,c2,...] [--remove] <keyspace/shard> <tablet type>
```

### SourceShardDelete

Deletes the SourceShard record with the provided index. This is meant as an emergency cleanup function. It does not call RefreshState for the shard master.

#### Example

<pre class="command-example">SourceShardDelete &lt;keyspace/shard&gt; &lt;uid&gt;</pre>

#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.
* <code>&lt;uid&gt;</code> &ndash; Required.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> and <code>&lt;uid&gt;</code> arguments are both required for the <code>&lt;SourceShardDelete&gt;</code> command This error occurs if the command is not called with at least 2 arguments.

### SourceShardAdd

Adds the SourceShard record with the provided index. This is meant as an emergency function. It does not call RefreshState for the shard master.

#### Example

<pre class="command-example">SourceShardAdd [--key_range=&lt;keyrange&gt;] [--tables=&lt;table1,table2,...&gt;] &lt;keyspace/shard&gt; &lt;uid&gt; &lt;source keyspace/shard&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| key_range | string | Identifies the key range to use for the SourceShard |
| tables | string | Specifies a comma-separated list of tables to replicate (used for vertical split). Each is either an exact match, or a regular expression of the form /regexp/ |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.
* <code>&lt;uid&gt;</code> &ndash; Required.
* <code>&lt;source keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code>, <code>&lt;uid&gt;</code>, and <code>&lt;source keyspace/shard&gt;</code> arguments are all required for the <code>&lt;SourceShardAdd&gt;</code> command This error occurs if the command is not called with exactly 3 arguments.

### ShardReplicationFix

Walks through a ShardReplication object and fixes the first error that it encounters.

#### Example

<pre class="command-example">ShardReplicationFix &lt;cell&gt; &lt;keyspace/shard&gt;</pre>

#### Arguments

* <code>&lt;cell&gt;</code> &ndash; Required. A cell is a location for a service. Generally, a cell resides in only one cluster. In Vitess, the terms "cell" and "data center" are interchangeable. The argument value is a string that does not contain whitespace.
* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;cell&gt;</code> and <code>&lt;keyspace/shard&gt;</code> arguments are required for the ShardReplicationRemove command This error occurs if the command is not called with exactly 2 arguments.

### WaitForFilteredReplication

Blocks until the specified shard has caught up with the filtered replication of its source shard.

#### Example

<pre class="command-example">WaitForFilteredReplication [-max_delay &lt;max_delay, default 30s&gt;] &lt;keyspace/shard&gt;</pre>

#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> argument is required for the <code>&lt;WaitForFilteredReplication&gt;</code> command This error occurs if the command is not called with exactly one argument.

### RemoveShardCell

Removes the cell from the shard's Cells list.

#### Example

<pre class="command-example">RemoveShardCell [-force] [-recursive] &lt;keyspace/shard&gt; &lt;cell&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| force | Boolean | Proceeds even if the cell's topology service cannot be reached. The assumption is that you turned down the entire cell, and just need to update the global topo data. |
| recursive | Boolean | Also delete all tablets in that cell belonging to the specified shard. |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.
* <code>&lt;cell&gt;</code> &ndash; Required. A cell is a location for a service. Generally, a cell resides in only one cluster. In Vitess, the terms "cell" and "data center" are interchangeable. The argument value is a string that does not contain whitespace.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> and <code>&lt;cell&gt;</code> arguments are required for the <code>&lt;RemoveShardCell&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.

### DeleteShard

Deletes the specified shard(s). In recursive mode, it also deletes all tablets belonging to the shard. Otherwise, there must be no tablets left in the shard.

#### Example

<pre class="command-example">DeleteShard [-recursive] [-even_if_serving] &lt;keyspace/shard&gt; ...</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| even_if_serving | Boolean | Remove the shard even if it is serving. Use with caution. |
| recursive | Boolean | Also delete all tablets belonging to the shard. |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>. To specify multiple values for this argument, separate individual values with a space.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> argument must be used to identify at least one keyspace and shard when calling the <code>&lt;DeleteShard&gt;</code> command This error occurs if the command is not called with at least one argument.

### ListBackups

Lists all the backups for a shard.

#### Example

<pre class="command-example">ListBackups &lt;keyspace/shard&gt;</pre>

#### Errors

* action <code>&lt;ListBackups&gt;</code> requires <code>&lt;keyspace/shard&gt;</code> This error occurs if the command is not called with exactly one argument.

### BackupShard

```
BackupShard [-allow_master=false] <keyspace/shard>
```

### RemoveBackup

Removes a backup for the BackupStorage.

#### Example

<pre class="command-example">RemoveBackup &lt;keyspace/shard&gt; &lt;backup name&gt;</pre>

#### Arguments

* <code>&lt;backup name&gt;</code> &ndash; Required.

#### Errors

* action <code>&lt;RemoveBackup&gt;</code> requires <code>&lt;keyspace/shard&gt;</code> <code>&lt;backup name&gt;</code> This error occurs if the command is not called with exactly 2 arguments.

### InitShardMaster

Sets the initial master for a shard. Will make all other tablets in the shard replicas of the provided master. WARNING: this could cause data loss on an already replicating shard. PlannedReparentShard or EmergencyReparentShard should be used instead.

#### Example

<pre class="command-example">InitShardMaster [-force] [-wait_replicas_timeout=&lt;duration&gt;] &lt;keyspace/shard&gt; &lt;tablet alias&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| force | Boolean | will force the reparent even if the provided tablet is not a master or the shard master |
| wait_replicas_timeout | Duration | time to wait for replicas to catch up in reparenting |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.
* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* action <code>&lt;InitShardMaster&gt;</code> requires <code>&lt;keyspace/shard&gt;</code> <code>&lt;tablet alias&gt;</code> This error occurs if the command is not called with exactly 2 arguments.
* active reparent commands disabled (unset the -disable_active_reparents flag to enable)

### PlannedReparentShard

Reparents the shard to a new master that can either be explicitly specified,
or chosen by Vitess.  Both the existing master and new master need to be up
and running to use this command. If the existing master for the shard is
down, you should use [EmergencyReparentShard](#emergencyreparentshard) instead.

If the `new_master` flag is not provided, Vitess will try to automatically
choose a replica to promote to master, avoiding any replicas specified in
the `avoid_master` flag, if provided.  Note that Vitess **will not consider
any replicas outside the cell the current master is in for promotion**,
therefore you **must** pass the `new_master` flag if you need to promote
a replica in a different cell from the master.  In the automated selection
mode Vitess will prefer the most advanced replica for promotion, to minimize
failover time.

#### Example

<pre class="command-example">PlannedReparentShard -keyspace_shard=&lt;keyspace/shard&gt; [-new_master=&lt;tablet alias&gt;] [-avoid_master=&lt;tablet alias&gt;]</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| avoid_master | string | alias of a tablet that should not be the master, i.e. reparent to any replica other than this one |
| keyspace_shard | string | keyspace/shard of the shard that needs to be reparented |
| new_master | string | alias of a tablet that should be the new master |
| wait_replicas_timeout | Duration | time to wait for replicas to catch up in reparenting |


#### Errors

* action <code>&lt;PlannedReparentShard&gt;</code> requires -keyspace_shard=<code>&lt;keyspace/shard&gt;</code> [-new_master=<code>&lt;tablet alias&gt;</code>] [-avoid_master=<code>&lt;tablet alias&gt;</code>] This error occurs if the command is not called with exactly 0 arguments.
* active reparent commands disabled (unset the -disable_active_reparents flag to enable)
* cannot use legacy syntax and flags -<code>&lt;keyspace_shard&gt;</code> and -<code>&lt;new_master&gt;</code> for action <code>&lt;PlannedReparentShard&gt;</code> at the same time

### EmergencyReparentShard

Reparents the shard to the new master. Assumes the old master is dead and not responding.

#### Example

<pre class="command-example">EmergencyReparentShard -keyspace_shard=&lt;keyspace/shard&gt; -new_master=&lt;tablet alias&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| keyspace_shard | string | keyspace/shard of the shard that needs to be reparented |
| new_master | string | alias of a tablet that should be the new master |
| wait_replicas_timeout | Duration | time to wait for replicas to catch up in reparenting |


#### Errors

* action <code>&lt;EmergencyReparentShard&gt;</code> requires -keyspace_shard=<code>&lt;keyspace/shard&gt;</code> -new_master=<code>&lt;tablet alias&gt;</code> This error occurs if the command is not called with exactly 0 arguments.
* active reparent commands disabled (unset the -disable_active_reparents flag to enable)
* cannot use legacy syntax and flag -<code>&lt;new_master&gt;</code> for action <code>&lt;EmergencyReparentShard&gt;</code> at the same time


### TabletExternallyReparented

Changes metadata in the topology service to acknowledge a shard master change performed by an external tool. See [Reparenting](../../../../user-guides/configuration-advanced/reparenting/#external-reparenting) for more information.

#### Example

<pre class="command-example">TabletExternallyReparented &lt;tablet alias&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;TabletExternallyReparented&gt;</code> command This error occurs if the command is not called with exactly one argument.


## See Also

* [vtctl command index](../../vtctl)
