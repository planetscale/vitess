---
title: vtctl Tablet Command Reference
series: vtctl
docs_nav_title: Tablets
---

The following `vtctl` commands are available for administering tablets.

## Commands

### InitTablet

Initializes a tablet in the topology.

#### Example

<pre class="command-example">InitTablet [-allow_update] [-allow_different_shard] [-allow_master_override] [-parent] [-db_name_override=&lt;db name&gt;] [-hostname=&lt;hostname&gt;] [-mysql_port=&lt;port&gt;] [-port=&lt;port&gt;] [-grpc_port=&lt;port&gt;] -keyspace=&lt;keyspace&gt; -shard=&lt;shard&gt; &lt;tablet alias&gt; &lt;tablet type&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| allow_master_override | Boolean | Use this flag to force initialization if a tablet is created as master, and a master for the keyspace/shard already exists. Use with caution. |
| allow_update | Boolean | Use this flag to force initialization if a tablet with the same name already exists. Use with caution. |
| db_name_override | string | Overrides the name of the database that the vttablet uses |
| grpc_port | Int | The gRPC port for the vttablet process |
| hostname | string | The server on which the tablet is running |
| keyspace | string | The keyspace to which this tablet belongs |
| mysql_host | string | The mysql host for the mysql server |
| mysql_port | Int | The mysql port for the mysql server |
| parent | Boolean | Creates the parent shard and keyspace if they don't yet exist |
| port | Int | The main port for the vttablet process |
| shard | string | The shard to which this tablet belongs |
| tags | string | A comma-separated list of key:value pairs that are used to tag the tablet |


#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.
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

* the <code>&lt;tablet alias&gt;</code> and <code>&lt;tablet type&gt;</code> arguments are both required for the <code>&lt;InitTablet&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.


### GetTablet

Outputs a JSON structure that contains information about the Tablet.

#### Example

<pre class="command-example">GetTablet &lt;tablet alias&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;GetTablet&gt;</code> command This error occurs if the command is not called with exactly one argument.


### IgnoreHealthError

Sets the regexp for health check errors to ignore on the specified tablet. The pattern has implicit ^$ anchors. Set to empty string or restart vttablet to stop ignoring anything.

#### Example

<pre class="command-example">IgnoreHealthError &lt;tablet alias&gt; &lt;ignore regexp&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.
* <code>&lt;ignore regexp&gt;</code> &ndash; Required.

#### Errors

* the <code>&lt;tablet alias&gt;</code> and <code>&lt;ignore regexp&gt;</code> arguments are required for the <code>&lt;IgnoreHealthError&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.

### UpdateTabletAddrs

Updates the IP address and port numbers of a tablet.

#### Example

<pre class="command-example">UpdateTabletAddrs [-hostname &lt;hostname&gt;] [-ip-addr &lt;ip addr&gt;] [-mysql-port &lt;mysql port&gt;] [-vt-port &lt;vt port&gt;] [-grpc-port &lt;grpc port&gt;] &lt;tablet alias&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| grpc-port | Int | The gRPC port for the vttablet process |
| hostname | string | The fully qualified host name of the server on which the tablet is running. |
| mysql-port | Int | The mysql port for the mysql daemon |
| mysql_host | string | The mysql host for the mysql server |
| vt-port | Int | The main port for the vttablet process |


#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;UpdateTabletAddrs&gt;</code> command This error occurs if the command is not called with exactly one argument.

### DeleteTablet

Deletes tablet(s) from the topology.

#### Example

<pre class="command-example">DeleteTablet [-allow_master] &lt;tablet alias&gt; ...</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| allow_master | Boolean | Allows for the master tablet of a shard to be deleted. Use with caution. |


#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>. To specify multiple values for this argument, separate individual values with a space.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument must be used to specify at least one tablet when calling the <code>&lt;DeleteTablet&gt;</code> command This error occurs if the command is not called with at least one argument.

### SetReadOnly

Sets the tablet as read-only.

#### Example

<pre class="command-example">SetReadOnly &lt;tablet alias&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;SetReadOnly&gt;</code> command This error occurs if the command is not called with exactly one argument.
* failed reading tablet %v: %v

### SetReadWrite

Sets the tablet as read-write.

#### Example

<pre class="command-example">SetReadWrite &lt;tablet alias&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;SetReadWrite&gt;</code> command This error occurs if the command is not called with exactly one argument.
* failed reading tablet %v: %v

### StartReplication

Starts replication on the specified tablet.

#### Example

<pre class="command-example">StartReplication &lt;tablet alias&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* action <code>&lt;StartReplication&gt;</code> requires <code>&lt;tablet alias&gt;</code> This error occurs if the command is not called with exactly one argument.
* failed reading tablet %v: %v

### StopReplication

Stops replication on the specified tablet.

#### Example

<pre class="command-example">StopReplication &lt;tablet alias&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* action <code>&lt;StopReplication&gt;</code> requires <code>&lt;tablet alias&gt;</code> This error occurs if the command is not called with exactly one argument.
* failed reading tablet %v: %v

### ChangeTabletType

Changes the db type for the specified tablet, if possible. This command is used primarily to arrange replicas, and it will not convert a master.<br><br>NOTE: This command automatically updates the serving graph.<br><br>

#### Example

<pre class="command-example">ChangeTabletType [-dry-run] &lt;tablet alias&gt; &lt;tablet type&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| dry-run | Boolean | Lists the proposed change without actually executing it |


#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.
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

* the <code>&lt;tablet alias&gt;</code> and <code>&lt;db type&gt;</code> arguments are required for the <code>&lt;ChangeTabletType&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.
* failed reading tablet %v: %v
* invalid type transition %v: %v -&gt;</code> %v

### Ping

hecks that the specified tablet is awake and responding to RPCs. This command can be blocked by other in-flight operations.

#### Example

<pre class="command-example">Ping &lt;tablet alias&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;Ping&gt;</code> command This error occurs if the command is not called with exactly one argument.

### RefreshState

Reloads the tablet record on the specified tablet.

#### Example

<pre class="command-example">RefreshState &lt;tablet alias&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;RefreshState&gt;</code> command This error occurs if the command is not called with exactly one argument.

### RefreshStateByShard

Runs 'RefreshState' on all tablets in the given shard.

#### Example

<pre class="command-example">RefreshStateByShard [-cells=c1,c2,...] &lt;keyspace/shard&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| cells | string | Specifies a comma-separated list of cells whose tablets are included. If empty, all cells are considered. |


#### Arguments

* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;keyspace/shard&gt;</code> argument is required for the <code>&lt;RefreshStateByShard&gt;</code> command This error occurs if the command is not called with exactly one argument.

### RunHealthCheck

Runs a health check on a remote tablet.

#### Example

<pre class="command-example">RunHealthCheck &lt;tablet alias&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;RunHealthCheck&gt;</code> command This error occurs if the command is not called with exactly one argument.

### IgnoreHealthError

Sets the regexp for health check errors to ignore on the specified tablet. The pattern has implicit ^$ anchors. Set to empty string or restart vttablet to stop ignoring anything.

#### Example

<pre class="command-example">IgnoreHealthError &lt;tablet alias&gt; &lt;ignore regexp&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.
* <code>&lt;ignore regexp&gt;</code> &ndash; Required.

#### Errors

* the <code>&lt;tablet alias&gt;</code> and <code>&lt;ignore regexp&gt;</code> arguments are required for the <code>&lt;IgnoreHealthError&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.

### Sleep

Blocks the action queue on the specified tablet for the specified amount of time. This is typically used for testing.

#### Example

<pre class="command-example">Sleep &lt;tablet alias&gt; &lt;duration&gt;</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.
* <code>&lt;duration&gt;</code> &ndash; Required. The amount of time that the action queue should be blocked. The value is a string that contains a possibly signed sequence of decimal numbers, each with optional fraction and a unit suffix, such as "300ms" or "1h45m". See the definition of the Go language's <a href="http://golang.org/pkg/time/#ParseDuration">ParseDuration</a> function for more details. Note that, in practice, the value should be a positively signed value.

#### Errors

* the <code>&lt;tablet alias&gt;</code> and <code>&lt;duration&gt;</code> arguments are required for the <code>&lt;Sleep&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.

### ExecuteHook

Runs the specified hook on the given tablet. A hook is a script that resides in the $VTROOT/vthook directory. You can put any script into that directory and use this command to run that script.<br><br>For this command, the param=value arguments are parameters that the command passes to the specified hook.

#### Example

<pre class="command-example">ExecuteHook &lt;tablet alias&gt; &lt;hook name&gt; [&lt;param1=value1&gt; &lt;param2=value2&gt; ...]</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.
* <code>&lt;hook name&gt;</code> &ndash; Required.
* <code>&lt;param1=value1&gt;</code> <code>&lt;param2=value2&gt;</code> . &ndash; Optional.

#### Errors

* the <code>&lt;tablet alias&gt;</code> and <code>&lt;hook name&gt;</code> arguments are required for the <code>&lt;ExecuteHook&gt;</code> command This error occurs if the command is not called with at least 2 arguments.

### ExecuteFetchAsApp

```
ExecuteFetchAsApp [-max_rows=10000] [-json] [-use_pool] <tablet alias> <sql command>
```

### ExecuteFetchAsDba

Runs the given SQL command as a DBA on the remote tablet.

#### Example

<pre class="command-example">ExecuteFetchAsDba [-max_rows=10000] [-disable_binlogs] [-json] &lt;tablet alias&gt; &lt;sql command&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| disable_binlogs | Boolean | Disables writing to binlogs during the query |
| json | Boolean | Output JSON instead of human-readable table |
| max_rows | Int | Specifies the maximum number of rows to allow in reset |
| reload_schema | Boolean | Indicates whether the tablet schema will be reloaded after executing the SQL command. The default value is <code>false</code>, which indicates that the tablet schema will not be reloaded. |


#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.
* <code>&lt;sql command&gt;</code> &ndash; Required.

#### Errors

* the <code>&lt;tablet alias&gt;</code> and <code>&lt;sql command&gt;</code> arguments are required for the <code>&lt;ExecuteFetchAsDba&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.

### VReplicationExec

```
VReplicationExec [-json] <tablet alias> <sql command>
```

### Backup

Stops mysqld and uses the BackupStorage service to store a new backup. This function also remembers if the tablet was replicating so that it can restore the same state after the backup completes.

#### Example

<pre class="command-example">Backup [-concurrency=4] &lt;tablet alias&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| concurrency | Int | Specifies the number of compression/checksum jobs to run simultaneously |


#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;Backup&gt;</code> command requires the <code>&lt;tablet alias&gt;</code> argument This error occurs if the command is not called with exactly one argument.


### ChangeSlaveType

Changes the db type for the specified tablet, if possible. This command is used primarily to arrange replicas, and it will not convert a master.<br><br>NOTE: This command automatically updates the serving graph.<br><br>

#### Example

<pre class="command-example">ChangeSlaveType [-dry-run] &lt;tablet alias&gt; &lt;tablet type&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| dry-run | Boolean | Lists the proposed change without actually executing it |


### RestoreFromBackup

Stops mysqld and restores the data from the latest backup.

#### Example

<pre class="command-example">RestoreFromBackup &lt;tablet alias&gt;</pre>

#### Errors

* the <code>&lt;RestoreFromBackup&gt;</code> command requires the <code>&lt;tablet alias&gt;</code> argument This error occurs if the command is not called with exactly one argument.


### ReparentTablet

Reparent a tablet to the current master in the shard. This only works if the current replication position matches the last known reparent action.

#### Example

<pre class="command-example">ReparentTablet &lt;tablet alias&gt;</pre>

#### Errors

* action <code>&lt;ReparentTablet&gt;</code> requires <code>&lt;tablet alias&gt;</code> This error occurs if the command is not called with exactly one argument.
* active reparent commands disabled (unset the -disable_active_reparents flag to enable)


## See Also

* [vtctl command index](../../vtctl)
