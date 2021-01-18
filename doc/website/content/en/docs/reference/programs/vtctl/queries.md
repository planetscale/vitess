---
title: vtctl Query Command Reference
series: vtctl
docs_nav_title: Queries
---

The following `vtctl` commands are available for administering queries.

## Commands

### VtGateExecute

Executes the given SQL query with the provided bound variables against the vtgate server.

#### Example

<pre class="command-example">VtGateExecute -server &lt;vtgate&gt; [-bind_variables &lt;JSON map&gt;] [-keyspace &lt;default keyspace&gt;] [-tablet_type &lt;tablet type&gt;] [-options &lt;proto text options&gt;] [-json] &lt;sql&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| json | Boolean | Output JSON instead of human-readable table |
| options | string | execute options values as a text encoded proto of the ExecuteOptions structure |
| server | string | VtGate server to connect to |
| target | string | keyspace:shard@tablet_type |


#### Arguments

* <code>&lt;vtgate&gt;</code> &ndash; Required.
* <code>&lt;sql&gt;</code> &ndash; Required.

#### Errors

* the <code>&lt;sql&gt;</code> argument is required for the <code>&lt;VtGateExecute&gt;</code> command This error occurs if the command is not called with exactly one argument.
* query commands are disabled (set the -enable_queries flag to enable)
* error connecting to vtgate '%v': %v
* Execute failed: %v

### VtTabletExecute

Executes the given query on the given tablet. -transaction_id is optional. Use VtTabletBegin to start a transaction.

#### Example

<pre class="command-example">VtTabletExecute [-username &lt;TableACL user&gt;] [-transaction_id &lt;transaction_id&gt;] [-options &lt;proto text options&gt;] [-json] &lt;tablet alias&gt; &lt;sql&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| json | Boolean | Output JSON instead of human-readable table |
| options | string | execute options values as a text encoded proto of the ExecuteOptions structure |
| transaction_id | Int | transaction id to use, if inside a transaction. |
| username | string | If set, value is set as immediate caller id in the request and used by vttablet for TableACL check |


#### Arguments

* <code>&lt;TableACL user&gt;</code> &ndash; Required.
* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.
* <code>&lt;sql&gt;</code> &ndash; Required.

#### Errors

* the <code>&lt;tablet_alias&gt;</code> and <code>&lt;sql&gt;</code> arguments are required for the <code>&lt;VtTabletExecute&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.
* query commands are disabled (set the -enable_queries flag to enable)
* cannot connect to tablet %v: %v
* Execute failed: %v

### VtTabletBegin

Starts a transaction on the provided server.

#### Example

<pre class="command-example">VtTabletBegin [-username &lt;TableACL user&gt;] &lt;tablet alias&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| username | string | If set, value is set as immediate caller id in the request and used by vttablet for TableACL check |


#### Arguments

* <code>&lt;TableACL user&gt;</code> &ndash; Required.
* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet_alias&gt;</code> argument is required for the <code>&lt;VtTabletBegin&gt;</code> command This error occurs if the command is not called with exactly one argument.
* query commands are disabled (set the -enable_queries flag to enable)
* cannot connect to tablet %v: %v
* Begin failed: %v

### VtTabletCommit

Commits the given transaction on the provided server.

#### Example

<pre class="command-example">VtTabletCommit [-username &lt;TableACL user&gt;] &lt;transaction_id&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| username | string | If set, value is set as immediate caller id in the request and used by vttablet for TableACL check |


#### Arguments

* <code>&lt;TableACL user&gt;</code> &ndash; Required.
* <code>&lt;transaction_id&gt;</code> &ndash; Required.

#### Errors

* the <code>&lt;tablet_alias&gt;</code> and <code>&lt;transaction_id&gt;</code> arguments are required for the <code>&lt;VtTabletCommit&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.
* query commands are disabled (set the -enable_queries flag to enable)
* cannot connect to tablet %v: %v

### VtTabletRollback

Rollbacks the given transaction on the provided server.

#### Example

<pre class="command-example">VtTabletRollback [-username &lt;TableACL user&gt;] &lt;tablet alias&gt; &lt;transaction_id&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| username | string | If set, value is set as immediate caller id in the request and used by vttablet for TableACL check |


#### Arguments

* <code>&lt;TableACL user&gt;</code> &ndash; Required.
* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.
* <code>&lt;transaction_id&gt;</code> &ndash; Required.

#### Errors

* the <code>&lt;tablet_alias&gt;</code> and <code>&lt;transaction_id&gt;</code> arguments are required for the <code>&lt;VtTabletRollback&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.
* query commands are disabled (set the -enable_queries flag to enable)
* cannot connect to tablet %v: %v

### VtTabletStreamHealth

Executes the StreamHealth streaming query to a vttablet process. Will stop after getting &lt;count&gt; answers.

#### Example

<pre class="command-example">VtTabletStreamHealth [-count &lt;count, default 1&gt;] &lt;tablet alias&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| count | Int | number of responses to wait for |


#### Arguments

* <code>&lt;count default 1&gt;</code> &ndash; Required.
* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;VtTabletStreamHealth&gt;</code> command This error occurs if the command is not called with exactly one argument.
* query commands are disabled (set the -enable_queries flag to enable)
* cannot connect to tablet %v: %v

## See Also

* [vtctl command index](../../vtctl)
