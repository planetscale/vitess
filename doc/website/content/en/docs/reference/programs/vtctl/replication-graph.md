---
title: vtctl Replication Graph Command Reference
series: vtctl
docs_nav_title: Replication Graph
---

The following `vtctl` commands are available for administering the Replication Graph.

## Commands

### GetShardReplication

Outputs a JSON structure that contains information about the ShardReplication.

#### Example

<pre class="command-example">GetShardReplication &lt;cell&gt; &lt;keyspace/shard&gt;</pre>

#### Arguments

* <code>&lt;cell&gt;</code> &ndash; Required. A cell is a location for a service. Generally, a cell resides in only one cluster. In Vitess, the terms "cell" and "data center" are interchangeable. The argument value is a string that does not contain whitespace.
* <code>&lt;keyspace/shard&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables as well as the shard associated with the command. The keyspace must be identified by a string that does not contain whitespace, while the shard is typically identified by a string in the format <code>&lt;range start&gt;-&lt;range end&gt;</code>.

#### Errors

* the <code>&lt;cell&gt;</code> and <code>&lt;keyspace/shard&gt;</code> arguments are required for the <code>&lt;GetShardReplication&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.

## See Also

* [vtctl command index](../../vtctl)
