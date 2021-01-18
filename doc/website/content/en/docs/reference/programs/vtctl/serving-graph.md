---
title: vtctl Serving Graph Command Reference
series: vtctl
docs_nav_title: Serving Graph
---

The following `vtctl` commands are available for administering the Serving Graph.

## Commands

### GetSrvKeyspaceNames

Outputs a list of keyspace names.

#### Example

<pre class="command-example">GetSrvKeyspaceNames &lt;cell&gt;</pre>

#### Arguments

* <code>&lt;cell&gt;</code> &ndash; Required. A cell is a location for a service. Generally, a cell resides in only one cluster. In Vitess, the terms "cell" and "data center" are interchangeable. The argument value is a string that does not contain whitespace.

#### Errors

* the <code>&lt;cell&gt;</code> argument is required for the <code>&lt;GetSrvKeyspaceNames&gt;</code> command This error occurs if the command is not called with exactly one argument.


### GetSrvKeyspace

Outputs a JSON structure that contains information about the SrvKeyspace.

#### Example

<pre class="command-example">GetSrvKeyspace &lt;cell&gt; &lt;keyspace&gt;</pre>

#### Arguments

* <code>&lt;cell&gt;</code> &ndash; Required. A cell is a location for a service. Generally, a cell resides in only one cluster. In Vitess, the terms "cell" and "data center" are interchangeable. The argument value is a string that does not contain whitespace.
* <code>&lt;keyspace&gt;</code> &ndash; Required. The name of a sharded database that contains one or more tables. Vitess distributes keyspace shards into multiple machines and provides an SQL interface to query the data. The argument value must be a string that does not contain whitespace.

#### Errors

* the <code>&lt;cell&gt;</code> and <code>&lt;keyspace&gt;</code> arguments are required for the <code>&lt;GetSrvKeyspace&gt;</code> command This error occurs if the command is not called with exactly 2 arguments.

### GetSrvVSchema

Outputs a JSON structure that contains information about the SrvVSchema.

#### Example

<pre class="command-example">GetSrvVSchema &lt;cell&gt;</pre>

#### Arguments

* <code>&lt;cell&gt;</code> &ndash; Required. A cell is a location for a service. Generally, a cell resides in only one cluster. In Vitess, the terms "cell" and "data center" are interchangeable. The argument value is a string that does not contain whitespace.

#### Errors

* the <code>&lt;cell&gt;</code> argument is required for the <code>&lt;GetSrvVSchema&gt;</code> command This error occurs if the command is not called with exactly one argument.

### DeleteSrvVSchema

```
DeleteSrvVSchema  <cell>
```


## See Also

* [vtctl command index](../../vtctl)
