---
title: vtctl Generic Command Reference
series: vtctl
docs_nav_title: Generic Commands
---

The following generic `vtctl` commands are available for administering Vitess.

## Commands

### Validate

Validates that all nodes reachable from the global replication graph and that all tablets in all discoverable cells are consistent.

#### Example

<pre class="command-example">Validate [-ping-tablets]</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| ping-tablets | Boolean | Indicates whether all tablets should be pinged during the validation process |


### ListAllTablets

Lists all tablets in an awk-friendly way.

#### Example

<pre class="command-example">ListAllTablets &lt;cell name&gt;</pre>

#### Arguments

* <code>&lt;cell name&gt;</code> &ndash; Required. A cell is a location for a service. Generally, a cell resides in only one cluster. In Vitess, the terms "cell" and "data center" are interchangeable. The argument value is a string that does not contain whitespace.

#### Errors

* the <code>&lt;cell name&gt;</code> argument is required for the <code>&lt;ListAllTablets&gt;</code> command This error occurs if the command is not called with exactly one argument.

### ListTablets

Lists specified tablets in an awk-friendly way.

#### Example

<pre class="command-example">ListTablets &lt;tablet alias&gt; ...</pre>

#### Arguments

* <code>&lt;tablet alias&gt;</code> &ndash; Required. A Tablet Alias uniquely identifies a vttablet. The argument value is in the format <code>&lt;cell name&gt;-&lt;uid&gt;</code>. To specify multiple values for this argument, separate individual values with a space.

#### Errors

* the <code>&lt;tablet alias&gt;</code> argument is required for the <code>&lt;ListTablets&gt;</code> command This error occurs if the command is not called with at least one argument.

### Help

Provides help for a command.

#### Example

```
Help [command name]
```

## See Also

* [vtctl command index](../../vtctl)
