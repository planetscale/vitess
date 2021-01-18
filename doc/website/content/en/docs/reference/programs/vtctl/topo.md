---
title: vtctl Topo Command Reference
series: vtctl
docs_nav_title: Topology Service
---

The following `vtctl` commands are available for administering Topology Services.

## Commands

### TopoCat

Retrieves the file(s) at &lt;path&gt; from the topo service, and displays it. It can resolve wildcards, and decode the proto-encoded data.

#### Example

<pre class="command-example">TopoCat [-cell &lt;cell&gt;] [-decode_proto] [-long] &lt;path&gt; [&lt;path&gt;...]</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| cell | string | topology cell to cat the file from. Defaults to global cell. |
| decode_proto | Boolean | decode proto files and display them as text |
| long | Boolean | long listing. |


#### Arguments

* <code>&lt;cell&gt;</code> &ndash; Required. A cell is a location for a service. Generally, a cell resides in only one cluster. In Vitess, the terms "cell" and "data center" are interchangeable. The argument value is a string that does not contain whitespace.
* <code>&lt;path&gt;</code> &ndash; Required.
* <code>&lt;path&gt;</code>. &ndash; Optional.

#### Errors

* <code>&lt;TopoCat&gt;</code>: no path specified This error occurs if the command is not called with at least one argument.
* <code>&lt;TopoCat&gt;</code>: invalid wildcards: %v
* <code>&lt;TopoCat&gt;</code>: some paths had errors

### TopoCp

```
TopoCp  [-cell <cell>] [-to_topo] <src> <dst>
```


## See Also

* [vtctl command index](../../vtctl)
