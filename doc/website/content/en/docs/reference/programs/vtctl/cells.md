---
title: vtctl Cell Command Reference
series: vtctl
docs_nav_title: Cells
---

The following `vtctl` commands are available for administering Cells.

## Commands

### AddCellInfo

Registers a local topology service in a new cell by creating the CellInfo with the provided parameters. The address will be used to connect to the topology service, and we'll put Vitess data starting at the provided root.

#### Example

<pre class="command-example">AddCellInfo [-server_address &lt;addr&gt;] [-root &lt;root&gt;] &lt;cell&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| root | string | The root path the topology service is using for that cell. |
| server_address | string | The address the topology service is using for that cell. |


#### Arguments

* <code>&lt;addr&gt;</code> &ndash; Required.
* <code>&lt;cell&gt;</code> &ndash; Required. A cell is a location for a service. Generally, a cell resides in only one cluster. In Vitess, the terms "cell" and "data center" are interchangeable. The argument value is a string that does not contain whitespace.

#### Errors

* the <code>&lt;cell&gt;</code> argument is required for the <code>&lt;AddCellInfo&gt;</code> command This error occurs if the command is not called with exactly one argument.

### DeleteCellInfo

Deletes the CellInfo for the provided cell. The cell cannot be referenced by any Shard record.

#### Example

<pre class="command-example">DeleteCellInfo &lt;cell&gt;</pre>

#### Errors

* the <code>&lt;cell&gt;</code> argument is required for the <code>&lt;DeleteCellInfo&gt;</code> command This error occurs if the command is not called with exactly one argument.


### GetCellInfo

Prints a JSON representation of the CellInfo for a cell.

#### Example

<pre class="command-example">GetCellInfo &lt;cell&gt;</pre>

#### Errors

* the <code>&lt;cell&gt;</code> argument is required for the <code>&lt;GetCellInfo&gt;</code> command This error occurs if the command is not called with exactly one argument.

### GetCellInfoNames

Lists all the cells for which we have a CellInfo object, meaning we have a local topology service registered.

#### Example

<pre class="command-example">GetCellInfoNames </pre>

#### Errors

* <code>&lt;GetCellInfoNames&gt;</code> command takes no parameter This error occurs if the command is not called with exactly 0 arguments.


### UpdateCellInfo

Updates the content of a CellInfo with the provided parameters. If a value is empty, it is not updated. The CellInfo will be created if it doesn't exist.

#### Example

<pre class="command-example">UpdateCellInfo [-server_address &lt;addr&gt;] [-root &lt;root&gt;] &lt;cell&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| root | string | The root path the topology service is using for that cell. |
| server_address | string | The address the topology service is using for that cell. |


#### Arguments

* <code>&lt;addr&gt;</code> &ndash; Required.
* <code>&lt;cell&gt;</code> &ndash; Required. A cell is a location for a service. Generally, a cell resides in only one cluster. In Vitess, the terms "cell" and "data center" are interchangeable. The argument value is a string that does not contain whitespace.

#### Errors

* the <code>&lt;cell&gt;</code> argument is required for the <code>&lt;UpdateCellInfo&gt;</code> command This error occurs if the command is not called with exactly one argument.

### GetCellInfo

Prints a JSON representation of the CellInfo for a cell.

#### Example

<pre class="command-example">GetCellInfo &lt;cell&gt;</pre>

#### Errors

* the <code>&lt;cell&gt;</code> argument is required for the <code>&lt;GetCellInfo&gt;</code> command This error occurs if the command is not called with exactly one argument.


## See Also

* [vtctl command index](../../vtctl)
