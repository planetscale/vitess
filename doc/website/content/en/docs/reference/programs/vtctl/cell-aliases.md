---
title: vtctl Cell Aliases Command Reference
series: vtctl
docs_nav_title: Cell Aliases
---

The following `vtctl` commands are available for administering Cell Aliases.

## Commands

### AddCellsAlias

Defines a group of cells within which replica/rdonly traffic can be routed across cells. By default, Vitess does not allow traffic between replicas that are part of different cells. Between cells that are not in the same group (alias), only master traffic can be routed.


#### Example

<pre class="command-example">AddCellsAlias [-cells &lt;cell1,cell2,cell3&gt;] &lt;alias&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| cells | string | The list of cell names that are members of this alias. |


#### Arguments

* <code>&lt;alias&gt;</code> &ndash; Required. Alias name for this grouping.

#### Errors

* the <code>&lt;alias&gt;</code> argument is required for the <code>&lt;AddCellsAlias&gt;</code> command This error occurs if the command is not called with exactly one argument.

### UpdateCellsAlias

Updates the content of a CellAlias with the provided parameters. Empty values and intersections with other aliases are not supported. 

#### Example

<pre class="command-example">UpdateCellsAliases [-cells &lt;cell1,cell2,cell3&gt;] &lt;alias&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| cells | string |The list of cell names that are members of this alias. |


#### Arguments

* <code>&lt;alias&gt;</code> &ndash; Required. Alias name group to update.

#### Errors

* the <code>&lt;alias&gt;</code> argument is required for the <code>&lt;UpdateCellsAlias&gt;</code> command This error occurs if the command is not called with exactly one argument.

### DeleteCellsAlias

Deletes the CellsAlias for the provided alias. After deleting an alias, cells that were part of the group are not going to be able to route replica/rdonly traffic to the rest of the cells that were part of the grouping. 

#### Example

<pre class="command-example">DeleteCellsAlias &lt;alias&gt;</pre>

#### Errors

* the <code>&lt;alias&gt;</code> argument is required for the <code>&lt;DeleteCellsAlias&gt;</code> command This error occurs if the command is not called with exactly one argument.

### GetCellsAliases

Fetches in json format all the existent cells alias groups.

#### Example

<pre class="command-example">GetCellsAliases</pre>

## See Also

* [vtctl command index](../../vtctl)
