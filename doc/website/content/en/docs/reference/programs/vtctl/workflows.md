---
title: vtctl Workflow Command Reference
series: vtctl
docs_nav_title: Workflows
---

The following `vtctl` commands are available for administering workflows.

## Commands

### WorkflowCreate

Creates the workflow with the provided parameters. The workflow is also started, unless -skip_start is specified.

#### Example

<pre class="command-example">WorkflowCreate [-skip_start] &lt;factoryName&gt; [parameters...]</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| skip_start | Boolean | If set, the workflow will not be started. |


#### Arguments

* <code>&lt;factoryName&gt;</code> &ndash; Required.

#### Errors

* the <code>&lt;factoryName&gt;</code> argument is required for the <code>&lt;WorkflowCreate&gt;</code> command This error occurs if the command is not called with at least one argument.
* no workflow.Manager registered


### WorkflowStart

Starts the workflow.

#### Example

<pre class="command-example">WorkflowStart &lt;uuid&gt;</pre>

#### Errors

* the <code>&lt;uuid&gt;</code> argument is required for the <code>&lt;WorkflowStart&gt;</code> command This error occurs if the command is not called with exactly one argument.
* no workflow.Manager registered


### WorkflowStop

Stops the workflow.

#### Example

<pre class="command-example">WorkflowStop &lt;uuid&gt;</pre>

#### Errors

* the <code>&lt;uuid&gt;</code> argument is required for the <code>&lt;WorkflowStop&gt;</code> command This error occurs if the command is not called with exactly one argument.
* no workflow.Manager registered

### WorkflowDelete

Deletes the finished or not started workflow.

#### Example

<pre class="command-example">WorkflowDelete &lt;uuid&gt;</pre>

#### Errors

* the <code>&lt;uuid&gt;</code> argument is required for the <code>&lt;WorkflowDelete&gt;</code> command This error occurs if the command is not called with exactly one argument.
* no workflow.Manager registered

### WorkflowWait
```
WorkflowWait  <uuid>
```

### WorkflowTree

Displays a JSON representation of the workflow tree.

#### Example

<pre class="command-example">WorkflowTree </pre>

#### Errors

* the <code>&lt;WorkflowTree&gt;</code> command takes no parameter This error occurs if the command is not called with exactly 0 arguments.
* no workflow.Manager registered

### WorkflowAction
```
WorkflowAction  <path> <name>
```


## See Also

* [vtctl command index](../../vtctl)
