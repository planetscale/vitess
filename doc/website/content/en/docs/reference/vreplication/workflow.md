---
title: Workflow
description: Wrapper on VExec to perform common actions on a workflow
weight: 110
---

### Command

```
Workflow  [-dry_run] <keyspace[.workflow]> <action>
```

### Description

Workflow is a convenience command for useful actions on a workflow that you can use instead of 
actually specifying a query to VExec.

### Parameters

#### -dry-run 
**optional**\
**default** false

<div class="cmd">
You can do a dry run where no actual action is taken but the command logs all the actions that would be taken
by SwitchReads.
</div>

#### keyspace.workflow 
**mandatory**

<div class="cmd">
Name of target keyspace and the associated workflow to SwitchWrites for.
</div>

#### action 
**mandatory**

<div class="cmd">
action is one of

* **stop**: sets the state of the workflow to Stopped: no further vreplication will happen until workflow is restarted
* **start**: restarts a Stopped workflows
* **delete**: removes the entries for this workflow in \_vt.vreplication
* **show**: returns a JSON object with details about the associated shards and also with all the columns
    from the \_vt.vreplication table
* **listall**: returns a comma separated list of all *running* workflows in a keyspace
</div>

#### Example
```
vtctlclient  Workflow keyspace1.workflow1 stop
vtctlclient  Workflow keyspace1.workflow1 show
vtctlclient  Workflow keyspace1 listall
```
