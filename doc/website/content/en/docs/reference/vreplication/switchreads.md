---
title: SwitchReads
description: Route reads to target keyspace
weight: 30
---

### Command

```
SwitchReads  [-cells=<cells>] [-reverse] [-dry-run] 
             -tablet_type={replica|rdonly} <keyspace.workflow>
```

### Description

SwitchReads is used to switch reads for tables in a MoveTables workflow or for entire keyspace 
to the target keyspace in a Reshard workflow.

### Parameters

#### -cells 
**optional**\
**default** all

<div class="cmd">
Comma separated list of cells or cell aliases in which reads should be switched in the target keyspace
</div>

#### -reverse 
**optional**\
**default** false

<div class="cmd">
When a workflow is setup the routing rules are setup so that reads/writes to the target shards
still go to the source shard since the target is not yet setup. If SwitchReads is called without
-reverse then the routing rules for the target keyspace are setup to actually use it. It is assumed
that the workflow was successful and user is ready to use the target keyspace now.

However if, for any reason, we want to abort this workflow using the -reverse flag deletes the
rules that were setup and vtgate will route the queries to this table to the the source table.
There is no way to reverse the use of the -reverse flag other than by recreating the routing rules
again using the vtctl ApplyRoutingRules command.
</div>

#### -dry-run 
**optional**\
**default** false

<div class="cmd">
You can do a dry run where no actual action is taken but the command logs all the actions that would be taken
by SwitchReads.
</div>

#### -tablet_type 
**mandatory**

<div class="cmd">
On which type of tablets should be reads be switched to the target keyspace. One of replica or rdonly. rdonly
tables should be switched first before replica tablets. 
</div>

#### keyspace.workflow 
**mandatory**

<div class="cmd">
Name of target keyspace and the associated workflow to SwitchReads for.
</div>
