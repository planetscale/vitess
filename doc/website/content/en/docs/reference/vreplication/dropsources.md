---
title: DropSources
description: Cleans up after a MoveTables and Reshard workflow
weight: 60
---

### Command

```
DropSources  [-dry_run] [-rename_tables] <keyspace.workflow>
```

### Description


Once SwitchWrites has been run DropSources cleans up the source resources by deleting the
source tables for a MoveTables workflow or source shards for a Reshard workflow. It also
cleans up other artifacts of the workflow, deleting forward and reverse replication streams and
blacklisted tables.

***Warning***: This command actually deletes data. We recommend that you run this
with the -dry_run parameter first and reads its output so that you know which actions will be performed.


### Parameters

#### -rename_tables 
**optional**\
**default** all

<div class="cmd">
Only applies for a MoveTables workflow. Instead of deleting the tables in the source it renames them
     by prefixing the tablename with an _ (underscore).
</div>

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
Name of target keyspace and the associated workflow to run VDiff on.
</div>

