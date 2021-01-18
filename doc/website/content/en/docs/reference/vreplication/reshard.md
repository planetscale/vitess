---
title: Reshard
description: split or merge shards in a keyspace
weight: 60
---

### Command

```
Reshard  [-skip_schema_copy] <keyspace.workflow> <source_shards> <target_shards>

```

### Description

Reshard support horizontal sharding by letting you change the sharding ranges of your existing keyspace.

### Parameters

#### -skip_schema_copy 
**optional**\
**default** false

<div class="cmd">
If true the source schema is copied to the target shards. If false, you need to create the tables
before calling reshard.
</div>

#### keyspace.workflow 
**mandatory**

<div class="cmd">
Name of keyspace being sharded and the associated workflow name, used in later commands to refer back to this reshard.
</div>

#### source_shards 
**mandatory**

<div class="cmd">
Comma separated shard names to reshard from.
</div>

#### target_shards
**mandatory**

<div class="cmd">
Comma separated shard names to reshard to.
</div>


### A Reshard Workflow

Once you decide on the new resharding strategy for a keyspace, you need to initiate a VReplication workflow as follows:

1. Initiate the migration using Reshard
2. Monitor the workflow using [Workflow](../workflow) or [VExec](../vexec)
3. Confirm that data has been copied over correctly using [VDiff](../vdiff)
4. Start the cutover by routing all reads from your application to those tables using [SwitchReads](../switchreads)
5. Complete the cutover by routing all writes using [SwitchWrites](../switchwrites)
6. Optionally cleanup the source tables using [DropSources](../dropsources)
