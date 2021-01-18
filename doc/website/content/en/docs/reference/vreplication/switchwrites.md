---
title: SwitchWrites
description: Route writes to target keyspace in a vreplication workflow
weight: 50
---

### Command

```
SwitchWrites  [-filtered_replication_wait_time=30s] [-cancel] [-reverse_replication=true] 
              [-dry-run] <keyspace.workflow>
```

### Description

SwitchWrites is used to switch writes for tables in a MoveTables workflow or for entire keyspace in the
Reshard workflow away from the master in the source keyspace to the master in the target keyspace

### Parameters

#### -filtered_replication_wait_time 
**optional**\
**default** 30s

<div class="cmd">
SwitchWrites first stops writes on the source master and waits for the replication to the target to
catchup with the point where the writes were stopped. If the wait time is longer than filtered_replication_wait_time
the command will error out. 
For setups with high write qps you may need to increase this value.
</div>

#### -cancel 
**optional**\
**default** false

<div class="cmd">
If a previous SwitchWrites returned with an error you can restart it by running the command again (after fixing
the issue that caused the failure) or the SwitchWrites can be canceled using this parameter. Only the SwitchWrites
is cancelled: the workflow is set to Running so that replication continues.
</div>

#### -reverse_replication 
**optional**\
**default** true

<div class="cmd">
SwitchWrites, by default, starts a reverse replication stream with the current target as the source, replicating
back to the original source. This enables a quick and simple rollback. This reverse workflow name is that
of the original workflow concatenated with \_reverse.
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
Name of target keyspace and the associated workflow to SwitchWrites for.
</div>
