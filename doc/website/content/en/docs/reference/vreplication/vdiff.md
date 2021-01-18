---
title: VDiff
description: Compare the source and target in a workflow to ensure integrity
weight: 20
---

### Command

```
VDiff  [-source_cell=<cell>] [-target_cell=<cell>] [-tablet_types=replica]
       [-filtered_replication_wait_time=30s] <keyspace.workflow>
```

### Description

VDiff does a row by row comparison of all tables associated with the workflow, diffing the
source keyspace and the target keyspace and reporting counts of missing/extra/unmatched rows.

It is highly recommended that you do this before you finalize a workflow with SwitchWrites.

### Parameters

#### -source_cell 
**optional**\
**default** all

<div class="cmd">
VDiff will choose a tablet from this cell to diff the source table(s) with the target tables
</div>

#### -target_cell 
**optional**\
**default** all

<div class="cmd">
VDiff will choose a tablet from this cell to diff the source table(s) with the target tables
</div>

#### -tablet_types
**optional**\
**default** replica

<div class="cmd">
A comma separated list of tablet types that are used while picking a tablet for sourcing data.
One or more from MASTER, REPLICA, RDONLY.<br><br>
</div>

#### -filtered_replication_wait_time 
**optional**\
**default** 30s

<div class="cmd">
VDiff finds the current position of the source master and then waits for the target replication to reach
that position for _filtered_replication_wait_time_. If the target is much behind the source or if there is 
a high write qps on the source then this time will need to be increased.
</div>


#### keyspace.workflow 
**mandatory**

<div class="cmd">
Name of target keyspace and the associated workflow to run VDiff on.
</div>

#### Example

```
$ vtctlclient VDiff customer.commerce2customer

Summary for corder: {ProcessedRows:10 MatchingRows:10 MismatchedRows:0 ExtraRowsSource:0 ExtraRowsTarget:0}
Summary for customer: {ProcessedRows:11 MatchingRows:11 MismatchedRows:0 ExtraRowsSource:0 ExtraRowsTarget:0}
```

### Notes
 * You can follow the progress of the command by tailing the vtctld logs
 * VDiff can take very long (hours/days) for huge tables, so this needs to be taken into account. If VDiff
 takes more than an hour and you use vtctlclient then it will hit the grpc/http default timeout of 1 hour. 
 In that case you can use vtctl (the bundled vctlclient + vtctld) instead.
 * There is no throttling, so you might see an increased lag in the replica used as the source.
 
_VReplication and VDiff performance improvements as well as freno-style throttling support are on the roadmap!_
