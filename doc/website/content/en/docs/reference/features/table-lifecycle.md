---
title: Table lifecycle
aliases: ['/docs/user-guides/table-lifecycle/','/docs/reference/table-lifecycle/']
---

Vitess manages a table lifecycle flow, an abstraction and automation for a `DROP TABLE` operation.

# Problems with DROP TABLE

Vitess inherits the same issues that MySQL has with `DROP TABLE`.  Doing a direct
`DROP TABLE my_table` in production can be a risky operation. In busy environments
this can lead to a complete lockdown of the database for the duration of seconds,
to minutes and more. This is typically less of a problem in Vitess than it might
be in normal MySQL, if you are keeping your shard instances (and thus shard
table instances) small, but could still be a problem.

There are two locking aspects to dropping tables:

- Purging dropped table's pages from the InnoDB buffer pool(s)
- Removing table's data file (`.ibd`) from the filesystem.

The exact locking behavior and duration can vary depending on
various factors:
 - Which filesystem is used
 - Whether the MySQL adaptive hash index is used
 - Whether you are attempting to hack around some of the MySQL `DROP TABLE`
   performance problems using hard links

It is common practice to avoid direct `DROP TABLE` statements and to follow
a more elaborate table lifecycle.

# Vitess table lifecycle

The lifecycle offered by Vitess consists of the following stages or some subset:

> _in use_ -> hold -> purge -> evac -> drop -> _removed_

To understand the flow better, consider the following breakdown:

- _In use_: the table is serving traffic, like a normal table.
- `hold`: the table is renamed to some arbitrary new name. The application cannot see it, and considers it as gone. However, the table still exists, with all of its data intact. It is possible to reinstate it (e.g. in case we realize some application still requires it) by renaming it back to its original name.
- `purge`: the table is in the process of being purged (i.e. rows are being deleted). The purge process completes when the table is completely empty. At the end of the purge process the table no longer has any pages in the buffer pool(s). However, the purge process itself loads the table pages to cache in order to delete rows.
  Vitess purges the table a few rows at a time, and uses a throttling mechanism to reduce load.
  Vitess disables binary logging for the purge. The deletes are not written to the binary logs and are not replicated. This reduces load from disk IO, network, and replication lag. Data is not purged on the replicas.
  Experience shows that dropping a table populated with data on a replica has lower performance impact than on the primary, and the tradeoff is worthwhile.
- `evac`: a waiting period during which we expect normal production traffic to slowly evacuate the (now inactive) table's pages from the buffer pool. Vitess hard codes this period for `72` hours. The time is heuristic, there is no tracking of table pages in the buffer pool.
- `drop`: an actual `DROP TABLE` is imminent
- _removed_: table is dropped. When using InnoDB and `innodb_file_per_table` this means the `.ibd` data file backing the table is removed, and disk space is reclaimed.

# Lifecycle subsets and configuration

Different environments and users have different requirements and workflows. For example:

- Some wish to immediately start purging the table, wait for pages to evacuate, then drop it.
- Some want to keep the table's data for a few days, then directly drop it.
- Some just wish to directly drop the table, they see no locking issues (e.g. smaller table).

Vitess supports all subsets via `-table_gc_lifecycle` flag to `vttablet`. The default is `"hold,purge,evac,drop"` (the complete cycle). Users may configure any subset, e.g. `"purge,drop"`, `"hold,drop"`, `"hold,evac,drop"` or even just `"drop"`.

Vitess will always work the steps in this order: `hold -> purge -> evac -> drop`. For example, setting `-table_gc_lifecycle "drop,hold"` still first _holds_, then _drops_

All subsets end with a `drop`, even if not explicitly mentioned. Thus, `"purge"` is interpreted as `"purge,drop"`.

# Automated lifecycle

Vitess internally uses the above table lifecycle for [online, managed schema migrations](../../../user-guides/schema-changes/managed-online-schema-changes/). Online schema migration tools `gh-ost` and `pt-online-schema-change` create artifact tables or end with leftover tables: Vitess automatically collects those tables. The artifact or leftover tables are immediate moved to `purge` state. Depending on `-table_gc_lifecycle`, they may spend time in this state, getting purged, or immediately transitioned to the next state.

# User-facing DROP TABLE lifecycle

Table lifecycle is not yet available directly to the application user. Vitess will introduce a special syntax to allow users to indicate they want Vitess to manage a table's lifecycle.
