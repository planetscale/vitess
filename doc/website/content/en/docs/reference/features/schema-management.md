---
title: Schema Management
aliases: ['/docs/schema-management/','/docs/user-guides/schema-management/','/docs/reference/schema-management/']
---

Using Vitess requires you to work with two different types of schemas:

1. The MySQL database schema. This is the schema of the individual MySQL instances.
2. The [VSchema](../vschema), which describes all the keyspaces and how they're sharded.

The workflow for the `VSchema` is as follows:

1. Apply the `VSchema` for each keyspace using the `ApplyVschema` command. This saves the VSchemas in the global topology service.
2. Execute `RebuildVSchemaGraph` for each cell (or all cells). This command propagates a denormalized version of the combined VSchema to all the specified cells. The main purpose for this propagation is to minimize the dependency of each cell from the global topology. The ability to push a change to only specific cells allows you to canary the change to make sure that it's good before deploying it everywhere.

This document describes the [`vtctl`](../../../reference/programs/vtctl/) commands that you can use to [review](#reviewing-your-schema) or [update](#changing-your-schema) your schema in Vitess.

Note that this functionality is not recommended for long-running schema changes. It is recommended to use a tool such as [`pt-online-schema-change`](https://www.percona.com/doc/percona-toolkit/LATEST/pt-online-schema-change.html) or [`gh-ost`](https://github.com/github/gh-ost) instead.


## Reviewing your schema

This section describes the following vtctl commands, which let you look at the schema and validate its consistency across tablets or shards:

* [GetSchema](#getschema)
* [ValidateSchemaShard](#validateschemashard)
* [ValidateSchemaKeyspace](#validateschemakeyspace)
* [GetVSchema](#getvschema)
* [GetSrvVSchema](#getsrvvschema)

### GetSchema

The [GetSchema](../../../reference/programs/vtctl/#getschema) command displays the full schema for a tablet or a subset of the tablet's tables. When you call `GetSchema`, you specify the tablet alias that uniquely identifies the tablet. The `<tablet alias>` argument value has the format `<cell name>-<uid>`.

**Note**: You can use the [`vtctl ListAllTablets`](../../../reference/programs/vtctl/#listalltablets) command to retrieve a list of tablets in a cell and their unique IDs.

The following example retrieves the schema for the tablet with the unique ID test-000000100:

``` sh
GetSchema test-000000100
```

### ValidateSchemaShard

The [`ValidateSchemaShard`](../../../reference/programs/vtctl/#validateschemashard) command confirms that for a given keyspace, all of the replica tablets in a specified shard have the same schema as the master tablet in that shard. When you call `ValidateSchemaShard`, you specify both the keyspace and the shard that you are validating.

The following command confirms that the master and replica tablets in shard `0` all have the same schema for the `user` keyspace:

``` sh
ValidateSchemaShard user/0
```

### ValidateSchemaKeyspace

The [`ValidateSchemaKeyspace`](../../../reference/programs/vtctl/#validateschemakeyspace) command confirms that all of the tablets in a given keyspace have the the same schema as the master tablet on shard `0` in that keyspace. Thus, whereas the `ValidateSchemaShard` command confirms the consistency of the schema on tablets within a shard for a given keyspace, `ValidateSchemaKeyspace` confirms the consistency across all tablets in all shards for that keyspace.

The following command confirms that all tablets in all shards have the same schema as the master tablet in shard 0 for the user keyspace:

``` sh
ValidateSchemaKeyspace user
```

### GetVSchema

The [`GetVSchema`](../../../reference/programs/vtctl/#getvschema) command displays the global VSchema for the specified keyspace.

### GetSrvVSchema

The [`GetSrvVSchema`](../../../reference/programs/vtctl/#getsrvvschema) command displays the combined VSchema for a given cell.

## Changing your schema

This section describes the following commands:

* [ApplySchema](#applyschema)
* [ApplyVSchema](#applyvschema)
* [RebuildVSchemaGraph](#rebuildvschemagraph)

### ApplySchema

Vitess' schema modification functionality is designed the following goals in mind:

* Enable simple updates that propagate to your entire fleet of servers.
* Require minimal human interaction.
* Minimize errors by testing changes against a temporary database.
* Guarantee very little downtime (or no downtime) for most schema updates.
* Do not store permanent schema data in the topology service.

Note that, at this time, Vitess only supports [data definition statements](https://dev.mysql.com/doc/refman/5.6/en/sql-data-definition-statements.html) that create, modify, or delete database tables. For instance, `ApplySchema` does not affect stored procedures or grants.

The [ApplySchema](../../../reference/programs/vtctl/#applyvschema) command applies a schema change to the specified keyspace on every master tablet, running in parallel on all shards. Changes are then propagated to replicas. The command format is: `ApplySchema {-sql=<sql> || -sql_file=<filename>} <keyspace>`

When the `ApplySchema` action actually applies a schema change to the specified keyspace, it performs the following steps:

1. It finds shards that belong to the keyspace, including newly added shards if a [resharding event](../sharding/#resharding) has taken place.
2. It validates the SQL syntax and determines the impact of the schema change. If the scope of the change is too large, Vitess rejects it. See the [permitted schema changes](#permitted-schema-changes) section for more detail.
3. It employs a pre-flight check to ensure that a schema update will succeed before the change is actually applied to the live database. In this stage, Vitess copies the current schema into a temporary database, applies the change there to validate it, and retrieves the resulting schema. By doing so, Vitess verifies that the change succeeds without actually touching live database tables.
4. It applies the SQL command on the master tablet in each shard.

The following sample command applies the SQL in the **user_table.sql** file to the **user** keyspace:

`ApplySchema -sql_file=user_table.sql user`

#### Permitted schema changes

The `ApplySchema` command supports a limited set of DDL statements. In addition, Vitess rejects some schema changes because large changes can slow replication and may reduce the availability of your overall system.

The following list identifies types of DDL statements that Vitess supports:

* `CREATE TABLE`
* `CREATE INDEX`
* `CREATE VIEW`
* `ALTER TABLE`
* `ALTER VIEW`
* `RENAME TABLE`
* `DROP TABLE`
* `DROP INDEX`
* `DROP VIEW`

In addition, Vitess applies the following rules when assessing the impact of a potential change:

* `DROP` statements are always allowed, regardless of the table's size.
* `ALTER` statements are only allowed if the table on the shard's master tablet has 100,000 rows or less.
* For all other statements, the table on the shard's master tablet must have 2 million rows or less.

If a schema change gets rejected because it affects too many rows, you can specify the flag `-allow_long_unavailability` to tell `ApplySchema` to skip this check. However, we do not recommend this. Instead, you should apply large schema changes by using an external tool such as `gh-ost` or `pt-online-schema-change`.

### ApplyVSchema

The [`ApplyVSchema`](../../../reference/programs/vtctl/#applyvschema) command applies the specified VSchema to the keyspace. The VSchema can be specified as a string or in a file.

### RebuildVSchemaGraph

The [`RebuildVSchemaGraph`](../../../reference/programs/vtctl/#rebuildvschemagraph) command propagates the global VSchema to a specific cell or the list of specified cells.
