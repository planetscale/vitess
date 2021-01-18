---
title: Materialize
weight: 3
aliases: ['/docs/user-guides/materialize/'] 
---

{{< info >}}
This guide follows on from the Get Started guides. Please make sure that you have an [Operator](../../../get-started/operator), [local](../../../get-started/local) or [Helm](../../../get-started/helm) installation ready.  Make sure you have only run the "101" step of the examples, for example `101_initial_cluster.sh` in the [local](../../../get-started/local) example. The commands in this guide also assumes you have setup the shell aliases from the example, e.g. `env.sh` in the [local](../../../get-started/local) example.
{{< /info >}}

**Materialize** is a new VReplication workflow in Vitess 6.  It can be used as a more general way to achieve something similar to [MoveTables](../../../concepts/move-tables), or as a way to generate materialized views of a table (or set of tables) in the same or different keyspace from the source table (or set of tables).  In general, it can be used to create and maintain continually updated materialized views in Vitess, without having to resort to manual or trigger-based population of the view content.

Since `Materialize` uses VReplication, the view can be kept up-to-date very close to real-time, which enables use-cases like creating copies of the same table sharded different ways for the purposes of certain types of queries that would otherwise be prohibitively expensive on the original table.  `Materialize` is also flexible enough to allow for you to pre-create the schema and vschema for the copied table, allowing you to, for example, maintain a copy of a table without some of the source table's MySQL indexes.  Alternatively, you could use `Materialize` to do certain schema changes (e.g. change the type of a table column) without having to use other tools like [gh-ost](https://github.com/github/gh-ost).

In our example, we will be using `Materialize` to perform something similar to the [MoveTables](../move-tables) user guide, which will cover just the basics of what is possible using `Materialize`.


Let's start by simulating this situation by loading sample data:

```sql
mysql < ../common/insert_commerce_data.sql
```

We can look at what we just inserted:

```sh
# On helm and local installs:
mysql --table < ../common/select_commerce_data.sql
# With operator:
mysql --table < select_commerce_data.sql

Using commerce/0
Customer
+-------------+--------------------+
| customer_id | email              |
+-------------+--------------------+
|           1 | alice@domain.com   |
|           2 | bob@domain.com     |
|           3 | charlie@domain.com |
|           4 | dan@domain.com     |
|           5 | eve@domain.com     |
+-------------+--------------------+
Product
+----------+-------------+-------+
| sku      | description | price |
+----------+-------------+-------+
| SKU-1001 | Monitor     |   100 |
| SKU-1002 | Keyboard    |    30 |
+----------+-------------+-------+
COrder
+----------+-------------+----------+-------+
| order_id | customer_id | sku      | price |
+----------+-------------+----------+-------+
|        1 |           1 | SKU-1001 |   100 |
|        2 |           2 | SKU-1002 |    30 |
|        3 |           3 | SKU-1002 |    30 |
|        4 |           4 | SKU-1002 |    30 |
|        5 |           5 | SKU-1002 |    30 |
+----------+-------------+----------+-------+
```

Note that we are using keyspace `commerce/0` to select data from our tables.

## Planning to use Materialize

In this scenario, we are going to make two copies of the `corder` table **in the same keyspace** using a different tablenames of `corder_view` and `corder_view_redacted`.  The first copy will be identical to the source table, but for the `corder_view_redacted` copy, we will use the opportunity to drop the `price` column from the copy.  Since we are doing the `Materialize` to the same keyspace, we do not need to create a new keyspace or tablets as we did for the [MoveTables](../move-tables) user guide.

## Create the destination tables

In the case where we using `Materialize` to copy tables between keyspaces, we can use the `"create_ddl": "copy"` option in the `Materialize` `json_spec` `table_settings` to create the target table for us (similar to what `MoveTables` does).  However, in our case where we are using `Materialize` with a target table name different from the source table name, we need to manually create the target tables.  Let's go ahead and do that:

```sql
$ mysql -A
Welcome to the MySQL monitor.  Commands end with ; or \g.
.
.

mysql> CREATE TABLE `corder_view` (
  `order_id` bigint NOT NULL,
  `customer_id` bigint DEFAULT NULL,
  `sku` varbinary(128) DEFAULT NULL,
  `price` bigint DEFAULT NULL,
  PRIMARY KEY (`order_id`)
) ENGINE=InnoDB;
Query OK, 0 rows affected (0.13 sec)

mysql> CREATE TABLE `corder_view_redacted` (
  `order_id` bigint NOT NULL,
  `customer_id` bigint DEFAULT NULL,
  `sku` varbinary(128) DEFAULT NULL,
  PRIMARY KEY (`order_id`)
) ENGINE=InnoDB;
Query OK, 0 rows affected (0.09 sec)
```

Now we need to make sure Vitess' view of our schema is up-to-date:

```sh
$ vtctlclient ReloadSchemaKeyspace commerce
```

And now we can proceed to the `Materialize` step(s).

## Start the Materialize (first copy)

We will run two `Materialize` operations, one for each copy/view of the `corder` table we will be creating.  We could combine these two operations into a single `Materialize` operation, but we will keep them separate for clarity.

```bash
$ vtctlclient Materialize '{"workflow": "copy_corder_1", "source_keyspace": "commerce", "target_keyspace": "commerce", "table_settings": [{"target_table": "corder_view", "source_expression": "select * from corder"}]}'
```

Now, we should see the materialized view table `corder_view`:

```sh
$ echo "select * from corder_view;" | mysql --table commerce
+----------+-------------+----------+-------+
| order_id | customer_id | sku      | price |
+----------+-------------+----------+-------+
|        1 |           1 | SKU-1001 |   100 |
|        2 |           2 | SKU-1002 |    30 |
|        3 |           3 | SKU-1002 |    30 |
|        4 |           4 | SKU-1002 |    30 |
|        5 |           5 | SKU-1002 |    30 |
+----------+-------------+----------+-------+
```

And if we insert a row into the source table, it will be replicated to the materialized view:

```
$ echo "insert into corder (order_id, customer_id, sku, price) values (6, 6, 'SKU-1002', 30);" | mysql commerce
$ echo "select * from corder_view;" | mysql --table commerce
+----------+-------------+----------+-------+
| order_id | customer_id | sku      | price |
+----------+-------------+----------+-------+
|        1 |           1 | SKU-1001 |   100 |
|        2 |           2 | SKU-1002 |    30 |
|        3 |           3 | SKU-1002 |    30 |
|        4 |           4 | SKU-1002 |    30 |
|        5 |           5 | SKU-1002 |    30 |
|        6 |           6 | SKU-1002 |    30 |
+----------+-------------+----------+-------+
```

Note that the target table is just a normal table, there is nothing that prevents you from writing to it directly. While you might not want to do that in this "view" use-case, in certain other use-cases, it might be completely acceptable to write to the table, as long as you don't end up altering or removing rows in a fashion that would break the "replication" part of VReplication (e.g. removing a row in the target table directly that is later updated in the source table).

## Viewing the workflow while in progress

While we can also see and manipulate the underlying VReplication streams
created by `Materialize`; there are commands to show, stop, start
and delete the operations associated with a Materialize workflow.
For example, once we have started the `Materialize` command above,
we can observe the status of the VReplication stream doing the
materialization via the `vtctlclient Workflow` command:

```sh
$ vtctlclient Workflow commerce.copy_corder_1 show
{
    "Workflow": "copy_corder_1",
    "SourceLocation": {
        "Keyspace": "commerce",
        "Shards": [
            "0"
        ]
    },
    "TargetLocation": {
        "Keyspace": "commerce",
        "Shards": [
            "0"
        ]
    },
    "MaxVReplicationLag": 1599019410,
    "ShardStatuses": {
        "0/zone1-0000000100": {
            "MasterReplicationStatuses": [
                {
                    "Shard": "0",
                    "Tablet": "zone1-0000000100",
                    "ID": 4,
                    "Bls": {
                        "keyspace": "commerce",
                        "shard": "0",
                        "filter": {
                            "rules": [
                                {
                                    "match": "corder_view",
                                    "filter": "select * from corder"
                                }
                            ]
                        }
                    },
                    "Pos": "MySQL56/c0d82d27-ecd0-11ea-9840-001e677affd5:1-97978",
                    "StopPos": "",
                    "State": "Copying",
                    "MaxReplicationLag": 9223372036854775807,
                    "DBName": "vt_commerce",
                    "TransactionTimestamp": 0,
                    "TimeUpdated": 1599019408,
                    "Message": "",
                    "CopyState": [
                        {
                            "Table": "corder_view",
                            "LastPK": "fields:<name:\"order_id\" type:INT64 > rows:<lengths:5 values:\"37014\" >"
                        }
                    ]
                }
            ],
            "TabletControls": null,
            "MasterIsServing": true
        }
    }
}
```

Note the state of `Copying`, this will transition to `Running` when the
bulk copying of rows is complete.

We can now also use the stop/start commands to temporarily stop the
materialization workflow.  E.g. `stop`:

```sh
$ vtctlclient Workflow commerce.copy_corder_1 stop
+------------------+--------------+
|      Tablet      | RowsAffected |
+------------------+--------------+
| zone1-0000000100 |            1 |
+------------------+--------------+
```

And `start` to start the workflow again and continue the materialization:

```sh
$ vtctlclient Workflow commerce.copy_corder_1 start
+------------------+--------------+
|      Tablet      | RowsAffected |
+------------------+--------------+
| zone1-0000000100 |            1 |
+------------------+--------------+
```

Eventually, when the copy is done, or we have materialized the data, and
do not want to continue the copy of new source rows, we can delete the
workflow via:

```sh
$ vtctlclient Workflow commerce.copy_corder_1 delete
+------------------+--------------+
|      Tablet      | RowsAffected |
+------------------+--------------+
| zone1-0000000100 |            1 |
+------------------+--------------+
```

Note that deleting the workflow will not drop the target table for the
`Materialize` workflow, or any of the data already copied.  The data
in the target table will remain as it was at the moment the workflow
was deleted (or previously stopped).


## Start the Materialize (redacted copy)

Now, we can perform the copy to the `corder_view_redacted` table we created earlier.  Note that we created this table without a price column;  we will not be copying that column.

```bash
$ vtctlclient Materialize '{"workflow": "copy_corder_2", "source_keyspace": "commerce", "target_keyspace": "commerce", "table_settings": [{"target_table": "corder_view_redacted", "source_expression": "select order_id, customer_id, sku from corder"}]}'
```

Again, looking the target table will show all the source table rows, this time without the `sku` column:

```sh
$ echo "select * from corder_view_redacted;" | mysql --table commerce
+----------+-------------+----------+
| order_id | customer_id | sku      |
+----------+-------------+----------+
|        1 |           1 | SKU-1001 |
|        2 |           2 | SKU-1002 |
|        3 |           3 | SKU-1002 |
|        4 |           4 | SKU-1002 |
|        5 |           5 | SKU-1002 |
|        6 |           6 | SKU-1002 |
+----------+-------------+----------+
```

Again, we can add a row to the source table, and see it replicated into the target table:

```sh
$ echo "insert into corder (order_id, customer_id, sku, price) values (7, 7, 'SKU-1002', 30);" | mysql commerce
$ echo "select * from corder_view_redacted;" | mysql --table commerce
+----------+-------------+----------+
| order_id | customer_id | sku      |
+----------+-------------+----------+
|        1 |           1 | SKU-1001 |
|        2 |           2 | SKU-1002 |
|        3 |           3 | SKU-1002 |
|        4 |           4 | SKU-1002 |
|        5 |           5 | SKU-1002 |
|        6 |           6 | SKU-1002 |
|        7 |           7 | SKU-1002 |
+----------+-------------+----------+
```

## What happened under the covers

As with `MoveTables`, a VReplication stream was formed for each of the `Materialize` workflows we executed.  We can see these by inspecting the VReplication table on the target keyspace master tablet, e.g. in this case:

```sh
$ vtctlclient VReplicationExec zone1-0000000100 'select * from _vt.vreplication'
+----+---------------+---------------------------------------------+----------------------------------------------------+----------+---------------------+---------------------+------+--------------+--------------+-----------------------+---------+---------+-------------+
| id |   workflow    |                   source                    |                        pos                         | stop_pos |       max_tps       | max_replication_lag | cell | tablet_types | time_updated | transaction_timestamp |  state  | message |   db_name   |
+----+---------------+---------------------------------------------+----------------------------------------------------+----------+---------------------+---------------------+------+--------------+--------------+-----------------------+---------+---------+-------------+
|  1 | copy_corder_1 | keyspace:"commerce" shard:"0"               | MySQL56/00a04e3a-e74d-11ea-a8c9-001e677affd5:1-926 |          | 9223372036854775807 | 9223372036854775807 |      |              |   1598416592 |            1598416591 | Running |         | vt_commerce |
|    |               | filter:<rules:<match:"corder_view"          |                                                    |          |                     |                     |      |              |              |                       |         |         |             |
|    |               | filter:"select * from corder" > >           |                                                    |          |                     |                     |      |              |              |                       |         |         |             |
|  2 | copy_corder_2 | keyspace:"commerce" shard:"0"               | MySQL56/00a04e3a-e74d-11ea-a8c9-001e677affd5:1-926 |          | 9223372036854775807 | 9223372036854775807 |      |              |   1598416592 |            1598416591 | Running |         | vt_commerce |
|    |               | filter:<rules:<match:"corder_view_redacted" |                                                    |          |                     |                     |      |              |              |                       |         |         |             |
|    |               | filter:"select order_id, customer_id, sku   |                                                    |          |                     |                     |      |              |              |                       |         |         |             |
|    |               | from corder" > >                            |                                                    |          |                     |                     |      |              |              |                       |         |         |             |
+----+---------------+---------------------------------------------+----------------------------------------------------+----------+---------------------+---------------------+------+--------------+--------------+-----------------------+---------+---------+-------------+
```

It is important to use the `vtctlclient VReplicationExec` command to inspect this table, since some of the fields are binary and might not render properly in a MySQL client (at least with default options).  In the above output, you can see a summary of the VReplication streams that were setup (and are still `Running`) to copy and then do continuous replication of the source table (`corder`) to the two different target tables.

## Cleanup

As seen earlier, you can easily use the `vtctlclient Workflow ... delete`
command to clean up a materialize operation.  If you like, you can also
instead use the `VReplicationExec` command to temporarily stop the replication
streams for the VReplication streams that make up the `Materialize` process.
For example, to stop both streams, you can do:

```sh
$ vtctlclient VReplicationExec zone1-0000000100 'update _vt.vreplication set state = "Stopped" where id in (1,2)'
+
+
$ vtctlclient VReplicationExec zone1-0000000100 'select * from _vt.vreplication'
+----+---------------+---------------------------------------------+-----------------------------------------------------+----------+---------------------+---------------------+------+--------------+--------------+-----------------------+---------+---------+-------------+
| id |   workflow    |                   source                    |                         pos                         | stop_pos |       max_tps       | max_replication_lag | cell | tablet_types | time_updated | transaction_timestamp |  state  | message |   db_name   |
+----+---------------+---------------------------------------------+-----------------------------------------------------+----------+---------------------+---------------------+------+--------------+--------------+-----------------------+---------+---------+-------------+
|  1 | copy_corder_1 | keyspace:"commerce" shard:"0"               | MySQL56/00a04e3a-e74d-11ea-a8c9-001e677affd5:1-1218 |          | 9223372036854775807 | 9223372036854775807 |      |              |   1598416861 |            1598416859 | Stopped |         | vt_commerce |
|    |               | filter:<rules:<match:"corder_view"          |                                                     |          |                     |                     |      |              |              |                       |         |         |             |
|    |               | filter:"select * from corder" > >           |                                                     |          |                     |                     |      |              |              |                       |         |         |             |
|  2 | copy_corder_2 | keyspace:"commerce" shard:"0"               | MySQL56/00a04e3a-e74d-11ea-a8c9-001e677affd5:1-1218 |          | 9223372036854775807 | 9223372036854775807 |      |              |   1598416861 |            1598416859 | Stopped |         | vt_commerce |
|    |               | filter:<rules:<match:"corder_view_redacted" |                                                     |          |                     |                     |      |              |              |                       |         |         |             |
|    |               | filter:"select order_id, customer_id, sku   |                                                     |          |                     |                     |      |              |              |                       |         |         |             |
|    |               | from corder" > >                            |                                                     |          |                     |                     |      |              |              |                       |         |         |             |
+----+---------------+---------------------------------------------+-----------------------------------------------------+----------+---------------------+---------------------+------+--------------+--------------+-----------------------+---------+---------+-------------+
```

Any changes to the source tables will now not be applied to the target tables until you update the `state` column back to `Running`.

Lastly, you can clean up the `Materialize` process by just using `VReplicationExec` to delete the rows in the `_vt.vreplication` table.  This will do the necessary runtime cleanup as well. E.g.:

```sh
$ vtctlclient VReplicationExec zone1-0000000100 'delete from  _vt.vreplication where id in (1,2)'
+
+
$ vtctlclient VReplicationExec zone1-0000000100 'select * from _vt.vreplication'
+----+----------+--------+-----+----------+---------+---------------------+------+--------------+--------------+-----------------------+-------+---------+---------+
| id | workflow | source | pos | stop_pos | max_tps | max_replication_lag | cell | tablet_types | time_updated | transaction_timestamp | state | message | db_name |
+----+----------+--------+-----+----------+---------+---------------------+------+--------------+--------------+-----------------------+-------+---------+---------+
+----+----------+--------+-----+----------+---------+---------------------+------+--------------+--------------+-----------------------+-------+---------+---------+
```

Note that this just cleans up the VReplication streams;  the actual source and target tables are left untouched and in the same state they were at the moment the VReplication streams were stopped or deleted.

## Recap

As mentioned at the beginning, `Materialize` gives you finer control over the VReplication process without having to form VReplication rules completely by hand.  For the ultimate flexibility, that is still possible, but you should be able to use `Materialize` together with other Vitess features like routing rules to cover a large set of potential migration and data maintenance use-cases without resorting to creating VReplication rules directly.
