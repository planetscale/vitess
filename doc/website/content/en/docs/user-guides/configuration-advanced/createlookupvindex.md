---
title: CreateLookupVindex
weight: 11
aliases: ['/docs/user-guides/createlookupvindex/'] 
---

{{< info >}}
This guide follows on from the Get Started guides. Please make sure that you have an [Operator](../../../get-started/operator), [local](../../../get-started/local) or [Helm](../../../get-started/helm) installation ready.  Make sure you are at the point where you have the sharded keyspace called `customer` setup.
{{< /info >}}

**CreateLookupVindex** is a new VReplication workflow in Vitess 6.  It is used to create **and** backfill a lookup Vindex automatically for a table that already exists, and may have a significant amount of data in it already.

Internally, the `CreateLookupVindex` process uses VReplication for the backfill process, until the lookup Vindex is "in sync". Then the normal process for adding/deleting/updating rows in the lookup Vindex via the usual transactional flow when updating the "owner" table for the Vindex takes over.

In this guide, we will walk through the process of using the `CreateLookupVindex` workflow, and give some insight into what happens underneath the covers.

`vtctlclient CreateLookupVindex` has the following syntax:

`CreateLookupVindex  [-cell=<cell>] [-tablet_types=<source_tablet_types>] <keyspace> <json_spec>`

 * `<json_spec>`:  Use the lookup Vindex specified in `<json_spec>` along with
VReplication to populate/backfill the lookup Vindex from the source table.
 * `<keyspace>`:  The Vitess keyspace we are creating the lookup Vindex in.
The source table is expected to also be in this keyspace.
 * `-tablet-types`:  Provided to specify the shard tablet types 
(e.g. `MASTER`, `REPLICA`, `RDONLY`) that are acceptable
as source tablets for the VReplication stream(s) that this command will
create. If not specified, the tablet type used will default to the value
of the vttablet `-vreplication_tablet_type` option, which defaults to
`REPLICA`.
 * `-cell`: By default VReplication streams, such as used by 
`CreateLookupVindex` will not cross cell boundaries.  If you want the 
VReplication streams to source their data from tablets in a cell other 
than the local cell, you can use the `-cell` option to specify this.

The `<json_spec>` describes the lookup Vindex to be created, and details about
the table it is to be created against (on which column, etc.).  However,
you do not have to specify details about the actual lookup table, Vitess
will create that automatically based on the type of the column you are
creating the Vindex column on, etc.

In the context of the regular `customer` database that is part of the Vitess
examples we started earlier, let's add some rows into the `customer.corder`
table, and then look at an example `<json_spec>`:

```sql
$ mysql -P 15306 -h 127.0.0.1 -u root --binary-as-hex=false -A
Welcome to the MySQL monitor.  Commands end with ; or \g.
.
.
.
mysql> use customer;
Database changed

mysql> show tables;
+-----------------------+
| Tables_in_vt_customer |
+-----------------------+
| corder                |
| customer              |
+-----------------------+
2 rows in set (0.00 sec)

mysql> desc corder;
+-------------+----------------+------+-----+---------+-------+
| Field       | Type           | Null | Key | Default | Extra |
+-------------+----------------+------+-----+---------+-------+
| order_id    | bigint         | NO   | PRI | NULL    |       |
| customer_id | bigint         | YES  |     | NULL    |       |
| sku         | varbinary(128) | YES  |     | NULL    |       |
| price       | bigint         | YES  |     | NULL    |       |
+-------------+----------------+------+-----+---------+-------+
4 rows in set (0.01 sec)

mysql> insert into corder (order_id, customer_id, sku, price) values (1, 1, "Product_1", 100);
Query OK, 1 row affected (0.01 sec)

mysql> insert into corder (order_id, customer_id, sku, price) values (2, 1, "Product_2", 101);
Query OK, 1 row affected (0.01 sec)

mysql> insert into corder (order_id, customer_id, sku, price) values (3, 2, "Product_3", 102);
Query OK, 1 row affected (0.01 sec)

mysql> insert into corder (order_id, customer_id, sku, price) values (4, 3, "Product_4", 103);
Query OK, 1 row affected (0.01 sec)

mysql> insert into corder (order_id, customer_id, sku, price) values (5, 4, "Product_5", 104);
Query OK, 1 row affected (0.03 sec)

mysql> select * from corder;
+----------+-------------+-----------+-------+
| order_id | customer_id | sku       | price |
+----------+-------------+-----------+-------+
|        1 |           1 | Product_1 |   100 |
|        2 |           1 | Product_2 |   101 |
|        3 |           2 | Product_3 |   102 |
|        4 |           3 | Product_4 |   103 |
|        5 |           4 | Product_5 |   104 |
+----------+-------------+-----------+-------+
5 rows in set (0.01 sec)
```

If we look at the VSchema for the `customer.corder` table, we
will see there is a `hash` index on the `customer_id` table, 
and 4 of our 5 rows have ended up on the `-80` shard, and the
5th row on the `80-` shard:

```sql
mysql> use customer/-80
Database changed

mysql> select * from corder;
+----------+-------------+-----------+-------+
| order_id | customer_id | sku       | price |
+----------+-------------+-----------+-------+
|        1 |           1 | Product_1 |   100 |
|        2 |           1 | Product_2 |   101 |
|        3 |           2 | Product_3 |   102 |
|        4 |           3 | Product_4 |   103 |
+----------+-------------+-----------+-------+
4 rows in set (0.00 sec)

mysql> use customer/80-
Database changed

mysql> select * from corder;
+----------+-------------+-----------+-------+
| order_id | customer_id | sku       | price |
+----------+-------------+-----------+-------+
|        5 |           4 | Product_5 |   104 |
+----------+-------------+-----------+-------+
1 row in set (0.01 sec)
```

Note that this skewed distribution is completely coincidental, for larger
numbers of rows, we would expect the distribution to be approximately even
for a `hash` index.

Now let's say we want to add a lookup Vindex on the `sku` column.
We can use a `consistent_lookup` or `consistent_lookup_unique`
Vindex type.  In our example we will use `consistent_lookup_unique`.

Here is our example `<json_spec>`:

```sh
$ cat lookup_vindex.json
{
    "sharded": true,
    "vindexes": {
        "corder_lookup": {
            "type": "consistent_lookup_unique",
            "params": {
                "table": "customer.corder_lookup",
                "from": "sku",
                "to": "keyspace_id"
            },
            "owner": "corder"
        }
    },
    "tables": {
        "corder": {
            "column_vindexes": [
                {
                    "column": "sku",
                    "name": "corder_lookup"
                }
            ]
        }
    }
}
```

Note that as mentioned above, we do not have to tell Vitess about
how to shard the actual backing table for the lookup Vindex or
any schema to create as it will do it automatically.  Now, let us
actually execute the `CreateLookupVindex` command:

```sh
$ vtctlclient -server localhost:15999 CreateLookupVindex -tablet_types=RDONLY customer "$(cat lookup_vindex.json)"
```

Note:

 * We are specifying a tablet_type of `RDONLY`; meaning it is going to
run the VReplication streams from tablets of the `RDONLY` type **only**.
If tablets of this type cannot be found, in a shard, the lookup Vindex
population will fail.

Now, in our case, the table is tiny, so the copy will be instant, but
in a real-world case this might take hours.  To monitor the process,
we can use the usual VReplication commands.  However, the VReplication
status commands needs to operate on individual tablets. Let's check
which tablets we have in our environment, so we know which tablets to
issue commands against:

```sh
$ vtctlclient -server localhost:15999 ListAllTablets | grep customer
zone1-0000000300 customer -80 master localhost:15300 localhost:17300 [] 2020-08-13T01:23:15Z
zone1-0000000301 customer -80 replica localhost:15301 localhost:17301 [] <null>
zone1-0000000302 customer -80 rdonly localhost:15302 localhost:17302 [] <null>
zone1-0000000400 customer 80- master localhost:15400 localhost:17400 [] 2020-08-13T01:23:15Z
zone1-0000000401 customer 80- replica localhost:15401 localhost:17401 [] <null>
zone1-0000000402 customer 80- rdonly localhost:15402 localhost:17402 [] <null>
```

i.e. now we can see what will happen:

  * VReplication streams will be setup from the master tablets
`zone1-0000000300` and `zone1-0000000400`; pulling data from the `RDONLY`
source tablets `zone1-0000000302` and `zone1-0000000402`.
  * Note that each master tablet will start streams from each source 
tablet, for a total of 4 streams in this case.

Lets observe the VReplication streams that got created using the
`vtctlclient VReplicationExec` command.  First let's look at the streams
to the first master tablet `zone1-0000000300`:

```sql
$ vtctlclient -server localhost:15999 VReplicationExec zone1-0000000300 "select * from _vt.vreplication" 
+----+-------------------+--------------------------------------+---------------------------------------------------+----------+---------------------+---------------------+------+--------------+--------------+-----------------------+---------+---------------------+-------------+
| id |     workflow      |                source                |                        pos                        | stop_pos |       max_tps       | max_replication_lag | cell | tablet_types | time_updated | transaction_timestamp |  state  |       message       |   db_name   |
+----+-------------------+--------------------------------------+---------------------------------------------------+----------+---------------------+---------------------+------+--------------+--------------+-----------------------+---------+---------------------+-------------+
|  2 | corder_lookup_vdx | keyspace:"customer" shard:"-80"      | MySQL56/68da1cdd-dd03-11ea-95de-68a86d2718b0:1-43 |          | 9223372036854775807 | 9223372036854775807 |      | RDONLY       |   1597282811 |                     0 | Stopped | Stopped after copy. | vt_customer |
|    |                   | filter:<rules:<match:"corder_lookup" |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|    |                   | filter:"select sku as sku,           |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|    |                   | keyspace_id() as keyspace_id from    |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|    |                   | corder where in_keyrange(sku,        |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|    |                   | 'customer.binary_md5', '-80')        |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|    |                   | group by sku, keyspace_id" > >       |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|    |                   | stop_after_copy:true                 |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|  3 | corder_lookup_vdx | keyspace:"customer" shard:"80-"      | MySQL56/7d2c819e-dd03-11ea-92e4-68a86d2718b0:1-38 |          | 9223372036854775807 | 9223372036854775807 |      | RDONLY       |   1597282811 |                     0 | Stopped | Stopped after copy. | vt_customer |
|    |                   | filter:<rules:<match:"corder_lookup" |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|    |                   | filter:"select sku as sku,           |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|    |                   | keyspace_id() as keyspace_id from    |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|    |                   | corder where in_keyrange(sku,        |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|    |                   | 'customer.binary_md5', '-80')        |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|    |                   | group by sku, keyspace_id" > >       |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|    |                   | stop_after_copy:true                 |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
+----+-------------------+--------------------------------------+---------------------------------------------------+----------+---------------------+---------------------+------+--------------+--------------+-----------------------+---------+---------------------+-------------+
```

And now the streams to the second master tablet `zone1-0000000400`:

```sql
$ vtctlclient -server localhost:15999 VReplicationExec zone1-0000000400 "select * from _vt.vreplication" 
+----+-------------------+--------------------------------------+---------------------------------------------------+----------+---------------------+---------------------+------+--------------+--------------+-----------------------+---------+---------------------+-------------+
| id |     workflow      |                source                |                        pos                        | stop_pos |       max_tps       | max_replication_lag | cell | tablet_types | time_updated | transaction_timestamp |  state  |       message       |   db_name   |
+----+-------------------+--------------------------------------+---------------------------------------------------+----------+---------------------+---------------------+------+--------------+--------------+-----------------------+---------+---------------------+-------------+
|  2 | corder_lookup_vdx | keyspace:"customer" shard:"-80"      | MySQL56/68da1cdd-dd03-11ea-95de-68a86d2718b0:1-43 |          | 9223372036854775807 | 9223372036854775807 |      | RDONLY       |   1597282811 |                     0 | Stopped | Stopped after copy. | vt_customer |
|    |                   | filter:<rules:<match:"corder_lookup" |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|    |                   | filter:"select sku as sku,           |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|    |                   | keyspace_id() as keyspace_id from    |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|    |                   | corder where in_keyrange(sku,        |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|    |                   | 'customer.binary_md5', '80-')        |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|    |                   | group by sku, keyspace_id" > >       |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|    |                   | stop_after_copy:true                 |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|  3 | corder_lookup_vdx | keyspace:"customer" shard:"80-"      | MySQL56/7d2c819e-dd03-11ea-92e4-68a86d2718b0:1-38 |          | 9223372036854775807 | 9223372036854775807 |      | RDONLY       |   1597282811 |                     0 | Stopped | Stopped after copy. | vt_customer |
|    |                   | filter:<rules:<match:"corder_lookup" |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|    |                   | filter:"select sku as sku,           |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|    |                   | keyspace_id() as keyspace_id from    |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|    |                   | corder where in_keyrange(sku,        |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|    |                   | 'customer.binary_md5', '80-')        |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|    |                   | group by sku, keyspace_id" > >       |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
|    |                   | stop_after_copy:true                 |                                                   |          |                     |                     |      |              |              |                       |         |                     |             |
+----+-------------------+--------------------------------------+---------------------------------------------------+----------+---------------------+---------------------+------+--------------+--------------+-----------------------+---------+---------------------+-------------+
```

There is a lot going on in this output, but the most important parts are the 
`state` and `message` fields which say `Stopped` and `Stopped after copy.`
for all four the streams.  This means that the VReplication streams finished
their copying/backfill of the lookup table.

Note that if the tables were large and the copy was still in progress, the
`state` field would say `Copying`, and you can see the state/progress of
the copy by looking at the `_vt.copy_state` table, e.g.:

```sql
$ vtctlclient -server localhost:15999 VReplicationExec zone1-0000000300 "select * from _vt.copy_state" 
+----------+------------+--------+
| vrepl_id | table_name | lastpk |
+----------+------------+--------+
+----------+------------+--------+
```

(In this case this table is empty, because the copy has finished already).

We can verify the result of the backfill by looking at the `customer` 
keyspace again in the MySQL client:

```sql
mysql> show tables;
+-----------------------+
| Tables_in_vt_customer |
+-----------------------+
| corder                |
| corder_lookup         |
| customer              |
+-----------------------+
3 rows in set (0.01 sec)
```

Note there is now a new table, `corder_lookup`; which was created as the
backing table for the lookup Vindex.  Lets look at this table:

```sql
mysql> desc corder_lookup;
+-------------+----------------+------+-----+---------+-------+
| Field       | Type           | Null | Key | Default | Extra |
+-------------+----------------+------+-----+---------+-------+
| sku         | varbinary(128) | NO   | PRI | NULL    |       |
| keyspace_id | varbinary(128) | YES  |     | NULL    |       |
+-------------+----------------+------+-----+---------+-------+
2 rows in set (0.01 sec)

mysql> select sku, hex(keyspace_id) from corder_lookup;
+-----------+------------------+
| sku       | hex(keyspace_id) |
+-----------+------------------+
| Product_2 | 166B40B44ABA4BD6 |
| Product_3 | 06E7EA22CE92708F |
| Product_1 | 166B40B44ABA4BD6 |
| Product_4 | 4EB190C9A2FA169C |
| Product_5 | D2FD8867D50D2DFE |
+-----------+------------------+
```

Basically, this shows exactly what we expected.  Now, we can clean up the
VReplication streams.  Note these commands will clean up all VReplication
streams on these tablets. You may want to filter by `id` if there are other
streams running:

```sh
$ vtctlclient -server localhost:15999 VReplicationExec zone1-0000000300 "delete from _vt.vreplication"
+
+
$ vtctlclient -server localhost:15999 VReplicationExec zone1-0000000400 "delete from _vt.vreplication"
+
+
```

Next, to confirm the lookup Vindex is doing what we think it should, we can
use the Vitess MySQL explain format, e.g.:

```sql
mysql> explain format=vitess select * from corder where customer_id = 1;
+----------+-------------------+----------+-------------+------------+--------------------------------------------+
| operator | variant           | keyspace | destination | tabletType | query                                      |
+----------+-------------------+----------+-------------+------------+--------------------------------------------+
| Route    | SelectEqualUnique | customer |             | UNKNOWN    | select * from corder where customer_id = 1 |
+----------+-------------------+----------+-------------+------------+--------------------------------------------+
1 row in set (0.00 sec)
```

Since the above `select` statement is doing a lookup using the primary Vindex
on the `corder` table, this query does not Scatter (variant is 
`SelectEqualUnique`), as expected.  Let's try a scatter query to see what that
looks like:

```sql
mysql> explain format=vitess select * from corder;
+----------+---------------+----------+-------------+------------+----------------------+
| operator | variant       | keyspace | destination | tabletType | query                |
+----------+---------------+----------+-------------+------------+----------------------+
| Route    | SelectScatter | customer |             | UNKNOWN    | select * from corder |
+----------+---------------+----------+-------------+------------+----------------------+
1 row in set (0.00 sec)
```

OK, variant is `SelectScatter` for a scatter query.  Let's try a lookup on
a column that does not have a primary or secondary (lookup) Vindex, e.g.
the `price` column:

```sql
mysql> explain format=vitess select * from corder where price = 103;
+----------+---------------+----------+-------------+------------+----------------------------------------+
| operator | variant       | keyspace | destination | tabletType | query                                  |
+----------+---------------+----------+-------------+------------+----------------------------------------+
| Route    | SelectScatter | customer |             | UNKNOWN    | select * from corder where price = 103 |
+----------+---------------+----------+-------------+------------+----------------------------------------+
1 row in set (0.00 sec)
```

That also scatters, as expected.

Now, let's try a lookup on the `sku` column, which we have created our lookup
Vindex on:

```sql
mysql> explain format=vitess select * from corder where sku = "Product_1";
+----------+-------------------+----------+-------------+------------+----------------------------------------------+
| operator | variant           | keyspace | destination | tabletType | query                                        |
+----------+-------------------+----------+-------------+------------+----------------------------------------------+
| Route    | SelectEqualUnique | customer |             | UNKNOWN    | select * from corder where sku = 'Product_1' |
+----------+-------------------+----------+-------------+------------+----------------------------------------------+
1 row in set (0.00 sec)
```

As expected, we can see it is not scattering anymore, which it would have
before we did `CreateLookupVindex`.

Lastly, let's ensure that the lookup Vindex is being updated appropriately
when we insert and delete rows:

```sql
mysql> select * from corder;
+----------+-------------+-----------+-------+
| order_id | customer_id | sku       | price |
+----------+-------------+-----------+-------+
|        5 |           4 | Product_5 |   104 |
|        1 |           1 | Product_1 |   100 |
|        2 |           1 | Product_2 |   101 |
|        3 |           2 | Product_3 |   102 |
|        4 |           3 | Product_4 |   103 |
+----------+-------------+-----------+-------+
5 rows in set (0.00 sec)

mysql> delete from corder where customer_id = 1 and sku = "Product_1";
Query OK, 1 row affected (0.03 sec)

mysql> select * from corder;
+----------+-------------+-----------+-------+
| order_id | customer_id | sku       | price |
+----------+-------------+-----------+-------+
|        2 |           1 | Product_2 |   101 |
|        3 |           2 | Product_3 |   102 |
|        4 |           3 | Product_4 |   103 |
|        5 |           4 | Product_5 |   104 |
+----------+-------------+-----------+-------+
4 rows in set (0.01 sec)

mysql> select sku, hex(keyspace_id) from corder_lookup;
+-----------+------------------+
| sku       | hex(keyspace_id) |
+-----------+------------------+
| Product_4 | 4EB190C9A2FA169C |
| Product_5 | D2FD8867D50D2DFE |
| Product_2 | 166B40B44ABA4BD6 |
| Product_3 | 06E7EA22CE92708F |
+-----------+------------------+
4 rows in set (0.01 sec)
```

We deleted a row from the `corder` table, and the matching lookup Vindex row 
is gone.

```sql
mysql> insert into corder (order_id, customer_id, sku, price) values (6, 1, "Product_6", 105);
Query OK, 1 row affected (0.02 sec)

mysql> select * from corder;
+----------+-------------+-----------+-------+
| order_id | customer_id | sku       | price |
+----------+-------------+-----------+-------+
|        2 |           1 | Product_2 |   101 |
|        3 |           2 | Product_3 |   102 |
|        4 |           3 | Product_4 |   103 |
|        6 |           1 | Product_6 |   105 |
|        5 |           4 | Product_5 |   104 |
+----------+-------------+-----------+-------+
5 rows in set (0.00 sec)

mysql> select sku, hex(keyspace_id) from corder_lookup;
+-----------+------------------+
| sku       | hex(keyspace_id) |
+-----------+------------------+
| Product_4 | 4EB190C9A2FA169C |
| Product_5 | D2FD8867D50D2DFE |
| Product_6 | 166B40B44ABA4BD6 |
| Product_2 | 166B40B44ABA4BD6 |
| Product_3 | 06E7EA22CE92708F |
+-----------+------------------+
5 rows in set (0.00 sec)
```

We added a new row to the `corder` table, and now we have a new row in the
lookup table.

