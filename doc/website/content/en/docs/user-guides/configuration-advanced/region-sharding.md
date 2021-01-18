---
title: Region-based Sharding
weight: 10
aliases: ['/docs/user-guides/region-sharding/'] 
---

{{< info >}}
This guide follows on from the Get Started guides. Please make sure that you have a [local](../../../get-started/local) installation ready. You should also have already gone through the [MoveTables](../../migration/move-tables) and [Resharding](../../configuration-advanced/resharding) tutorials.
{{< /info >}}

## Preparation

Having gone through the Resharding tutorial, you should be familiar with [VSchema](../../../concepts/vschema) and [Vindexes](../../../reference/vindexes).
In this tutorial, we will perform resharding on an existing keyspace using a location-based vindex. We will create 4 shards (-40, 40-80, 80-c0, c0-).
The location will be denoted by a `country` column.

## Schema

We will create one table in the unsharded keyspace to start with.
```text
CREATE TABLE customer (
  id int NOT NULL,
  fullname varbinary(256),
  nationalid varbinary(256),
  country varbinary(256),
  primary key(id)
  );
```

The customer table is the main table we want to shard using country. 

## Region Vindex
We will use a `region_json` vindex to compute the keyspace_id for a customer row using the (id, country) fields.
Here's what the vindex definition looks like:
```text
    "region_vdx": {
	    "type": "region_json",
	    "params": {
	        "region_map": "/home/user/my-vitess/examples/region_sharding/countries.json",
	        "region_bytes": "1"
	    }
    },
```
And we use it thus:
```text
    "customer": {
      "column_vindexes": [
        {
            "columns": ["id", "country"],
	        "name": "region_vdx"
        },
```
This vindex uses a byte mapping of countries provided in a JSON file and combines that with the id column in the customer table to compute the keyspace_id. 
This is what the JSON file contains:
```text
{
    "United States": 1,
    "Canada": 2,
    "France": 64,
    "Germany": 65,
    "China": 128,
    "Japan": 129,
    "India": 192,
    "Indonesia": 193
}
```
The values for the countries have been chosen such that 2 countries fall into each shard.

In this example, we are using 1 byte to represent a country code. You can use 1 or 2 bytes. With 2 bytes, 65536 distinct locations can be supported. The byte value of the country(or other location identifier) is prefixed to a hash value computed from the id to produce the keyspace_id.
This will be primary vindex on the `customer` table. As such, it is sufficient for resharding, inserts and selects.
However, we don't yet support updates and deletes using a multi-column vindex.
In order for those to work, we need to create a lookup vindex that can used to find the correct rows by id.
The lookup vindex also makes querying by id efficient. Without it, queries that provided id but not country will scatter to all shards.


To do this, we will use the new vreplication workflow `CreateLookupVindex`. This workflow will create the lookup table and a lookup vindex. It will also associate the lookup vindex with the `customer` table.

## Start the Cluster

Start by copying the region_sharding example included with Vitess to your preferred location.
```sh
cp -r /usr/local/vitess/examples/region_sharding ~/my-vitess/examples/region_sharding
cd ~/my-vitess/examples/region_sharding
```

The VSchema for this tutorial uses a config file. You will need to edit the value of the `region_map` parameter in the vschema file `main_vschema_sharded.json`.
For example:
```text
"region_map": "/home/user/my-vitess/examples/region_sharding/countries.json",
```

Now start the cluster
```sh
./101_initial_cluster.sh
```

You should see output similar to the following:

```text
~/my-vitess-example> ./101_initial_cluster.sh
add /vitess/global
add /vitess/zone1
add zone1 CellInfo
etcd start done...
Starting vtctld...
Starting MySQL for tablet zone1-0000000100...
Starting vttablet for zone1-0000000100...
HTTP/1.1 200 OK
Date: Mon, 17 Aug 2020 14:20:08 GMT
Content-Type: text/html; charset=utf-8

W0817 07:20:08.822742    7735 main.go:64] W0817 14:20:08.821985 reparent.go:185] master-elect tablet zone1-0000000100 is not the shard master, proceeding anyway as -force was used
W0817 07:20:08.823004    7735 main.go:64] W0817 14:20:08.822370 reparent.go:191] master-elect tablet zone1-0000000100 is not a master in the shard, proceeding anyway as -force was used
I0817 07:20:08.823239    7735 main.go:64] I0817 14:20:08.823075 reparent.go:222] resetting replication on tablet zone1-0000000100
I0817 07:20:08.833215    7735 main.go:64] I0817 14:20:08.833019 reparent.go:241] initializing master on zone1-0000000100
I0817 07:20:08.849955    7735 main.go:64] I0817 14:20:08.849736 reparent.go:274] populating reparent journal on new master zone1-0000000100
New VSchema object:
{
  "tables": {
    "customer": {

    }
  }
}
If this is not what you expected, check the input data (as JSON parsing will skip unexpected fields).
Waiting for vtgate to be up...
vtgate is up!
Access vtgate at http://localhost:15001/debug/status
```

You can also verify that the processes have started with `pgrep`:

```bash
~/my-vitess-example> pgrep -fl vtdataroot
9160 etcd
9222 vtctld
9280 mysqld_safe
9843 mysqld
9905 vttablet
10040 vtgate
10224 mysqld
```

_The exact list of processes will vary. For example, you may not see `mysqld_safe` listed._

If you encounter any errors, such as ports already in use, you can kill the processes and start over:

```sh
pkill -9 -e -f '(vtdataroot|VTDATAROOT)' # kill Vitess processes
rm -rf vtdataroot
```

## Aliases

For ease-of-use, Vitess provides aliases for `mysql` and `vtctlclient`. These are automatically created when you start the cluster.
```bash
source ./env.sh
```

Setting up aliases changes `mysql` to always connect to Vitess for your current session. To revert this, type `unalias mysql && unalias vtctlclient` or close your session.

## Connect to your cluster

You should now be able to connect to the VTGate server that was started in `101_initial_cluster.sh`:

```bash
~/my-vitess-example> mysql
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 2
Server version: 5.7.9-Vitess (Ubuntu)

Copyright (c) 2000, 2019, Oracle and/or its affiliates. All rights reserved.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> show tables;
+-------------------+
| Tables_in_vt_main |
+-------------------+
| customer          |
+-------------------+
1 row in set (0.01 sec)
```

## Insert some data into the cluster

```bash
~/my-vitess-example> mysql < insert_customers.sql
```

## Examine the data we just inserted

```bash
~/my-vitess-example> mysql --table < show_initial_data.sql
```

```text
+----+------------------+-------------+---------------+
| id | fullname         | nationalid  | country       |
+----+------------------+-------------+---------------+
|  1 | Philip Roth      | 123-456-789 | United States |
|  2 | Gary Shteyngart  | 234-567-891 | United States |
|  3 | Margaret Atwood  | 345-678-912 | Canada        |
|  4 | Alice Munro      | 456-789-123 | Canada        |
|  5 | Albert Camus     | 912-345-678 | France        |
|  6 | Colette          | 102-345-678 | France        |
|  7 | Hermann Hesse    | 304-567-891 | Germany       |
|  8 | Cornelia Funke   | 203-456-789 | Germany       |
|  9 | Cixin Liu        | 789-123-456 | China         |
| 10 | Jian Ma          | 891-234-567 | China         |
| 11 | Haruki Murakami  | 405-678-912 | Japan         |
| 12 | Banana Yoshimoto | 506-789-123 | Japan         |
| 13 | Arundhati Roy    | 567-891-234 | India         |
| 14 | Shashi Tharoor   | 678-912-345 | India         |
| 15 | Andrea Hirata    | 607-891-234 | Indonesia     |
| 16 | Ayu Utami        | 708-912-345 | Indonesia     |
+----+------------------+-------------+---------------+
```

## Prepare for resharding

Now that we have some data in our unsharded cluster, let us go ahead and perform the setup needed for resharding.
The initial vschema is unsharded and simply lists the customer table (see script output above).
We are going to first apply the sharding vschema to the cluster from `main_vschema_sharded.json`
```text
{
  "sharded": true,
  "vindexes": {
    "region_vdx": {
      "type": "region_json",
      "params": {
        "region_map": "/home/user/my-vitess/examples/region_sharding/countries.json",
        "region_bytes": "1"
      }
    }
  },
  "tables": {
    "customer": {
      "column_vindexes": [
        {
          "columns": ["id", "country"],
              "name": "region_vdx"
        }
      ]
    }
  }
}
```
Then we will create a lookup vindex (`CreateLookupVindex`) using the definition in `lookup_vindex.json`

Here is the lookup vindex definition. Here we both define the lookup vindex, and associate it with the customer table.
```text
{
  "sharded": true,
  "vindexes": {
    "customer_region_lookup": {
      "type": "consistent_lookup_unique",
      "params": {
        "table": "main.customer_lookup",
        "from": "id",
        "to": "keyspace_id"
      },
      "owner": "customer"
    }
  },
  "tables": {
    "customer": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "customer_region_lookup"
        }
      ]
    }
  }
}
```
Once the vindex is available, we have to `Externalize` it for it to be usable.
Putting this all together, we run the script that combines the above steps.

```sh
./201_main_sharded.sh
```
Once this is complete, we can view the new vschema. Note that it now includes both region_vdx and a lookup vindex.
```text
~/my-vitess-example> vtctlclient GetVSchema main
{
  "sharded": true,
  "vindexes": {
    "customer_region_lookup": {
      "type": "consistent_lookup_unique",
      "params": {
        "from": "id",
        "table": "main.customer_lookup",
        "to": "keyspace_id"
      },
      "owner": "customer"
    },
    "hash": {
      "type": "hash"
    },
    "region_vdx": {
      "type": "region_json",
      "params": {
        "region_bytes": "1",
        "region_map": "/home/user/my-vitess/examples/region_sharding/countries.json"
      }
    }
  },
  "tables": {
    "customer": {
      "columnVindexes": [
        {
          "name": "region_vdx",
          "columns": [
            "id",
            "country"
          ]
        },
        {
          "column": "id",
          "name": "customer_region_lookup"
        }
      ]
    },
    "customer_lookup": {
      "columnVindexes": [
        {
          "column": "id",
          "name": "hash"
        }
      ]
    }
  }
}
```
Notice that the vschema shows a hash vindex on the lookup table. This is automatically created by the workflow.
Creating a lookup vindex via `CreateLookupVindex` also creates the backing table needed to hold the vindex, and populates it with the correct rows.
We can see that by checking the database.

```text
mysql> show tables;
+-------------------+
| Tables_in_vt_main |
+-------------------+
| customer          |
| customer_lookup   |
+-------------------+
2 rows in set (0.00 sec)

mysql> describe customer_lookup;
+-------------+----------------+------+-----+---------+-------+
| Field       | Type           | Null | Key | Default | Extra |
+-------------+----------------+------+-----+---------+-------+
| id          | int(11)        | NO   | PRI | NULL    |       |
| keyspace_id | varbinary(128) | YES  |     | NULL    |       |
+-------------+----------------+------+-----+---------+-------+
2 rows in set (0.01 sec)

mysql> select id, hex(keyspace_id) from customer_lookup;
+----+--------------------+
| id | hex(keyspace_id)   |
+----+--------------------+
|  1 | 01166B40B44ABA4BD6 |
|  2 | 0106E7EA22CE92708F |
|  3 | 024EB190C9A2FA169C |
|  4 | 02D2FD8867D50D2DFE |
|  5 | 4070BB023C810CA87A |
|  6 | 40F098480AC4C4BE71 |
|  7 | 41FB8BAAAD918119B8 |
|  8 | 41CC083F1E6D9E85F6 |
|  9 | 80692BB9BF752B0F58 |
| 10 | 80594764E1A2B2D98E |
| 11 | 81AEFC44491CFE474C |
| 12 | 81D3748269B7058A0E |
| 13 | C062DCE203C602F358 |
| 14 | C0ACBFDA0D70613FC4 |
| 15 | C16A8B56ED414942B8 |
| 16 | C15B711BC4CEEBF2EE |
+----+--------------------+
16 rows in set (0.01 sec)
```

Once the sharding vschema and lookup vindex (+table) are ready, we can bring up the sharded cluster.
Since we have 4 shards, we will bring up 4 sets of vttablets, 1 per shard. In this example, we are deploying only 1 tablet per shard and disabling semi-sync, but in general each shard will consist of at least 3 tablets.
```bash
./202_new_tablets.sh
```
```text
Starting MySQL for tablet zone1-0000000200...
Starting vttablet for zone1-0000000200...
HTTP/1.1 200 OK
Date: Mon, 17 Aug 2020 15:07:41 GMT
Content-Type: text/html; charset=utf-8

Starting MySQL for tablet zone1-0000000300...
Starting vttablet for zone1-0000000300...
HTTP/1.1 200 OK
Date: Mon, 17 Aug 2020 15:07:46 GMT
Content-Type: text/html; charset=utf-8

Starting MySQL for tablet zone1-0000000400...
Starting vttablet for zone1-0000000400...
HTTP/1.1 200 OK
Date: Mon, 17 Aug 2020 15:07:50 GMT
Content-Type: text/html; charset=utf-8

Starting MySQL for tablet zone1-0000000500...
Starting vttablet for zone1-0000000500...
HTTP/1.1 200 OK
Date: Mon, 17 Aug 2020 15:07:55 GMT
Content-Type: text/html; charset=utf-8

W0817 08:07:55.217317   15230 main.go:64] W0817 15:07:55.215654 reparent.go:185] master-elect tablet zone1-0000000200 is not the shard master, proceeding anyway as -force was used
W0817 08:07:55.218083   15230 main.go:64] W0817 15:07:55.215771 reparent.go:191] master-elect tablet zone1-0000000200 is not a master in the shard, proceeding anyway as -force was used
I0817 08:07:55.218121   15230 main.go:64] I0817 15:07:55.215918 reparent.go:222] resetting replication on tablet zone1-0000000200
I0817 08:07:55.229794   15230 main.go:64] I0817 15:07:55.229416 reparent.go:241] initializing master on zone1-0000000200
I0817 08:07:55.249680   15230 main.go:64] I0817 15:07:55.249325 reparent.go:274] populating reparent journal on new master zone1-0000000200
W0817 08:07:55.286894   15247 main.go:64] W0817 15:07:55.286288 reparent.go:185] master-elect tablet zone1-0000000300 is not the shard master, proceeding anyway as -force was used
W0817 08:07:55.287392   15247 main.go:64] W0817 15:07:55.286354 reparent.go:191] master-elect tablet zone1-0000000300 is not a master in the shard, proceeding anyway as -force was used
I0817 08:07:55.287411   15247 main.go:64] I0817 15:07:55.286448 reparent.go:222] resetting replication on tablet zone1-0000000300
I0817 08:07:55.300499   15247 main.go:64] I0817 15:07:55.300276 reparent.go:241] initializing master on zone1-0000000300
I0817 08:07:55.324774   15247 main.go:64] I0817 15:07:55.324454 reparent.go:274] populating reparent journal on new master zone1-0000000300
W0817 08:07:55.363497   15264 main.go:64] W0817 15:07:55.362451 reparent.go:185] master-elect tablet zone1-0000000400 is not the shard master, proceeding anyway as -force was used
W0817 08:07:55.364061   15264 main.go:64] W0817 15:07:55.362569 reparent.go:191] master-elect tablet zone1-0000000400 is not a master in the shard, proceeding anyway as -force was used
I0817 08:07:55.364079   15264 main.go:64] I0817 15:07:55.362689 reparent.go:222] resetting replication on tablet zone1-0000000400
I0817 08:07:55.378370   15264 main.go:64] I0817 15:07:55.378201 reparent.go:241] initializing master on zone1-0000000400
I0817 08:07:55.401258   15264 main.go:64] I0817 15:07:55.400569 reparent.go:274] populating reparent journal on new master zone1-0000000400
W0817 08:07:55.437158   15280 main.go:64] W0817 15:07:55.435986 reparent.go:185] master-elect tablet zone1-0000000500 is not the shard master, proceeding anyway as -force was used
W0817 08:07:55.437953   15280 main.go:64] W0817 15:07:55.436038 reparent.go:191] master-elect tablet zone1-0000000500 is not a master in the shard, proceeding anyway as -force was used
I0817 08:07:55.437982   15280 main.go:64] I0817 15:07:55.436107 reparent.go:222] resetting replication on tablet zone1-0000000500
I0817 08:07:55.449958   15280 main.go:64] I0817 15:07:55.449725 reparent.go:241] initializing master on zone1-0000000500
I0817 08:07:55.467790   15280 main.go:64] I0817 15:07:55.466993 reparent.go:274] populating reparent journal on new master zone1-0000000500
```

## Perform Resharding

Once the tablets are up, we can go ahead with the resharding.
```bash
./203_reshard.sh
```
This script has only one command: `Reshard`
```bash
vtctlclient Reshard -tablet_types=MASTER main.main2regions '0' '-40,40-80,80-c0,c0-'
```
Let us unpack this a bit. Since we are running only master tablets in this cluster, we have to tell the `Reshard` command to use them as the source for copying data into the target shards.
The next argument is of the form `keyspace.workflow`. `keyspace` is the one we want to reshard. `workflow` is an identifier chosen by the user. It can be any arbitrary string and is used to tie the different steps of the resharding flow together.
We will see it being used in subsequent steps.
Then we have the source shard `0` and target shards `-40,40-80,80-c0,c0-`

This step copies all the data from source to target and sets up vreplication to keep the targets in sync with the source

We can check the correctness of the copy using `VDiff` and the `keyspace.workflow` we used for `Reshard`
```bash
vtctlclient VDiff main.main2regions
I0817 08:22:53.958578   16065 main.go:64] I0817 15:22:53.956743 traffic_switcher.go:389] Migration ID for workflow main2regions: 7369191857547657706
Summary for customer: {ProcessedRows:16 MatchingRows:16 MismatchedRows:0 ExtraRowsSource:0 ExtraRowsTarget:0}
```
Let's take a look at the vreplication streams
```bash
vtctlclient VReplicationExec zone1-0000000200 'select * from _vt.vreplication'
+----+--------------+--------------------------------+---------------------------------------------------+----------+---------------------+---------------------+------+--------------+--------------+-----------------------+---------+---------+---------+
| id |   workflow   |             source             |                        pos                        | stop_pos |       max_tps       | max_replication_lag | cell | tablet_types | time_updated | transaction_timestamp |  state  | message | db_name |
+----+--------------+--------------------------------+---------------------------------------------------+----------+---------------------+---------------------+------+--------------+--------------+-----------------------+---------+---------+---------+
|  1 | main2regions | keyspace:"main" shard:"0"      | MySQL56/cd3b495a-e096-11ea-9088-34e12d1e6711:1-44 |          | 9223372036854775807 | 9223372036854775807 |      | MASTER       |   1597676983 |                     0 | Running |         | vt_main |
|    |              | filter:<rules:<match:"/.*"     |                                                   |          |                     |                     |      |              |              |                       |         |         |         |
|    |              | filter:"-40" > >               |                                                   |          |                     |                     |      |              |              |                       |         |         |         |
+----+--------------+--------------------------------+---------------------------------------------------+----------+---------------------+---------------------+------+--------------+----
```
We have a running stream on tablet 200 (shard `-40`) that will keep it up-to-date with the source shard (`0`)

## Cutover

Once the copy process is complete, we can start cutting-over traffic.
This is done in 2 steps, `SwitchReads` and `SwitchWrites`. Note that the commands are named for the tablet_types and not user operations.
`Reads` is used for replica/rdonly, and `Writes` for master. Read operations on master will not be affected by a `SwitchReads`.

```bash
./204_switch_reads.sh
./205_switch_writes.sh
```

Let us take a look at the sharded data
```text
mysql> use main/-40;
Database changed

mysql> select * from customer;
+----+-----------------+-------------+---------------+
| id | fullname        | nationalid  | country       |
+----+-----------------+-------------+---------------+
|  1 | Philip Roth     | 123-456-789 | United States |
|  2 | Gary Shteyngart | 234-567-891 | United States |
|  3 | Margaret Atwood | 345-678-912 | Canada        |
|  4 | Alice Munro     | 456-789-123 | Canada        |
+----+-----------------+-------------+---------------+
4 rows in set (0.01 sec)

mysql> select id,hex(keyspace_id) from customer_lookup;
+----+--------------------+
| id | hex(keyspace_id)   |
+----+--------------------+
|  1 | 01166B40B44ABA4BD6 |
|  2 | 0106E7EA22CE92708F |
+----+--------------------+
2 rows in set (0.00 sec)
```
You can see that only data from US and Canada exists in the customer table in this shard. 
Repeat this for the other shards (40-80, 80-c0 and c0-) and see that each shard contains 4 rows in customer table.

The lookup table, however, has a different number of rows.
This is because we are using a `hash` vindex to shard the lookup table which means that it is distributed differently from the customer table.
If we look at the next shard 40-80:

```text
mysql> use main/40-80;

Database changed
mysql> select id, hex(keyspace_id) from customer_lookup;
+----+--------------------+
| id | hex(keyspace_id)   |
+----+--------------------+
|  3 | 024EB190C9A2FA169C |
|  5 | 4070BB023C810CA87A |
|  9 | 80692BB9BF752B0F58 |
| 10 | 80594764E1A2B2D98E |
| 13 | C062DCE203C602F358 |
| 15 | C16A8B56ED414942B8 |
| 16 | C15B711BC4CEEBF2EE |
+----+--------------------+
7 rows in set (0.00 sec)
```

## Drop source

Once resharding is complete, we can teardown the source shard
```bash
./206_down_shard_0.sh
./207_delete_shard_0.sh
```
What we have now is a sharded keyspace. The original unsharded keyspace no longer exists.

## Teardown

Once you are done playing with the example, you can tear it down completely.

```bash
./301_teardown.sh
rm -rf vtdataroot
```
