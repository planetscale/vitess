---
title: MoveTables
weight: 2
aliases: ['/docs/user-guides/move-tables/'] 
---

{{< info >}}
This guide follows on from the Get Started guides. Please make sure that you have an [Operator](../../../get-started/operator), [local](../../../get-started/local) or [Helm](../../../get-started/helm) installation ready after the `101_initial_cluster` step, and making sure you have setup aliases and port-forwarding (if necessary).
{{< /info >}}

[MoveTables](../../../concepts/move-tables) is a new VReplication workflow in Vitess 6 and later, and obsoletes Vertical Split from earlier releases.

This feature enables you to move a subset of tables between keyspaces without downtime. For example, after [Initially deploying Vitess](../../../get-started/local), your single commerce schema may grow so large that it needs to be split into multiple keyspaces.

As a stepping stone towards splitting a single table across multiple servers (sharding), it usually makes sense to first split from having a single monolithic keyspace (`commerce`) to having multiple keyspaces (`commerce` and `customer`). For example, in our hypothetical ecommerce system we may know that `customer` and `corder` tables are closely related and both growing quickly.

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
customer
+-------------+--------------------+
| customer_id | email              |
+-------------+--------------------+
|           1 | alice@domain.com   |
|           2 | bob@domain.com     |
|           3 | charlie@domain.com |
|           4 | dan@domain.com     |
|           5 | eve@domain.com     |
+-------------+--------------------+
product
+----------+-------------+-------+
| sku      | description | price |
+----------+-------------+-------+
| SKU-1001 | Monitor     |   100 |
| SKU-1002 | Keyboard    |    30 |
+----------+-------------+-------+
corder
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

Notice that we are using keyspace `commerce/0` to select data from our tables.

## Planning to Move Tables

In this scenario, we are going to add the `customer` keyspace to the `commerce` keyspace we already have.  This new keyspace will be backed by its own set of mysqld instances. We will then move the tables `customer` and `corder` from the `commerce` keyspace into the newly created `customer`. The `product` table will remain in the `commerce` keyspace. This operation happens online, which means that it does not block either read or write operations to the tables, __except__ for a small window during the final cut-over.

## Show our current tablets

```sh
$ echo "show vitess_tablets;" | mysql --table
+-------+----------+-------+------------+---------+------------------+-----------+----------------------+
| Cell  | Keyspace | Shard | TabletType | State   | Alias            | Hostname  | MasterTermStartTime  |
+-------+----------+-------+------------+---------+------------------+-----------+----------------------+
| zone1 | commerce | 0     | MASTER     | SERVING | zone1-0000000100 | localhost | 2020-08-26T00:37:21Z |
| zone1 | commerce | 0     | REPLICA    | SERVING | zone1-0000000101 | localhost |                      |
| zone1 | commerce | 0     | RDONLY     | SERVING | zone1-0000000102 | localhost |                      |
+-------+----------+-------+------------+---------+------------------+-----------+----------------------+
```

As can be seen, we have 3 tablets running, with tablet ids 100, 101 and 102;  which we use in the examples to form the tablet alias/names like `zone1-0000000100`, etc.

## Create new tablets

The first step in our MoveTables operation is to deploy new tablets for our `customer` keyspace. By the convention used in our examples, we are going to use the tablet ids 200-202 as the `commerce` keyspace previously used `100-102`. Once the tablets have started, we can force the first tablet to be the master using the `InitShardMaster` `-force` flag:

### Using Helm

```bash
helm upgrade vitess ../../helm/vitess/ -f 201_customer_tablets.yaml
```

After a few minutes the pods should appear running:

```bash
$ kubectl get pods,jobs
NAME                                           READY   STATUS      RESTARTS   AGE
pod/vtctld-58bd955948-pgz7k                    1/1     Running     0          5m36s
pod/vtgate-zone1-c7444bbf6-t5xc6               1/1     Running     3          5m36s
pod/zone1-commerce-0-init-shard-master-gshz9   0/1     Completed   0          5m35s
pod/zone1-commerce-0-replica-0                 2/2     Running     0          5m35s
pod/zone1-commerce-0-replica-1                 2/2     Running     0          5m35s
pod/zone1-commerce-0-replica-2                 2/2     Running     0          5m35s
pod/zone1-customer-0-init-shard-master-7w7rm   0/1     Completed   0          84s
pod/zone1-customer-0-replica-0                 2/2     Running     0          84s
pod/zone1-customer-0-replica-1                 2/2     Running     0          84s
pod/zone1-customer-0-replica-2                 2/2     Running     0          84s

NAME                                           COMPLETIONS   DURATION   AGE
job.batch/zone1-commerce-0-init-shard-master   1/1           90s        5m36s
job.batch/zone1-customer-0-init-shard-master   1/1           23s        84s
```

`InitShardMaster` is performed implicitly by Helm for you.

### Using Operator

```bash
kubectl apply -f 201_customer_tablets.yaml
```

After a few minutes the pods should appear running:

```bash
$ kubectl get pods
NAME                                             READY   STATUS    RESTARTS   AGE
example-etcd-faf13de3-1                          1/1     Running   0          8m11s
example-etcd-faf13de3-2                          1/1     Running   0          8m11s
example-etcd-faf13de3-3                          1/1     Running   0          8m11s
example-vttablet-zone1-1250593518-17c58396       3/3     Running   1          2m20s
example-vttablet-zone1-2469782763-bfadd780       3/3     Running   1          7m57s
example-vttablet-zone1-2548885007-46a852d0       3/3     Running   1          7m47s
example-vttablet-zone1-3778123133-6f4ed5fc       3/3     Running   1          2m20s
example-zone1-vtctld-1d4dcad0-59d8498459-kdml8   1/1     Running   1          8m11s
example-zone1-vtgate-bc6cde92-6bd99c6888-csnkj   1/1     Running   2          8m11s
vitess-operator-8454d86687-4wfnc                 1/1     Running   0          22m
```

Again, the operator will perform `InitShardMaster` implicitly for you.

Make sure that you restart the port-forward after launching the pods has completed:

```bash
killall kubectl
./pf.sh &
```

### Using a Local Deployment

```bash
for i in 200 201 202; do
 CELL=zone1 TABLET_UID=$i ./scripts/mysqlctl-up.sh
 CELL=zone1 KEYSPACE=customer TABLET_UID=$i ./scripts/vttablet-up.sh
done

vtctlclient InitShardMaster -force customer/0 zone1-200
vtctlclient ReloadSchemaKeyspace customer
```

## Show our old and new tablets

```sh
$ echo "show vitess_tablets;" | mysql --table
+-------+----------+-------+------------+---------+------------------+-----------+----------------------+
| Cell  | Keyspace | Shard | TabletType | State   | Alias            | Hostname  | MasterTermStartTime  |
+-------+----------+-------+------------+---------+------------------+-----------+----------------------+
| zone1 | commerce | 0     | MASTER     | SERVING | zone1-0000000100 | localhost | 2020-08-26T00:37:21Z |
| zone1 | commerce | 0     | REPLICA    | SERVING | zone1-0000000101 | localhost |                      |
| zone1 | commerce | 0     | RDONLY     | SERVING | zone1-0000000102 | localhost |                      |
| zone1 | customer | 0     | MASTER     | SERVING | zone1-0000000200 | localhost | 2020-08-26T00:52:39Z |
| zone1 | customer | 0     | REPLICA    | SERVING | zone1-0000000201 | localhost |                      |
| zone1 | customer | 0     | RDONLY     | SERVING | zone1-0000000202 | localhost |                      |
+-------+----------+-------+------------+---------+------------------+-----------+----------------------+
```


__Note:__ The following change does not change actual routing yet. We will use a _switch_ directive to achieve that shortly.

## Start the Move

In this step we will initiate the MoveTables, which copies tables from the commerce keyspace into customer. This operation does not block any database activity; the MoveTables operation is performed online:

```bash
$ vtctlclient MoveTables -workflow=commerce2customer commerce customer '{"customer":{}, "corder":{}}'
```

You can read this command as:  "Start copying the tables called **customer** and **corder** from the **commerce** keyspace to the **customer** keyspace."

A few things to note:

 * In a real-world situation this process might take hours/days to complete if the table has millions or billions of rows.
 * The workflow name (`commerce2customer` in this case) is arbitrary, you can name it whatever you want.  You will use this handle/alias for the other `MoveTables` related commands like `SwitchReads` and `SwitchWrites` in the next steps.

## Check routing rules (optional)

To see what happens under the covers, let's look at the **routing rules** that the `MoveTables` operation created.  These are instructions used by VTGate to determine which backend keyspace to send requests for a given table or schema/table combo:

```sh
$ vtctlclient GetRoutingRules commerce
{
  "rules": [
    {
      "fromTable": "customer",
      "toTables": [
        "commerce.customer"
      ]
    },
    {
      "fromTable": "customer.customer",
      "toTables": [
        "commerce.customer"
      ]
    },
    {
      "fromTable": "corder",
      "toTables": [
        "commerce.corder"
      ]
    },
    {
      "fromTable": "customer.corder",
      "toTables": [
        "commerce.corder"
      ]
    }
  ]
}
```

Basically what the `MoveTables` operation has done is to create routing rules to explicitly route queries to the tables `customer` and `corder`, as well as the schema/table combos of `customer.customer` and `customer.corder` to the respective tables in the `commerce` keyspace.  This is done so that when `MoveTables` creates the new copy of the tables in the `customer` keyspace, there is no ambiguity about where to route requests for the `customer` and `corder` tables.  All requests for those tables will keep going to the original instance of those tables in `commerce` keyspace.  Any changes to the tables after the `MoveTables` is executed will be copied faithfully to the new copy of these tables in the `customer` keyspace.

## Monitoring Progress (optional)

In this example there are only a few rows in the tables, so the `MoveTables` operation only takes seconds. If the tables were large, you may need to monitor the progress of the operation.  There is no simple way to get a percentage complete status, but you can estimate the progress by running the following against the master tablet of the target keyspace:

```sh
$ vtctlclient VReplicationExec zone1-0000000200 "select * from _vt.copy_state"
+----------+------------+--------+
| vrepl_id | table_name | lastpk |
+----------+------------+--------+
+----------+------------+--------+
```

In the above case the copy is already complete, but if it was still ongoing, there would be details about the last PK (primary key) copied by the VReplication copy process.  You could use information about the last copied PK along with the max PK and data distribution of the source table to estimate progress.

## Validate Correctness (optional)

We can use VDiff to checksum the two sources and confirm they are in sync:

```bash
$ vtctlclient VDiff customer.commerce2customer
```

You should see output similar to the following:

```bash
Summary for corder: {ProcessedRows:5 MatchingRows:5 MismatchedRows:0 ExtraRowsSource:0 ExtraRowsTarget:0}
Summary for customer: {ProcessedRows:5 MatchingRows:5 MismatchedRows:0 ExtraRowsSource:0 ExtraRowsTarget:0}
```

This can obviously take a long time on very large tables.

## Phase 1: Switch Reads

Once the MoveTables operation is complete, the first step in making the changes live is to _switch_ `SELECT` statements to read from the new keyspace. Other statements will continue to route to the `commerce` keyspace. By staging this as two operations, Vitess allows you to test the changes and reduce the associated risks. For example, you may have a different configuration of hardware or software on the new keyspace.

```bash
vtctlclient SwitchReads -tablet_type=rdonly customer.commerce2customer
vtctlclient SwitchReads -tablet_type=replica customer.commerce2customer
```

## Interlude: check the routing rules (optional)

Lets look at what has happened to the routing rules since we checked the last time.  The two `SwitchReads` commands above added a number of new routing rules for the tables involved in the `MoveTables` operation/workflow, e.g.:

```sh
$ vtctlclient GetRoutingRules commerce
{
  "rules": [
    {
      "fromTable": "commerce.corder@rdonly",
      "toTables": [
        "customer.corder"
      ]
    },
    {
      "fromTable": "commerce.corder@replica",
      "toTables": [
        "customer.corder"
      ]
    },
    {
      "fromTable": "customer.customer@rdonly",
      "toTables": [
        "customer.customer"
      ]
    },
    {
      "fromTable": "customer@rdonly",
      "toTables": [
        "customer.customer"
      ]
    },
    {
      "fromTable": "commerce.customer@replica",
      "toTables": [
        "customer.customer"
      ]
    },
    {
      "fromTable": "corder",
      "toTables": [
        "commerce.corder"
      ]
    },
    {
      "fromTable": "customer.corder@replica",
      "toTables": [
        "customer.corder"
      ]
    },
    {
      "fromTable": "customer.customer@replica",
      "toTables": [
        "customer.customer"
      ]
    },
    {
      "fromTable": "customer.corder",
      "toTables": [
        "commerce.corder"
      ]
    },
    {
      "fromTable": "corder@rdonly",
      "toTables": [
        "customer.corder"
      ]
    },
    {
      "fromTable": "customer.corder@rdonly",
      "toTables": [
        "customer.corder"
      ]
    },
    {
      "fromTable": "customer",
      "toTables": [
        "commerce.customer"
      ]
    },
    {
      "fromTable": "customer.customer",
      "toTables": [
        "commerce.customer"
      ]
    },
    {
      "fromTable": "commerce.customer@rdonly",
      "toTables": [
        "customer.customer"
      ]
    },
    {
      "fromTable": "corder@replica",
      "toTables": [
        "customer.corder"
      ]
    },
    {
      "fromTable": "customer@replica",
      "toTables": [
        "customer.customer"
      ]
    }
  ]
}
```

As you can see, we now have requests to the `rdonly` and `replica` tablets for the source `commerce` keyspace being redirected to the in-sync copy of the table in the target `customer` keyspace.

## Phase 2: Switch Writes

After the reads have been _switched_, and you have verified that the system is operating as expected, it is time to _switch_ the _write_ operations. The command to execute the switch is very similar to switching reads:

```bash
$ vtctlclient SwitchWrites customer.commerce2customer
```

## Interlude: check the routing rules (optional)

Again, if we look at the routing rules after the `SwitchWrites` process, we will find that it has been cleaned up, and replaced with a blanket redirect for the moved tables (`customer` and `corder`) from the source keyspace (`commerce`) to the target keyspace (`customer`), e.g.:

```sh
$ vtctlclient GetRoutingRules commerce
{
  "rules": [
    {
      "fromTable": "commerce.customer",
      "toTables": [
        "customer.customer"
      ]
    },
    {
      "fromTable": "customer",
      "toTables": [
        "customer.customer"
      ]
    },
    {
      "fromTable": "commerce.corder",
      "toTables": [
        "customer.corder"
      ]
    },
    {
      "fromTable": "corder",
      "toTables": [
        "customer.corder"
      ]
    }
  ]
}
```

## Reverse workflow

As part of the `SwitchWrites` operation above, Vitess will automatically (unless you supply the `-reverse_replication false` flag) setup a reverse VReplication workflow to copy changes now applied to the moved tables in the target keyspace (i.e. tables `customer` and `corder` in the `customer` keyspace) back to the original source tables in the source keyspace (`customer`).  This allows us to reverse the process using additional `SwitchReads` and `SwitchWrites` commands without data loss, even after we have started writing to the new copy of the table in the new keyspace.  Note that the workflow for this reverse process is given the name of the original workflow with `_reverse` appended.  So in our example where the MoveTables workflow was called `commerce2customer`;  the reverse workflow would be `commerce2customer_reverse`.

## Drop Sources

The final step is to remove the data from the original keyspace. As well as freeing space on the original tablets, this is an important step to eliminate potential future confusions. If you have a misconfiguration down the line and accidentally route queries for the  `customer` and `corder` tables to `commerce`, it is much better to return a "table not found" error, rather than return stale data:

```sh
$ vtctlclient DropSources customer.commerce2customer
```

After this step is complete, you should see an error similar to:

```sh
# Expected to fail!
mysql --table < ../common/select_commerce_data.sql
Using commerce/0
Customer
ERROR 1146 (42S02) at line 4: vtgate: http://localhost:15001/: target: commerce.0.master, used tablet: zone1-100 
(localhost): vttablet: rpc error: code = NotFound desc = Table 'vt_commerce.customer' doesn't exist (errno 1146) 
(sqlstate 42S02) (CallerID: userData1): Sql: "select * from customer", BindVars: {}
```

This confirms that the data has been correctly cleaned up.  Note that the `DropSources` process also cleans up the reverse VReplication workflow mentioned above.  Finally, the only thing that is not cleaned up is the explicit routing rules from the source keyspace to the target keyspace.  The assumption is that you might still have applications that refer to the tables by their explicit `schema.table` designation, and you want these applications to (still) transparently be forwarded to the new location of the data.  When you are absolutely sure that no applications are using this access pattern, you can clean up the routing rules by manually adjusting the routing rules via the `vtctlclient ApplyRoutingRules` command.

## Next Steps

Congratulations! You've successfully moved tables between keyspaces. The next step to try out is to shard one of your keyspaces in [Resharding](../../configuration-advanced/resharding).
