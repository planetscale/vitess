---
title: Helm Chart (deprecated)
weight: 6
featured: true
---

This tutorial is deprecated. We recommend that you use the [Operator](../operator) instead.

This tutorial demonstrates how Vitess can be used with Minikube to deploy Vitess clusters using Helm.

## Prerequisites

Before we get started, let’s get a few things out of the way:

1. Install [Minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/) and start a Minikube engine:

    ```bash
    minikube start
    ```

2. Install [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/) and ensure it is in your `PATH`. For example, on Linux:

    ```bash
    curl -LO https://storage.googleapis.com/kubernetes-release/release/`curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt`/bin/linux/amd64/kubectl
    ```

3. Install [Helm 3](https://helm.sh/):

    ```bash
    wget https://get.helm.sh/helm-v3.2.1-linux-amd64.tar.gz
    tar -xzf helm-v3.*
    # copy linux-amd64/helm into your path
    ```

4. Install the MySQL client locally. For example, on Ubuntu:

    ```bash
    apt install mysql-client
    ```

5. Install vtctlclient locally:

    If you are familiar with Go development, the easiest way to do this is:
    ```bash
    go get vitess.io/vitess/go/cmd/vtctlclient
    ```
    If not, you can also [download the latest Vitess release](https://github.com/vitessio/vitess/releases) and extract `vtctlclient` from it.


## Start a single keyspace cluster

So you searched keyspace on Google and got a bunch of stuff about NoSQL… what’s the deal? It took a few hours, but after diving through the ancient Vitess scrolls you figure out that in the NewSQL world, keyspaces and databases are essentially the same thing when unsharded. Finally, it’s time to get started.

Change to the helm example directory:

```sh
git clone git@github.com:vitessio/vitess.git
cd vitess/examples/helm
```

In this directory, you will see a group of yaml files. The first digit of each file name indicates the phase of example. The next two digits indicate the order in which to execute them. For example, `101_initial_cluster.yaml` is the first file of the first phase. We shall execute that now:

```sh
helm install vitess ../../helm/vitess -f 101_initial_cluster.yaml
```

You should see output similar to the following:

```sh
$ helm install vitess ../../helm/vitess -f 101_initial_cluster.yaml

NAME: vitess
LAST DEPLOYED: Tue Apr 14 20:32:18 2020
NAMESPACE: default
STATUS: deployed
REVISION: 1
TEST SUITE: None
NOTES:
Release name: vitess

To access administrative web pages, start a proxy with:
  kubectl proxy --port=8001

Then use the following URLs:

      vtctld: http://localhost:8001/api/v1/namespaces/default/services/vtctld:web/proxy/app/
      vtgate: http://localhost:8001/api/v1/namespaces/default/services/vtgate-zone1:web/proxy/

```

### Verify cluster

You can check the state of your cluster with `kubectl get pods,jobs`. After a few minutes, it should show that all pods are in the status of running:

```sh
$ kubectl get pods,jobs
NAME                                           READY   STATUS      RESTARTS   AGE
pod/commerce-apply-schema-initial-vlc6k        0/1     Completed   0          2m42s
pod/commerce-apply-vschema-initial-9wb2k       0/1     Completed   0          2m42s
pod/vtctld-58bd955948-pgz7k                    1/1     Running     0          2m43s
pod/vtgate-zone1-c7444bbf6-t5xc6               1/1     Running     3          2m43s
pod/zone1-commerce-0-init-shard-master-gshz9   0/1     Completed   0          2m42s
pod/zone1-commerce-0-replica-0                 2/2     Running     0          2m42s
pod/zone1-commerce-0-replica-1                 2/2     Running     0          2m42s
pod/zone1-commerce-0-replica-2                 2/2     Running     0          2m42s

NAME                                           COMPLETIONS   DURATION   AGE
job.batch/commerce-apply-schema-initial        1/1           94s        2m43s
job.batch/commerce-apply-vschema-initial       1/1           87s        2m43s
job.batch/zone1-commerce-0-init-shard-master   1/1           90s        2m43s
```

## Setup Port-forward

For ease-of-use, Vitess provides a script to port-forward from kubernetes to your local machine. This script also recommends setting up aliases for `mysql` and `vtctlclient`:

```bash
./pf.sh &
sleep 5

alias vtctlclient="vtctlclient -server=localhost:15999"
alias mysql="mysql -h 127.0.0.1 -P 15306"
```

Setting up aliases changes `mysql` to always connect to Vitess for your current session. To revert this, type `unalias mysql && unalias vtctlclient` or close your session.

### Connect to your cluster

You should now be able to connect to the VTGate Server in your cluster with the MySQL client:

```text
~/my-vitess-example> mysql
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 1
Server version: 5.7.9-Vitess Percona Server (GPL), Release 29, Revision 11ad961

Copyright (c) 2000, 2020, Oracle and/or its affiliates. All rights reserved.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> SHOW DATABASES;
+-----------+
| Databases |
+-----------+
| commerce  |
+-----------+
1 row in set (0.00 sec)
```

### Summary

In this example, we deployed a single unsharded keyspace named `commerce`. Unsharded keyspaces have a single shard named `0`. The following schema reflects a common ecommerce scenario that was created by the script:

``` sql
create table product(
  sku varbinary(128),
  description varbinary(128),
  price bigint,
  primary key(sku)
);
create table customer(
  customer_id bigint not null auto_increment,
  email varbinary(128),
  primary key(customer_id)
);
create table corder(
  order_id bigint not null auto_increment,
  customer_id bigint,
  sku varbinary(128),
  price bigint,
  primary key(order_id)
);
```

The schema has been simplified to include only those fields that are significant to the example:

* The `product` table contains the product information for all of the products.
* The `customer` table has a `customer_id` that has an `auto_increment`. A typical customer table would have a lot more columns, and sometimes additional detail tables.
* The `corder` table (named so because `order` is an SQL reserved word) has an `order_id` auto-increment column. It also has foreign keys into `customer(customer_id)` and `product(sku)`.

## Next Steps

You can now proceed with [MoveTables](../../user-guides/migration/move-tables).

Or alternatively, if you would like to teardown your example:

```sh
helm delete vitess
kubectl delete pvc -l "app=vitess"
kubectl delete vitesstoponodes --all
```
Congratulations on completing this exercise!
