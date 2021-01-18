---
title: Migrating data into Vitess
weight: 1
aliases: ['/docs/user-guides/migrate-data/'] 
---

# Introduction 

There are two main parts to migrating your data to Vitess: migrating the actual data and repointing the application. The answer here will focus primarily on the methods that can be used to migrate your data into Vitess.

## Overview

There are three different methods to migrate your data into Vitess. Choosing the appropriate option depends on several factors.
1. The nature of the application accessing the MySQL database
1. The size of the MySQL database to be migrated
1. The load, especially the write load, on the MySQL database
1. Your tolerance for downtime during the migration of data
1. Whether you require the ability to reverse the migration if needed
1. The network level configuration of your components

The three different methods are:

* ‘Stop-the-world’
* VReplication from Vitess setup in front of the existing external MySQL database
* Application-level migration

## Method 1: “Stop-the-world”:

The simplest method to migrate data is to do a ‘dump and restore’ or ‘stop-the-world’. We recommend using [‘go-mydumper’](https://github.com/aquarapid/go-mydumper). To execute this method you would follow these steps:
1. Stop writing to the source MySQL database
1. Take a logical dump of the database using go-mydumper or possibly mysqldump
1. Apply some simple transformations on the output
1. Import the data into Vitess via the frontend
1. Repoint your application to the new database  
1. Resume writing to the new database in Vitess 

This method is only suitable for migrating small or non-critical databases that can tolerate downtime. The database will be unavailable for writes between the time the dump is started and the time the restore of the dump is completed. For databases of 10’s of GB and up this process could take hours or even days. The amount of downtime scales with the amount of data being migrated.

Please note the ‘dump and restore’ method likely isn’t viable for most production applications, unless the applicable downtime can be handled. 

## Method 2: VReplication from Vitess setup in front of the existing external MySQL database 

A set of Vitess components will be created, on a temporary basis, to run in front of the source MySQL database in order to migrate the data. These components should reference at least one of the replicas, in addition to the master, of the MySQL database. The Vitess components can be run on bare metal, in a VM, or potentially even in Kubernetes. 

It is important to note that the Vitess components must be reachable over a network by Vitess’s backend systems. Your topology must be set up such that the source database is reachable from your vitess cluster. Similarly, all the VTTablets being configured for migration must be set up to run against your database within the same Vitess cluster.

Vitess offers a choice of two VReplication commands to perform the data migration process described above: *MoveTables* or *Materialize*.

Both methods use a combination of transactional SELECTs and filtered MySQL replication to copy each of the tables in the source database to Vitess. Once all the data is copied, the two databases are kept in sync using the replication stream from the source database. While in this synchronized state, you can verify the source and destination are in sync, and testing on the copy of the data in Vitess can commence.

Once the testing has completed, application traffic can be removed from the source MySQL database and switched to the Vitess database. For this switch, a small amount of downtime will be necessary. This downtime could be seconds or minutes, depending on the application and application automation.

There are some differences between *MoveTables* and *Materialize* that you will need to evaluate to determine which process to use:

### Materialize: This process works well if you want to get data out as purely a copy or you want to transform the data during the copying process

* Has more flexibility because you can transform the data while you are migrating it. E.g. you can choose not to migrate specific columns from a table
* It isn’t directly reversible. E.g. changes to the downstream Vitess copy of the data after the application cutover will not flow back to the original source MySQL database
* Switching over application traffic is not integrated. You have to manually configure the commands in order to do the switch over

### MoveTables: This process works well if you want to have minimum downtime during the switch over and to be able to reverse the switch over

* Switch reads and switch writes are integrated
* Allows the switch over to be reversible due to reverse replication. Writes to Vitess can be propagated back to the source MySQL database after the copy
* Cannot transform the data during the migration. The assumption is that the entire dataset is being copied as is

### Choosing the Right Method

The first and most important point to consider when choosing the right method is whether you can or cannot interconnect between components on your network. If you cannot, or do not wish to, perform extra steps to ensure interconnectivity then you will need to use the ‘Stop-the-world’ method. 

If you can ensure interconnectivity and that the target VTTablets are in the same Vitess cluster, then for cases when larger amounts of downtime are not an option you will want to use VReplication with either *MoveTables* or *Materialize*.

## Method 3: Application-level migration

In some cases it might be necessary to perform the data migration on an application level.  Reasons for this might be things like:

* The source data is spread across a large set of MySQL databases, and is being consolidated as part of the migration process. Thus it’s not possible to migrate data using only normal MySQL replication
* The source database systems are not running MySQL Row-Based Replication and it's not possible, feasible, or practical to convert them
* The source database system might not be MySQL, in which case a custom application-level migration will be necessary

In these cases custom tools must first be written on the application side to start writing data to both the legacy database and Vitess. Secondly, the source data must be moved over in bulk to the Vitess database and then the switch over can be performed.

There are multiple options to do those steps, however we won’t go into detail as each situation for these cases is unique. A summary of some potential options are:

### “Stop the world”:  

* Write application-level tools to export, import, and verify data between the source and destination systems.

### Dual writes:  

* Modify the application to start doing dual writes between the source and destination databases, while the application is still pointing to the source database as the primary datastore. 
* Create custom tools to backfill old data from the source to destination system. VReplication could be used to form a part of this solution.
* Cut-over by having the application start to read, as well as write, from the destination Vitess database as the primary data source. This option can be reversible, assuming the dual writes continue after the read cutover.
