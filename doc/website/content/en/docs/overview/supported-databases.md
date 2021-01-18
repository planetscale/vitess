---
title: Supported Databases  
weight: 2
featured: true
---

Vitess deploys, scales and manages clusters of open-source SQL database instances. Currently, Vitess supports the [MySQL](https://www.mysql.com/), [Percona](https://www.percona.com/software/mysql-database/percona-server) and [MariaDB](https://mariadb.org) databases.

The [VTGate](../../concepts/vtgate/) proxy server advertises its version as MySQL 5.7.

## MySQL versions 5.6 to 8.0

Vitess supports the core features of MySQL versions 5.6 to 8.0, with [some limitations](../../reference/mysql-compatibility/). Vitess also supports [Percona Server for MySQL](https://www.percona.com/software/mysql-database/percona-server) versions 5.6 to 8.0.

With MySQL 5.6 reaching end of life in February 2021, it is recommended to deploy MySQL 5.7 and later.

## MariaDB versions 10.0 to 10.3

Vitess supports the core features of MariaDB versions 10.0 to 10.3. Vitess [does not yet](https://github.com/vitessio/vitess/issues/5362) support version 10.4 of MariaDB.

## See also

+ [MySQL Compatibility](../../reference/mysql-compatibility/)
