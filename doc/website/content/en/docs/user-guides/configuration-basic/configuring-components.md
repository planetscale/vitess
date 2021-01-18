---
title: Configuring Components
weight: 2
aliases: ['/docs/launching/server-configuration/', '/docs/user-guides/server-configuration/', '/docs/user-guides/configuring-components/']
---

## Managed MySQL

The following describes the requirements for Vitess when fully managing MySQL with `mysqlctl` (see [VTTablet Modes](../../../reference/programs/vttablet#managed-mysql)).

When using [Unmanaged or Remote MySQL](../../../reference/programs/vttablet#unmanaged-or-remote-mysql) instead, the requirement is only that the server speak the MySQL protocol.

### Version and Flavor

`mysqlctl` supports MySQL/Percona Server 5.6 to 8.0, and MariaDB 10.0 to 10.3. MariaDB 10.4 is currently known to have installation issues ([#5362](https://github.com/vitessio/vitess/issues/5362)).

### Base Configuration

Starting with Vitess 4.0, `mysqlctl` will auto-detect the version and flavor of MySQL you are using, and automatically-include a base configuration file in `config/mycnf/*`.

Auto-dection works by searching for `mysqld` in the `$PATH`, as well as in the environment variable `$VT_MYSQL_ROOT`. If auto-detection fails, `mysqlctl` will apply version detection based on the `$MYSQL_FLAVOR` environment variable. Auto-detection will always take precedence over `$MYSQL_FLAVOR`.
 
### Specifying Additional Configuration

The automatically-included base configuration makes only the required settings changes for Vitess to operate correctly. It is recommended to configure InnoDB settings such as `innodb_buffer_pool_size` and `innodb_log_file_size` according to your available system resources.

`mysqlctl` **will not** read configuration files from common locations such as `/etc/my.cnf` or `/etc/mysql/my.cnf`. To include a custom `my.cnf` file as part of the initialization of tablets, set the `$EXTRA_MY_CNF` environment variable to a list of colon-separated files. Each file must be an absolute path.

In Kubernetes, you can use a ConfigMap to overwrite the entire `$VTROOT/config/mycnf` directory with your custom versions, rather than baking them into a custom container image.

### Unsupported Configuration Changes

When specifying additional configuration changes to Vitess, please keep in mind that changing the following settings is unsupported:

| Setting             | Reason         |
|---------------------|----------------|
| `auto_commit`       | MySQL autocommit needs to be turned on. VTTablet uses connection pools to MySQL. If autocommit is turned off, MySQL will start an implicit transaction (with a point in time snapshot) for each connection and will work very hard at keeping the current view unchanged, which would be counter-productive. |
| `log-bin`           | Several Vitess features rely on the binary log being enabled. |
| `binlog-format`     | Vitess only supports row-based replication. Do not change this setting from the included configuration files. |
| `binlog-row-image`  | Vitess only supports the default value (`FULL`) |
| `log-slave-updates` | Vitess requires this setting enabled, as it is in the included configuration files. |
| `character-set\*`   | Vitess only supports `utf8` (and variants such as `utf8mb4`) | 
| `gtid-mode`         | Vitess relies on GTIDs to track changes to topology. |
| `gtid-strict-mode`/`enforce-gtid-consistency` | Vitess requires this setting to be unchanged. |
| `sql-mode`          | Vitess can operate with non-default SQL modes, but VTGate will not allow you to change the sql-mode on a per-session basis. This can create compatibility issues for applications that require changes to this setting. |

### init\_db.sql

When a new instance is initialized with mysqlctl init (as opposed to restarting in a previously initialized data dir with mysqlctl start), the `init_db.sql` file is applied to the server immediately after running the bootstrap procedure (either `mysqld --initialize-insecure` or `mysql_install_db`, depending on the MySQL version). This file is also responsible for removing unprivileged users, as well as adding the necessary tables and grants for Vitess.

Note that changes to this file will not be reflected in shards that have already been initialized and had at least one backup taken. New instances in such shards will automatically restore the latest backup upon vttablet startup, overwriting the data dir created by `mysqlctl`.

## Vitess Servers

### Logging

Vitess servers write to log files, and they are rotated when they reach a maximum size. It’s recommended that you run at INFO level logging. The information printed in the log files come in handy for troubleshooting. You can limit the disk usage by running cron jobs that periodically purge or archive them.

Vitess supports both MySQL protocol and gRPC for communication between client and Vitess and uses gRPC for communication between Vitess servers. By default, Vitess does not use SSL.

Also, even without using SSL, we allow the use of an application-provided CallerID object. It allows unsecured but easy to use authorization using Table ACLs.

See the [TLS](https://github.com/aquarapid/vitess_examples/blob/master/tls/securing_vitess.md) example for more information on how to set up both of these features, and what command line parameters exist.

### Topology Service configuration  

Vttablet, vtgate and vtctld need the right command line parameters to find the topology service. First the `topo_implementation` flag needs to be set to one of zk2, etcd2, or consul. Then they're all configured as follows:

* The `topo_global_server_address` contains the server address / addresses of the global topology service.
* The `topo_global_root` contains the directory / path to use.

Note that the local cell for the tablet must exist and be configured properly in the Topology Service for vttablet to start. Local cells are configured inside the topology service, by using the `vtctl AddCellInfo` command. See the Topology Service documentation for more information.

## VTTablet

VTTablet has a large number of command line options. Some important ones will be covered here. In terms of provisioning these are the recommended values

* 2-4 cores (in proportion to MySQL cores)
* 2-4 GB RAM

### Directory Configuration

vttablet supports a number of command line options and environment variables to facilitate its setup.

The VTDATAROOT environment variable specifies the toplevel directory for all data files. If not set, it defaults to /vt.

By default, a vttablet will use a subdirectory in VTDATAROOT named vt\_NNNNNNNNNN where NNNNNNNNNN is the tablet id. The tablet\_dir command-line parameter allows overriding this relative path. This is useful in containers where the filesystem only contains one vttablet, in order to have a fixed root directory.

When starting up and using mysqlctl to manage MySQL, the MySQL files will be in subdirectories of the tablet root. For instance, bin-logs for the binary logs, data for the data files, and relay-logs for the relay logs.

It is possible to host different parts of a MySQL server files on different partitions. For instance, the data file may reside in flash, while the bin logs and relay logs are on spindle. To achieve this, create a symlink from $VTDATAROOT/\<dir name\> to the proper location on disk. When MySQL is configured by mysqlctl, it will realize this directory exists, and use it for the files it would otherwise have put in the tablet directory. For instance, to host the binlogs in /mnt/bin-logs:

* Create a symlink from $VTDATAROOT/bin-logs to /mnt/bin-logs.
* When starting up a tablet:
      * /mnt/bin-logs/vt\_NNNNNNNNNN will be created.
      * $VTDATAROOT/vt\_NNNNNNNNNN/bin-logs will be a symlink to /mnt/bin-logs/vt\_NNNNNNNNNN

### Initialization

* Init\_keyspace, init\_shard, init\_tablet\_type: These parameters should be set at startup with the keyspace / shard / tablet type to start the tablet as. Note ‘master’ is not allowed here, instead use ‘replica’, as the tablet when starting will figure out if it is the master (this way, all replica tablets start with the same command line parameters, independently of which one is the master).

### Query server parameters

* **queryserver-config-pool-size**: This value should typically be set to the max number of simultaneous queries you want MySQL to run. This should typically be around 2-3x the number of allocated CPUs. Around 4-16. There is not much harm in going higher with this value, but you may see no additional benefits.
* **queryserver-config-stream-pool-size**: This value is relevant only if you plan to run streaming queries against the database. It’s recommended that you use rdonly instances for such streaming queries. This value depends on how many simultaneous streaming queries you plan to run. Typical values are in the low 100s.
* **queryserver-config-transaction-cap**: This value should be set to how many concurrent transactions you wish to allow. This should be a function of transaction QPS and transaction length. Typical values are in the low 100s.
* **queryserver-config-query-timeout**: This value should be set to the upper limit you’re willing to allow a query to run before it’s deemed too expensive or detrimental to the rest of the system. VTTablet will kill any query that exceeds this timeout. This value is usually around 15-30s.
* **queryserver-config-transaction-timeout**: This value is meant to protect the situation where a client has crashed without completing a transaction. Typical value for this timeout is 30s.
* **queryserver-config-max-result-size**: This parameter prevents the OLTP application from accidentally requesting too many rows. If the result exceeds the specified number of rows, VTTablet returns an error. The default value is 10,000.

### DB config parameters

VTTablet requires multiple user credentials to perform its tasks. Since it's required to run on the same machine as MySQL, it’s most beneficial to use the more efficient unix socket connections.

**connection** parameters

* `db_socket`: The unix socket to connect on. If this is specified, host and port will not be used.
* `db_host`: The host name for the tcp connection.
* `db_port`: The tcp port to be used with the `db_host`.
* `db_charset`: Character set. Only utf8 or latin1 based character sets are supported.
* `db_flags`: Flag values as defined by MySQL.
* `db_ssl_ca`, `db_ssl_ca_path`, `db_ssl_cert`, `db_ssl_key`: SSL flags.

**app** credentials are for serving app queries:

* `db_app_user`: App username.
* `db_app_password`: Password for the app username. If you need a more secure way of managing and supplying passwords, VTTablet does allow you to plug into a "password server" that can securely supply and refresh usernames and passwords. Please contact the Vitess team for help if you’d like to write such a custom plugin.
* `db_app_use_ssl`: Set this flag to false if you don't want to use SSL for this connection. This will allow you to turn off SSL for all users except for `repl`, which may have to be turned on for replication that goes over open networks.

**appdebug** credentials are for the appdebug user:

* `db_appdebug_user`
* `db_appdebug_password`
* `db_appdebug_use_ssl`

**dba** credentials will be used for housekeeping work like loading the schema or killing runaway queries:

* `db_dba_user`
* `db_dba_password`
* `db_dba_use_ssl`

**repl** credentials are for managing replication.

* `db_repl_user`
* `db_repl_password`
* `db_repl_use_ssl`

**filtered** credentials are for performing resharding:

* `db_filtered_user`
* `db_filtered_password`
* `db_filtered_use_ssl`

### Monitoring

VTTablet exports a wealth of real-time information about itself. This section will explain the essential ones:

#### /debug/status

This page has a variety of human-readable information about the current VTTablet. You can look at this page to get a general overview of what’s going on. It also has links to various other diagnostic URLs below.

#### /debug/vars

This is the most important source of information for monitoring. There are other URLs below that can be used to further drill down.

#### Queries (as described in /debug/vars section)

Vitess has a structured way of exporting certain performance stats. The most common one is the Histogram structure, which is used by Queries:

``` json
  "Queries": {
    "Histograms": {
      "PASS\_SELECT": {
        "1000000": 1138196,
        "10000000": 1138313,
        "100000000": 1138342,
        "1000000000": 1138342,
        "10000000000": 1138342,
        "500000": 1133195,
        "5000000": 1138277,
        "50000000": 1138342,
        "500000000": 1138342,
        "5000000000": 1138342,
        "Count": 1138342,
        "Time": 387710449887,
        "inf": 1138342
      }
    },
    "TotalCount": 1138342,
    "TotalTime": 387710449887
  },
```  

The histograms are broken out into query categories. In the above case, "`PASS\_SELECT`" is the only category. An entry like `"500000": 1133195` means that `1133195` queries took under `500000` nanoseconds to execute.

`Queries.Histograms.PASS\_SELECT.Count` is the total count in the `PASS\_SELECT` category.

`Queries.Histograms.PASS\_SELECT.Time` is the total time in the `PASS\_SELECT` category.

`Queries.TotalCount` is the total count across all categories.

`Queries.TotalTime` is the total time across all categories.

There are other Histogram variables described below, and they will always have the same structure.

Use this variable to track:

* QPS
* Latency
* Per-category QPS. For replicas, the only category will be PASS\_SELECT, but there will be more for masters.
* Per-category latency
* Per-category tail latency

#### Results

``` json
  "Results": {
    "0": 0,
    "1": 0,
    "10": 1138326,
    "100": 1138326,
    "1000": 1138342,
    "10000": 1138342,
    "5": 1138326,
    "50": 1138326,
    "500": 1138342,
    "5000": 1138342,
    "Count": 1138342,
    "Total": 1140438,
    "inf": 1138342
  }
```  

Results is a simple histogram with no timing info. It gives you a histogram view of the number of rows returned per query.

#### Mysql

Mysql is a histogram variable like Queries, except that it reports MySQL execution times. The categories are "Exec" and “ExecStream”.

In the past, the exec time difference between VTTablet and MySQL used to be substantial. With the newer versions of Go, the VTTablet exec time has been predominantly been equal to the mysql exec time, conn pool wait time and consolidations waits. In other words, this variable has not shown much value recently. However, it’s good to track this variable initially, until it’s determined that there are no other factors causing a big difference between MySQL performance and VTTablet performance.

#### Transactions

Transactions is a histogram variable that tracks transactions. The categories are "Completed" and “Aborted”.

#### Waits

Waits is a histogram variable that tracks various waits in the system. Right now, the only category is "Consolidations". A consolidation happens when one query waits for the results of an identical query already executing, thereby saving the database from performing duplicate work.

This variable used to report connection pool waits, but a refactor moved those variables out into the pool related vars.

#### Errors

``` json
  "Errors": {
    "Deadlock": 0,
    "Fail": 1,
    "NotInTx": 0,
    "TxPoolFull": 0
  },
```

Errors are reported under different categories. It’s beneficial to track each category separately as it will be more helpful for troubleshooting. Right now, there are four categories. The category list may vary as Vitess evolves.

Plotting errors/query can sometimes be useful for troubleshooting.

VTTablet also exports an InfoErrors variable that tracks inconsequential errors that don’t signify any kind of problem with the system. For example, a dup key on insert is considered normal because apps tend to use that error to instead update an existing row. So, no monitoring is needed for that variable.

#### InternalErrors

``` json
  "InternalErrors": {
    "HungQuery": 0,
    "Invalidation": 0,
    "MemcacheStats": 0,
    "Mismatch": 0,
    "Panic": 0,
    "Schema": 0,
    "StrayTransactions": 0,
    "Task": 0
  },
```

An internal error is an unexpected situation in code that may possibly point to a bug. Such errors may not cause outages, but even a single error needs be escalated for root cause analysis.

#### Kills

``` json
  "Kills": {
    "Queries": 2,
    "Transactions": 0
  },
```

Kills reports the queries and transactions killed by VTTablet due to timeout. It’s a very important variable to look at during outages.

#### TransactionPool*

There are a few variables with the above prefix:

``` json
  "TransactionPoolAvailable": 300,
  "TransactionPoolCapacity": 300,
  "TransactionPoolIdleTimeout": 600000000000,
  "TransactionPoolMaxCap": 300,
  "TransactionPoolTimeout": 30000000000,
  "TransactionPoolWaitCount": 0,
  "TransactionPoolWaitTime": 0,
```

* `WaitCount` will give you how often the transaction pool gets full that causes new transactions to wait.
* `WaitTime`/`WaitCount` will tell you the average wait time.
* `Available` is a gauge that tells you the number of available connections in the pool in real-time. `Capacity-Available` is the number of connections in use. Note that this number could be misleading if the traffic is spiky.

#### Other Pool variables

Just like `TransactionPool`, there are variables for other pools:

* `ConnPool`: This is the pool used for read traffic.
* `StreamConnPool`: This is the pool used for streaming queries.

There are other internal pools used by VTTablet that are not very consequential.

#### `TableACLAllowed`, `TableACLDenied`, `TableACLPseudoDenied`

The above three variables table acl stats broken out by table, plan and user.

#### `QueryPlanCacheSize`

If the application does not make good use of bind variables, this value would reach the QueryCacheCapacity. If so, inspecting the current query cache will give you a clue about where the misuse is happening.

#### `QueryCounts`, `QueryErrorCounts`, `QueryRowCounts`, `QueryTimesNs`

These variables are another multi-dimensional view of Queries. They have a lot more data than Queries because they’re broken out into tables as well as plan. This is a priceless source of information when it comes to troubleshooting. If an outage is related to rogue queries, the graphs plotted from these vars will immediately show the table on which such queries are run. After that, a quick look at the detailed query stats will most likely identify the culprit.

#### `UserTableQueryCount`, `UserTableQueryTimesNs`, `UserTransactionCount`, `UserTransactionTimesNs`

These variables are yet another view of Queries, but broken out by user, table and plan. If you have well-compartmentalized app users, this is another priceless way of identifying a rogue "user app" that could be misbehaving.

#### `DataFree`, `DataLength`, `IndexLength`, `TableRows`

These variables are updated periodically from information\_schema.tables. They represent statistical information as reported by MySQL about each table. They can be used for planning purposes, or to track unusual changes in table stats.

* `DataFree` represents `data_free`
* `DataLength` represents `data_length`
* `IndexLength` represents `index_length`
* `TableRows` represents `table_rows`

#### /debug/health

This URL prints out a simple "ok" or “not ok” string that can be used to check if the server is healthy. The health check makes sure mysqld connections work, and replication is configured (though not necessarily running) if not master.

#### /queryz, /debug/query\_stats, /debug/query\_plans, /streamqueryz

* /debug/query\_stats is a JSON view of the per-query stats. This information is pulled in real-time from the query cache. The per-table stats in /debug/vars are a roll-up of this information.
* /queryz is a human-readable version of /debug/query\_stats. If a graph shows a table as a possible source of problems, this is the next place to look at to see if a specific query is the root cause.
* /debug/query\_plans is a more static view of the query cache. It just shows how VTTablet will process or rewrite the input query.
* /streamqueryz lists the currently running streaming queries. You have the option to kill any of them from this page.

#### /querylogz, /debug/querylog, /txlogz, /debug/txlog

* /debug/querylog is a never-ending stream of currently executing queries with verbose information about each query. This URL can generate a lot of data because it streams every query processed by VTTablet. The details are as per this function: https://github.com/vitessio/vitess/blob/master/go/vt/vttablet/tabletserver/tabletenv/logstats.go#L202
* /querylogz is a limited human readable version of /debug/querylog. It prints the next 300 queries by default. The limit can be specified with a limit=N parameter on the URL.
* /txlogz is like /querylogz, but for transactions.
* /debug/txlog is the JSON counterpart to /txlogz.

#### /consolidations

This URL has an MRU list of consolidations. This is a way of identifying if multiple clients are spamming the same query to a server.

#### /schemaz, /debug/schema

* /schemaz shows the schema info loaded by VTTablet.
* /debug/schema is the JSON version of /schemaz.

#### /debug/query\_rules

This URL displays the currently active query blacklist rules.

### Alerting

Alerting is built on top of the variables you monitor. Before setting up alerts, you should get some baseline stats and variance, and then you can build meaningful alerting rules. You can use the following list as a guideline to build your own:

* Query latency among all vttablets
* Per keyspace latency
* Errors/query
* Memory usage
* Unhealthy for too long
* Too many vttablets down
* Health has been flapping
* Transaction pool full error rate
* Any internal error
* Traffic out of balance among replicas
* Qps/core too high

## VTGate

A typical VTGate should be provisioned as follows.

* 2-4 cores
* 2-4 GB RAM

Since VTGate is stateless, you can scale it linearly by just adding more servers as needed. Beyond the recommended values, it’s better to add more VTGates than giving more resources to existing servers, as recommended in the philosophy section.

Load-balancer in front of vtgate to scale up (not covered by Vitess). Stateless, can use the health URL for health check.

### Parameters

Some of the important VTGate and VTTablet flags for modifying query serving behavior:

VTGate:
* `cells_to_watch`: which cell vtgate is in and will monitor tablets from. Cross-cell master access needs multiple cells here.
* `tablet_types_to_wait`: VTGate waits for at least one serving tablet per tablet type specified here during startup, before listening to the serving port, so that VTGate does not start up serving errors. It should match a subset of the available tablet types VTGate connects to (master, replica, rdonly). The default is empty, i.e. VTGate will not wait for any serving tablets to start listening.
* `discovery_low_replication_lag (default: 30s)`: when replication lags of all VTTablet instances in a particular shard and of a specific tablet type are less than or equal this value, VTGate does not filter the tablets by replication lag and uses all to balance traffic.
* `discovery_high_replication_lag_minimum_serving (default: 2h)`: the replication lag that is considered too high when applying the `min_number_serving_vttablets` threshold
* `min_number_serving_vttablets (default: 2)`: when replication lag exceeds `discovery_low_replication_lag`, but not `discovery_high_replication_lag_minimum_serving`, keep serving from at least this many replica tablets per shard. This threshold also applies separately to the minimum number of serving rdonly tablets per shard.
* `transaction_mode (default: multi)`: default write transaction mode to allow:
  * `single`: disallow multi-db transactions
  * `multi`: allow multi-db transactions with best effort commit
  * `twopc`: allow multi-db transactions with 2pc commit.

  Note that `transaction_mode` does not affect read-only transactions.
* `normalize_queries (default: true)`: Turning this flag on will cause vtgate to rewrite queries with bind vars. This is beneficial if the app doesn't itself send normalized queries.

VTTablet:
* `unhealthy_threshold (default: 2h)`: a replicating tablet (e.g. tablet type replica or rdonly) will publish itself as unhealthy if replication lag exceeds this threshold.
* `degraded_threshold (30s)`: a replicating tablet (e.g. tablet type replica or rdonly) will publish itself as degraded if replication lag exceeds this threshold. This will cause VTGates to choose more up-to-date servers over this one. If all servers are degraded, VTGate resorts to serving from all of them. Also note the potential impact of the `-min_number_serving_vttablets` above.

### Monitoring

#### /debug/status

This is the landing page for a VTGate, which gives you the status of how a particular server is doing. Of particular interest there is the list of tablets this vtgate process is connected to, as this is the list of tablets that can potentially serve queries.

#### /debug/vars

##### VTGateApi

This is the main histogram variable to track for vtgates. It gives you a break up of all queries by command, keyspace, and type.

##### HealthcheckConnections

It shows the number of tablet connections for query/healthcheck per keyspace, shard, and tablet type.

##### /debug/query\_plans

This URL gives you all the query plans for queries going through VTGate.

##### /debug/vschema

This URL shows the vschema as loaded by VTGate.

### Alerting

For VTGate, here’s a list of possible variables to alert on:

* Error rate
* Error/query rate
* Error/query/tablet-type rate
* VTGate serving graph is stale by x minutes (topology service is down)
* Qps/core
* Latency

