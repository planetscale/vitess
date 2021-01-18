---
title: vtctl
description: vtctl Command Index
aliases: ['/docs/reference/vitess-api/','/docs/reference/vtctl/']
---

`vtctl` is a command-line tool used to administer a Vitess cluster. It is available as both a standalone tool (`vtctl`) and client-server (`vtctlclient` in combination with `vtctld`). Using client-server is recommended, as it provides an additional layer of security when using the client remotely.

## Commands

### Tablets

| Name | Example Usage |
| :-------- | :--------------- |
| [InitTablet](../vtctl/tablets#inittablet) | `InitTablet [-allow_update] [-allow_different_shard] [-allow_master_override] [-parent] [-db_name_override=<db name>] [-hostname=<hostname>] [-mysql_port=<port>] [-port=<port>] [-grpc_port=<port>] [-tags=tag1:value1,tag2:value2] -keyspace=<keyspace> -shard=<shard> <tablet alias> <tablet type>` |
| [GetTablet](../vtctl/tablets#gettablet) | `GetTablet <tablet alias>` |
| [UpdateTabletAddrs](../vtctl/tablets#updatetabletaddrs) | `UpdateTabletAddrs [-hostname <hostname>] [-ip-addr <ip addr>] [-mysql-port <mysql port>] [-vt-port <vt port>] [-grpc-port <grpc port>] <tablet alias>` |
| [DeleteTablet](../vtctl/tablets#deletetablet) | `DeleteTablet [-allow_master] <tablet alias> ...` |
| [SetReadOnly](../vtctl/tablets#setreadonly) | `SetReadOnly <tablet alias>` |
| [SetReadWrite](../vtctl/tablets#setreadwrite) | `SetReadWrite <tablet alias>` |
| [StartReplication](../vtctl/tablets#startreplication) | `StartReplication <tablet alias>` |
| [StopReplication](../vtctl/tablets#stopreplication) | `StopReplication <tablet alias>` |
| [ChangeTabletType](../vtctl/tablets#changetablettype) | `ChangeTabletType [-dry-run] <tablet alias> <tablet type>` |
| [Ping](../vtctl/tablets#ping) | `Ping <tablet alias>` |
| [RefreshState](../vtctl/tablets#refreshstate) | `RefreshState <tablet alias>` |
| [RefreshStateByShard](../vtctl/tablets#refreshstatebyshard) | `RefreshStateByShard [-cells=c1,c2,...] <keyspace/shard>` |
| [RunHealthCheck](../vtctl/tablets#runhealthcheck) | `RunHealthCheck <tablet alias>` |
| [IgnoreHealthError](../vtctl/tablets#ignorehealtherror) | `IgnoreHealthError <tablet alias> <ignore regexp>` |
| [Sleep](../vtctl/tablets#sleep) | `Sleep <tablet alias> <duration>` |
| [ExecuteHook](../vtctl/tablets#executehook) | `ExecuteHook <tablet alias> <hook name> [<param1=value1> <param2=value2> ...]` |
| [ExecuteFetchAsApp](../vtctl/tablets#executefetchasapp) | `ExecuteFetchAsApp [-max_rows=10000] [-json] [-use_pool] <tablet alias> <sql command>` |
| [ExecuteFetchAsDba](../vtctl/tablets#executefetchasdba) | `ExecuteFetchAsDba [-max_rows=10000] [-disable_binlogs] [-json] <tablet alias> <sql command>` |
| [VReplicationExec](../vtctl/tablets#vreplicationexec) | `VReplicationExec [-json] <tablet alias> <sql command>` |
| [Backup](../vtctl/tablets#backup) | `Backup [-concurrency=4] [-allow_master=false] <tablet alias>` |
| [RestoreFromBackup](../vtctl/tablets#restorefrombackup) | `RestoreFromBackup <tablet alias>` |
| [ReparentTablet](../vtctl/tablets#reparenttablet) | `ReparentTablet <tablet alias>` |

### Shards

| Name | Example Usage |
| :-------- | :--------------- |
| [CreateShard](../vtctl/shards#createshard) | `CreateShard [-force] [-parent] <keyspace/shard>` |
| [GetShard](../vtctl/shards#getshard) | `GetShard <keyspace/shard>` |
| [ValidateShard](../vtctl/shards#validateshard) | `ValidateShard [-ping-tablets] <keyspace/shard>` |
| [ShardReplicationPositions](../vtctl/shards#shardreplicationpositions) | `ShardReplicationPositions <keyspace/shard>` |
| [ListShardTablets](../vtctl/shards#listshardtablets) | `ListShardTablets <keyspace/shard>` |
| [SetShardIsMasterServing](../vtctl/shards#setshardismasterserving) | `SetShardIsMasterServing <keyspace/shard> <is_master_serving>` |
| [SetShardTabletControl](../vtctl/shards#setshardtabletcontrol) | `SetShardTabletControl [--cells=c1,c2,...] [--blacklisted_tables=t1,t2,...] [--remove] [--disable_query_service] <keyspace/shard> <tablet type>` |
| [UpdateSrvKeyspacePartition](../vtctl/shards#updatesrvkeyspacepartition)| `UpdateSrvKeyspacePartition [--cells=c1,c2,...] [--remove] <keyspace/shard> <tablet type>` |
| [SourceShardDelete](../vtctl/shards#sourcesharddelete) | `SourceShardDelete <keyspace/shard> <uid>` |
| [SourceShardAdd](../vtctl/shards#sourceshardadd) | `SourceShardAdd [--key_range=<keyrange>] [--tables=<table1,table2,...>] <keyspace/shard> <uid> <source keyspace/shard>` |
| [ShardReplicationFix](../vtctl/shards#shardreplicationfix) | `ShardReplicationFix <cell> <keyspace/shard>` |
| [WaitForFilteredReplication](../vtctl/shards#waitforfilteredreplication) | `WaitForFilteredReplication [-max_delay <max_delay, default 30s>] <keyspace/shard>` |
| [RemoveShardCell](../vtctl/shards#removeshardcell) | `RemoveShardCell [-force] [-recursive] <keyspace/shard> <cell>` |
| [DeleteShard](../vtctl/shards#deleteshard) | `DeleteShard [-recursive] [-even_if_serving] <keyspace/shard> ...` |
| [ListBackups](../vtctl/shards#listbackups) | `ListBackups <keyspace/shard>` |
| [BackupShard](../vtctl/shards#backupshard) | `BackupShard [-allow_master=false] <keyspace/shard>` |
| [RemoveBackup](../vtctl/shards#removebackup) | `RemoveBackup <keyspace/shard> <backup name>` |
| [InitShardMaster](../vtctl/shards#initshardmaster) | `InitShardMaster [-force] [-wait_replicas_timeout=<duration>] <keyspace/shard> <tablet alias>` |
| [PlannedReparentShard](../vtctl/shards#plannedreparentshard) | `PlannedReparentShard -keyspace_shard=<keyspace/shard> [-new_master=<tablet alias>] [-avoid_master=<tablet alias>] [-wait_replicas_timeout=<duration>]` |
| [EmergencyReparentShard](../vtctl/shards#emergencyreparentshard) | `EmergencyReparentShard -keyspace_shard=<keyspace/shard> -new_master=<tablet alias>` |
| [TabletExternallyReparented](../vtctl/shards#tabletexternallyreparented) | `TabletExternallyReparented <tablet alias>` |

### Keyspaces

| Name | Example Usage |
| :-------- | :--------------- |
| [CreateKeyspace](../vtctl/keyspaces#createkeyspace) | `CreateKeyspace  [-sharding_column_name=name] [-sharding_column_type=type] [-served_from=tablettype1:ks1,tablettype2:ks2,...] [-force] [-keyspace_type=type] [-base_keyspace=base_keyspace] [-snapshot_time=time] <keyspace name>` |
| [DeleteKeyspace](../vtctl/keyspaces#deletekeyspace) | `DeleteKeyspace  [-recursive] <keyspace>` |
| [RemoveKeyspaceCell](../vtctl/keyspaces#removekeyspacesell) | `RemoveKeyspaceCell  [-force] [-recursive] <keyspace> <cell>` |
| [GetKeyspace](../vtctl/keyspaces#getkeyspace) | `GetKeyspace  <keyspace>` |
| [GetKeyspaces](../vtctl/keyspaces#getkeyspaces) | `GetKeyspaces  ` |
| [SetKeyspaceShardingInfo](../vtctl/keyspaces#setkeyspaceshardinginfo) | `SetKeyspaceShardingInfo  [-force] <keyspace name> [<column name>] [<column type>]` |
| [SetKeyspaceServedFrom](../vtctl/keyspaces#setkeyspaceservedfrom) | `SetKeyspaceServedFrom  [-source=<source keyspace name>] [-remove] [-cells=c1,c2,...] <keyspace name> <tablet type>` |
| [RebuildKeyspaceGraph](../vtctl/keyspaces#rebuildkeyspacegraph) | `RebuildKeyspaceGraph  [-cells=c1,c2,...] <keyspace> ...` |
| [ValidateKeyspace](../vtctl/keyspaces#validatekeyspace) | `ValidateKeyspace  [-ping-tablets] <keyspace name>` |
| [Reshard](../vtctl/keyspaces#reshard) | `Reshard  [-skip_schema_copy] <keyspace.workflow> <source_shards> <target_shards>` |
| [MoveTables](../vtctl/keyspaces#movetables) | `MoveTables  [-cell=<cell>] [-tablet_types=<source_tablet_types>] -workflow=<workflow> <source_keyspace> <target_keyspace> <table_specs>` |
| [DropSources](../vtctl/keyspaces#dropsources) | `DropSources  [-dry_run] <keyspace.workflow>` |
| [CreateLookupVindex](../vtctl/keyspaces#createLookupvindex) | `CreateLookupVindex  [-cell=<cell>] [-tablet_types=<source_tablet_types>] <keyspace> <json_spec>` |
| [ExternalizeVindex](../vtctl/keyspaces#externalizevindex) | `ExternalizeVindex  <keyspace>.<vindex>` |
| [Materialize](../vtctl/keyspaces#materialize) | `Materialize  <json_spec>, example : '{"workflow": "aaa", "source_keyspace": "source", "target_keyspace": "target", "table_settings": [{"target_table": "customer", "source_expression": "select * from customer", "create_ddl": "copy"}]}'` |
| [SplitClone](../vtctl/keyspaces#splitclone) | `SplitClone  <keyspace> <from_shards> <to_shards>` |
| [VerticalSplitClone](../vtctl/keyspaces#verticalsplitclone) | `VerticalSplitClone  <from_keyspace> <to_keyspace> <tables>` |
| [VDiff](../vtctl/keyspaces#VDiff) | `VDiff  [-source_cell=<cell>] [-target_cell=<cell>] [-tablet_types=replica] [-filtered_replication_wait_time=30s] <keyspace.workflow>` |
| [MigrateServedTypes](../vtctl/keyspaces#migrateservedtypes) | `MigrateServedTypes  [-cells=c1,c2,...] [-reverse] [-skip-refresh-state] <keyspace/shard> <served tablet type>` |
| [MigrateServedFrom](../vtctl/keyspaces#migrateservedfrom) | `MigrateServedFrom  [-cells=c1,c2,...] [-reverse] <destination keyspace/shard> <served tablet type>` |
| [SwitchReads](../vtctl/keyspaces#switchreads) | `SwitchReads  [-cells=c1,c2,...] [-reverse] -tablet_type={replica|rdonly} [-dry-run] <keyspace.workflow>` |
| [SwitchWrites](../vtctl/keyspaces#switchwrites) | `SwitchWrites  [-filtered_replication_wait_time=30s] [-cancel] [-reverse_replication=false] [-dry-run] <keyspace.workflow>` |
| [CancelResharding](../vtctl/keyspaces#cancelresharding) | `CancelResharding  <keyspace/shard>` |
| [ShowResharding](../vtctl/keyspaces#showresharding) | `ShowResharding  <keyspace/shard>` |
| [FindAllShardsInKeyspace](../vtctl/keyspaces#findallshardsinkeyspace) | `FindAllShardsInKeyspace  <keyspace>` |
| [WaitForDrain](../vtctl/keyspaces#waitfordrain) | `WaitForDrain  [-timeout <duration>] [-retry_delay <duration>] [-initial_wait <duration>] <keyspace/shard> <served tablet type>` |

### Generic

| Name | Example Usage |
| :-------- | :--------------- |
| [Validate](../vtctl/generic#validate) | `Validate [-ping-tablets]` |
| [ListAllTablets](../vtctl/generic#listalltablets) | `ListAllTablets <cell name1>, <cell name2>, ...` |
| [ListTablets](../vtctl/generic#listtablets) | `ListTablets <tablet alias> ...` |
| [Help](../vtctl/generic#help) | `Help [command name]` |

### Schema, Version, Permissions

| Name | Example Usage |
| :-------- | :--------------- |
| [GetSchema](../vtctl/schema-version-permissions#getschema) | `GetSchema  [-tables=<table1>,<table2>,...] [-exclude_tables=<table1>,<table2>,...] [-include-views] <tablet alias>` |
| [ReloadSchema](../vtctl/schema-version-permissions#reloadschema) | `ReloadSchema  <tablet alias>` |
| [ReloadSchemaShard](../vtctl/schema-version-permissions#reloadschemashard) | `ReloadSchemaShard  [-concurrency=10] [-include_master=false] <keyspace/shard>` |
| [ReloadSchemaKeyspace](../vtctl/schema-version-permissions#reloadschemakeyspace) | `ReloadSchemaKeyspace  [-concurrency=10] [-include_master=false] <keyspace>` |
| [ValidateSchemaShard](../vtctl/schema-version-permissions#validateschemashard) | `ValidateSchemaShard  [-exclude_tables=''] [-include-views] <keyspace/shard>` |
| [ValidateSchemaKeyspace](../vtctl/schema-version-permissions#validateschemakeyspace) | `ValidateSchemaKeyspace  [-exclude_tables=''] [-include-views] <keyspace name>` |
| [ApplySchema](../vtctl/schema-version-permissions#applyschema) | `ApplySchema  [-allow_long_unavailability] [-wait_replicas_timeout=10s] {-sql=<sql> || -sql-file=<filename>} <keyspace>` |
| [CopySchemaShard](../vtctl/schema-version-permissions#copyschemashard) | `CopySchemaShard  [-tables=<table1>,<table2>,...] [-exclude_tables=<table1>,<table2>,...] [-include-views] [-skip-verify] [-wait_replicas_timeout=10s] {<source keyspace/shard> || <source tablet alias>} <destination keyspace/shard>` |
| [ValidateVersionShard](../vtctl/schema-version-permissions#validateversionshard) | `ValidateVersionShard  <keyspace/shard>` |
| [ValidateVersionKeyspace](../vtctl/schema-version-permissions#validateversionkeyspace) | `ValidateVersionKeyspace  <keyspace name>` |
| [GetPermissions](../vtctl/schema-version-permissions#getpermissions) | `GetPermissions  <tablet alias>` |
| [ValidatePermissionsShard](../vtctl/schema-version-permissions#validatepermissionsshard) | `ValidatePermissionsShard  <keyspace/shard>` |
| [ValidatePermissionsKeyspace](../vtctl/schema-version-permissions#validatepermissionskeyspace) | `ValidatePermissionsKeyspace  <keyspace name>` |
| [GetVSchema](../vtctl/schema-version-permissions#getvschema) | `GetVSchema  <keyspace>` |
| [ApplyVSchema](../vtctl/schema-version-permissions#applyvschema) | `ApplyVSchema  {-vschema=<vschema> || -vschema_file=<vschema file> || -sql=<sql> || -sql_file=<sql file>} [-cells=c1,c2,...] [-skip_rebuild] [-dry-run] <keyspace>` |
| [GetRoutingRules](../vtctl/schema-version-permissions#getroutingrules) | `GetRoutingRules  ` |
| [ApplyRoutingRules](../vtctl/schema-version-permissions#applyroutingrules) | `ApplyRoutingRules  {-rules=<rules> || -rules_file=<rules_file>} [-cells=c1,c2,...] [-skip_rebuild] [-dry-run]` |
| [RebuildVSchemaGraph](../vtctl/schema-version-permissions#rebuildvschemagraph) | `RebuildVSchemaGraph  [-cells=c1,c2,...]` |

### Serving Graph

| Name | Example Usage |
| :-------- | :--------------- |
| [GetSrvKeyspaceNames](../vtctl/serving-graph#getsrvkeyspacenames) | `GetSrvKeyspaceNames  <cell>` |
| [GetSrvKeyspace](../vtctl/serving-graph#getsrvkeyspace) | `GetSrvKeyspace  <cell> <keyspace>` |
| [GetSrvVSchema](../vtctl/serving-graph#getsrvsvchema) | `GetSrvVSchema  <cell>` |
| [DeleteSrvVSchema](../vtctl/serving-graph#deletesrvvschema) | `DeleteSrvVSchema  <cell>` |

### Replication Graph

| Name | Example Usage |
| :-------- | :--------------- |
| [GetShardReplication](../vtctl/replication-graph#getshardreplication) | `GetShardReplication  <cell> <keyspace/shard>` |

### Cells

| Name | Example Usage |
| :-------- | :--------------- |
| [AddCellInfo](../vtctl/cells#addcellinfo) | `AddCellInfo  [-server_address <addr>] [-root <root>] <cell>` |
| [UpdateCellInfo](../vtctl/cells#updatecellinfo) | `UpdateCellInfo  [-server_address <addr>] [-root <root>] <cell>` |
| [DeleteCellInfo](../vtctl/cells#deletecellinfo) | `DeleteCellInfo  [-force] <cell>` |
| [GetCellInfoNames](../vtctl/cells#getcellinfonames) | `GetCellInfoNames  ` |
| [GetCellInfo](../vtctl/cells#getcellinfo) | `GetCellInfo  <cell>` |

### CellsAliases

| Name | Example Usage |
| :-------- | :--------------- |
| [AddCellsAlias](../vtctl/cell-aliases#addcellsalias) | `AddCellsAlias  [-cells <cell,cell2...>] <alias>` |
| [UpdateCellsAlias](../vtctl/cell-aliases#updatecellsalias) | `UpdateCellsAlias  [-cells <cell,cell2,...>] <alias>` |
| [DeleteCellsAlias](../vtctl/cell-aliases#deletecellsalias) | `DeleteCellsAlias  <alias>` |
| [GetCellsAliases](../vtctl/cell-aliases#getcellsaliases) | `GetCellsAliases  ` |

### Queries

| Name | Example Usage |
| :-------- | :--------------- |
| [VtGateExecute](../vtctl/queries#vtgateexecute) | `VtGateExecute  -server <vtgate> [-bind_variables <JSON map>] [-keyspace <default keyspace>] [-tablet_type <tablet type>] [-options <proto text options>] [-json] <sql>` |
| [VtTabletExecute](../vtctl/queries#vttabletexecute) | `VtTabletExecute  [-username <TableACL user>] [-transaction_id <transaction_id>] [-options <proto text options>] [-json] <tablet alias> <sql>` |
| [VtTabletBegin](../vtctl/queries#vttabletbegin) | `VtTabletBegin  [-username <TableACL user>] <tablet alias>` |
| [VtTabletCommit](../vtctl/queries#vttabletcommit) | `VtTabletCommit  [-username <TableACL user>] <transaction_id>` |
| [VtTabletRollback](../vtctl/queries#vttabletrollback) | `VtTabletRollback  [-username <TableACL user>] <tablet alias> <transaction_id>` |
| [VtTabletStreamHealth](../vtctl/queries#vttabletstreamhealth) | `VtTabletStreamHealth  [-count <count, default 1>] <tablet alias>` |

### Resharding Throttler

| Name | Example Usage |
| :-------- | :--------------- |
| [ThrottlerMaxRates](../vtctl/resharding-throttler#throttlermaxrates) | `ThrottlerMaxRates  -server <vtworker or vttablet>` |
| [ThrottlerSetMaxRate](../vtctl/resharding-throttler#throttlersetmaxrate) | `ThrottlerSetMaxRate  -server <vtworker or vttablet> <rate>` |
| [GetThrottlerConfiguration](../vtctl/resharding-throttler#getthrottlerconfiguration) | `GetThrottlerConfiguration  -server <vtworker or vttablet> [<throttler name>]` |
| [UpdateThrottlerConfiguration](../vtctl/resharding-throttler#updatethrottlerconfiguration) | `UpdateThrottlerConfiguration  -server <vtworker or vttablet> [-copy_zero_values] "<configuration protobuf text>" [<throttler name>]` |
| [ResetThrottlerConfiguration](../vtctl/resharding-throttler#resetthrottlerconfiguration) | `ResetThrottlerConfiguration  -server <vtworker or vttablet> [<throttler name>]` |

### Topo

| Name | Example Usage |
| :-------- | :--------------- |
| [TopoCat](../vtctl/topo#topocat) | `TopoCat  [-cell <cell>] [-decode_proto] [-decode_proto_json] [-long] <path> [<path>...]` |
| [TopoCp](../vtctl/topo#topocp) | `TopoCp  [-cell <cell>] [-to_topo] <src> <dst>` |

### Workflows

| Name | Example Usage |
| :-------- | :--------------- |
| [WorkflowCreate](../vtctl/workflows#workflowcreate) | `WorkflowCreate  [-skip_start] <factoryName> [parameters...]` |
| [WorkflowStart](../vtctl/workflows#workflowstart) | `WorkflowStart  <uuid>` |
| [WorkflowStop](../vtctl/workflows#workflowstop) | `WorkflowStop  <uuid>` |
| [WorkflowDelete](../vtctl/workflows#workflowdelete) | `WorkflowDelete  <uuid>` |
| [WorkflowWait](../vtctl/workflows#workflowwait) | `WorkflowWait  <uuid>` |
| [WorkflowTree](../vtctl/workflows#workflowtree) | `WorkflowTree  ` |
| [WorkflowAction](../vtctl/workflows#workflowaction) | `WorkflowAction  <path> <name>` |

## Options

The following global options apply to `vtctl`:


| Name | Type | Definition |
| :------------------------------------ | :--------- | :----------------------------------------------------------------------------------------- |
| -alsologtostderr | | log to standard error as well as files |
| -app_idle_timeout | duration | Idle timeout for app connections (default 1m0s) |
| -app_pool_size | int | Size of the connection pool for app connections (default 40) |
| -azblob_backup_account_key_file | string | Path to a file containing the Azure Storage account key; if this flag is unset, the environment variable VT_AZBLOB_ACCOUNT_KEY will be used as the key itself (NOT a file path) |
| -azblob_backup_account_name | string | Azure Storage Account name for backups; if this flag is unset, the environment variable VT_AZBLOB_ACCOUNT_NAME will be used |
| -azblob_backup_container_name | string | Azure Blob Container Name |
| -azblob_backup_parallelism | int | Azure Blob operation parallelism (requires extra memory when increased) (default 1) |
| -azblob_backup_storage_root | string | Root prefix for all backup-related Azure Blobs; this should exclude both initial and trailing '/' (e.g. just 'a/b' not '/a/b/') |
| -backup_engine_implementation | string | Specifies which implementation to use for creating new backups (builtin or xtrabackup). Restores will always be done with whichever engine created a given backup. (default "builtin") |
| -backup_storage_block_size | int | if backup_storage_compress is true, backup_storage_block_size sets the byte size for each block while compressing (default is 250000). (default 250000) |
| -backup_storage_compress | | if set, the backup files will be compressed (default is true). Set to false for instance if a backup_storage_hook is specified and it compresses the data. (default true)|
| -backup_storage_hook | string | if set, we send the contents of the backup files through this hook. |
| -backup_storage_implementation | string | which implementation to use for the backup storage feature |
| -backup_storage_number_blocks | int | if backup_storage_compress is true, backup_storage_number_blocks sets the number of blocks that can be processed, at once, before the writer blocks, during compression (default is 2). It should be equal to the number of CPUs available for compression (default 2) |
| -binlog_player_protocol | string | the protocol to download binlogs from a vttablet (default "grpc") |
| -binlog_use_v3_resharding_mode | | True iff the binlog streamer should use V3-style sharding, which doesn't require a preset sharding key column. (default true)|
|-ceph_backup_storage_config | string | Path to JSON config file for ceph backup storage (default "ceph_backup_config.json") |
| -consul_auth_static_file | string | JSON File to read the topos/tokens from. |
| -cpu_profile | string | write cpu profile to file |
| -datadog-agent-host | string | host to send spans to. if empty, no tracing will be done |
| -datadog-agent-port | string | port to send spans to. if empty, no tracing will be done |
| -db-credentials-file | string | db credentials file; send SIGHUP to reload this file |
| -db-credentials-server | string | db credentials server type (use 'file' for the file implementation) (default "file") |
| -dba_idle_timeout | duration | Idle timeout for dba connections (default 1m0s) |
| -dba_pool_size | int | Size of the connection pool for dba connections (default 20) |
| -detach | | detached mode - run vtcl detached from the terminal |
| -disable_active_reparents | | if set, do not allow active reparents. Use this to protect a cluster using external reparents. |
| -discovery_high_replication_lag_minimum_serving | duration | the replication lag that is considered too high when selecting the minimum num vttablets for serving (default 2h0m0s) |
| -discovery_low_replication_lag | duration | the replication lag that is considered low enough to be healthy (default 30s) |
| -emit_stats | | true iff we should emit stats to push-based monitoring/stats backends |
| -enable-consolidator | | This option enables the query consolidator. (default true) |
| -enable-consolidator-replicas | | This option enables the query consolidator only on replicas. |
| -enable-query-plan-field-caching | | This option fetches & caches fields (columns) when storing query plans (default true) |
| -enable-tx-throttler | | If true replication-lag-based throttling on transactions will be enabled. |
| -enable_hot_row_protection | | If true, incoming transactions for the same row (range) will be queued and cannot consume all txpool slots. |
| -enable_hot_row_protection_dry_run | | If true, hot row protection is not enforced but logs if transactions would have been queued. |
| -enable_queries | | if set, allows vtgate and vttablet queries. May have security implications, as the queries will be run from this process. |
| -enable_transaction_limit | | If true, limit on number of transactions open at the same time will be enforced for all users. User trying to open a new transaction after exhausting their limit will receive an error immediately, regardless of whether there are available slots or not. |
| -enable_transaction_limit_dry_run | | If true, limit on number of transactions open at the same time will be tracked for all users, but not enforced. |
| -enforce_strict_trans_tables | | If true, vttablet requires MySQL to run with STRICT_TRANS_TABLES or STRICT_ALL_TABLES on. It is recommended to not turn this flag off. Otherwise MySQL may alter your supplied values before saving them to the database. (default true)|
|-file_backup_storage_root | string | root directory for the file backup storage |
| -gcs_backup_storage_bucket | string | Google Cloud Storage bucket to use for backups |
| -gcs_backup_storage_root | string | root prefix for all backup-related object names |
| -grpc_auth_mode | string | Which auth plugin implementation to use (eg: static) |
| -grpc_auth_mtls_allowed_substrings | string | List of substrings of at least one of the client certificate names (separated by colon). |
| -grpc_auth_static_client_creds | string | when using grpc_static_auth in the server, this file provides the credentials to use to authenticate with server |
| -grpc_auth_static_password_file | string | JSON File to read the users/passwords from. |
| -grpc_ca | string | ca to use, requires TLS, and enforces client cert check |
| -grpc_cert | string | certificate to use, requires grpc_key, enables TLS |
| -grpc_compression | string | how to compress gRPC, default: nothing, supported: snappy |
| -grpc_enable_tracing | | Enable GRPC tracing |
| -grpc_initial_conn_window_size | int | grpc initial connection window size |
| -grpc_initial_window_size | int | grpc initial window size |
| -grpc_keepalive_time | duration | After a duration of this time if the client doesn't see any activity it pings the server to see if the transport is still alive. (default 10s) |
| -grpc_keepalive_timeout | duration | After having pinged for keepalive check, the client waits for a duration of Timeout and if no activity is seen even after that the connection is closed. (default 10s) |
| -grpc_key | string | key to use, requires grpc_cert, enables TLS |
| -grpc_max_connection_age | duration | Maximum age of a client connection before GoAway is sent. (default 2562047h47m16.854775807s) |
| -grpc_max_connection_age_grace | duration | Additional grace period after grpc_max_connection_age, after which connections are forcibly closed. (default 2562047h47m16.854775807s) |
| -grpc_max_message_size | int | Maximum allowed RPC message size. Larger messages will be rejected by gRPC with the error 'exceeding the max size'. (default 16777216) |
| -grpc_port | int | Port to listen on for gRPC calls |
| -grpc_prometheus | | Enable gRPC monitoring with Prometheus |
| -grpc_server_initial_conn_window_size | int | grpc server initial connection window size |
| -grpc_server_initial_window_size | int | grpc server initial window size |
| -grpc_server_keepalive_enforcement_policy_min_time | duration | grpc server minimum keepalive time (default 5m0s) |
| -grpc_server_keepalive_enforcement_policy_permit_without_stream | | grpc server permit client keepalive pings even when there are no active streams (RPCs) |
| -heartbeat_enable | | If true, vttablet records (if master) or checks (if replica) the current time of a replication heartbeat in the table _vt.heartbeat. The result is used to inform the serving state of the vttablet via healthchecks.|
|-heartbeat_interval | duration | How frequently to read and write replication heartbeat. (default 1s) |
| -hot_row_protection_concurrent_transactions | int | Number of concurrent transactions let through to the txpool/MySQL for the same hot row. Should be > 1 to have enough 'ready' transactions in MySQL and benefit from a pipelining effect. (default 5) |
| -hot_row_protection_max_global_queue_size | int | Global queue limit across all row (ranges). Useful to prevent that the queue can grow unbounded. (default 1000) |
| -hot_row_protection_max_queue_size | int | Maximum number of BeginExecute RPCs which will be queued for the same row (range). (default 20) |
| -jaeger-agent-host | string | host and port to send spans to. if empty, no tracing will be done |
| -keep_logs | duration | keep logs for this long (using ctime) (zero to keep forever) |
| -keep_logs_by_mtime | duration | keep logs for this long (using mtime) (zero to keep forever) |
| -lameduck-period | duration | keep running at least this long after SIGTERM before stopping (default 50ms) |
| -legacy_replication_lag_algorithm | | use the legacy algorithm when selecting the vttablets for serving (default true) |
| -log_backtrace_at | value | when logging hits line file:N, emit a stack trace |
| -log_dir | string | If non-empty, write log files in this directory |
| -log_err_stacks | | log stack traces for errors |
| -log_rotate_max_size | uint | size in bytes at which logs are rotated (glog.MaxSize) (default 1887436800) |
| -logtostderr | | log to standard error instead of files |
| -master_connect_retry | duration | how long to wait in between replica reconnect attempts. Only precise to the second. (default 10s) |
| -mem-profile-rate | int | profile every n bytes allocated (default 524288) |
| -min_number_serving_vttablets | int | the minimum number of vttablets that will be continue to be used even with low replication lag (default 2) |
| -mutex-profile-fraction | int | profile every n mutex contention events (see runtime.SetMutexProfileFraction) |
| -mysql_auth_server_static_file | string | JSON File to read the users/passwords from. |
| -mysql_auth_server_static_string | string | JSON representation of the users/passwords config. |
| -mysql_auth_static_reload_interval | duration | Ticker to reload credentials |
| -mysql_clientcert_auth_method | string | client-side authentication method to use. Supported values: mysql_clear_password, dialog. (default "mysql_clear_password") |
| -mysql_server_flush_delay | duration | Delay after which buffered response will flushed to client. (default 100ms) |
| -mysqlctl_client_protocol | string | the protocol to use to talk to the mysqlctl server (default "grpc") |
| -mysqlctl_mycnf_template | string | template file to use for generating the my.cnf file during server init |
| -mysqlctl_socket | string | socket file to use for remote mysqlctl actions (empty for local actions) |
| -onterm_timeout | duration | wait no more than this for OnTermSync handlers before stopping (default 10s) |
| -pid_file | string | If set, the process will write its pid to the named file, and delete it on graceful shutdown. |
| -pool_hostname_resolve_interval | duration | if set force an update to all hostnames and reconnect if changed, defaults to 0 (disabled) |
| -purge_logs_interval | duration | how often try to remove old logs (default 1h0m0s) |
| -query-log-stream-handler | string | URL handler for streaming queries log (default "/debug/querylog") |
| -querylog-filter-tag | string | string that must be present in the query as a comment for the query to be logged, works for both vtgate and vttablet |
| -querylog-format | string | format for query logs ("text" or "json") (default "text") |
| -queryserver-config-acl-exempt-acl | string | an acl that exempt from table acl checking (this acl is free to access any vitess tables). |
| -queryserver-config-enable-table-acl-dry-run | | If this flag is enabled, tabletserver will emit monitoring metrics and let the request pass regardless of table acl check results |
| -queryserver-config-idle-timeout | int | query server idle timeout (in seconds), vttablet manages various mysql connection pools. This config means if a connection has not been used in given idle timeout, this connection will be removed from pool. This effectively manages number of connection objects and optimize the pool performance. (default 1800) |
| -queryserver-config-max-dml-rows | int | query server max dml rows per statement, maximum number of rows allowed to return at a time for an update or delete with either 1) an equality where clauses on primary keys, or 2) a subselect statement. For update and delete statements in above two categories, vttablet will split the original query into multiple small queries based on this configuration value.  |
| -queryserver-config-max-result-size | int | query server max result size, maximum number of rows allowed to return from vttablet for non-streaming queries. (default 10000) |
| -queryserver-config-message-conn-pool-prefill-parallelism | int | DEPRECATED: Unused. |
| -queryserver-config-message-conn-pool-size | int | DEPRECATED |
| -queryserver-config-message-postpone-cap | int | query server message postpone cap is the maximum number of messages that can be postponed at any given time. Set this number to substantially lower than transaction cap, so that the transaction pool isn't exhausted by the message subsystem. (default 4) |
| -queryserver-config-passthrough-dmls | | query server pass through all dml statements without rewriting |
| -queryserver-config-pool-prefill-parallelism | int | query server read pool prefill parallelism, a non-zero value will prefill the pool using the specified parallism. |
| -queryserver-config-pool-size | int | query server read pool size, connection pool is used by regular queries (non streaming, not in a transaction) (default 16) |
| -queryserver-config-query-cache-size | int | query server query cache size, maximum number of queries to be cached. vttablet analyzes every incoming query and generate a query plan, these plans are being cached in a lru cache. This config controls the capacity of the lru cache. (default 5000) |
| -queryserver-config-query-pool-timeout | int | query server query pool timeout (in seconds), it is how long vttablet waits for a connection from the query pool. If set to 0 (default) then the overall query timeout is used instead. |
| -queryserver-config-query-pool-waiter-cap | int | query server query pool waiter limit, this is the maximum number of queries that can be queued waiting to get a connection (default 5000) |
| -queryserver-config-query-timeout | int | query server query timeout (in seconds), this is the query timeout in vttablet side. If a query takes more than this timeout, it will be killed. (default 30) |
| -queryserver-config-schema-reload-time | int | query server schema reload time, how often vttablet reloads schemas from underlying MySQL instance in seconds. vttablet keeps table schemas in its own memory and periodically refreshes it from MySQL. This config controls the reload time. (default 1800) |
| -queryserver-config-stream-buffer-size | int | query server stream buffer size, the maximum number of bytes sent from vttablet for each stream call. It's recommended to keep this value in sync with vtgate's stream_buffer_size. (default 32768) |
| -queryserver-config-stream-pool-prefill-parallelism | int | query server stream pool prefill parallelism, a non-zero value will prefill the pool using the specified parallelism |
| -queryserver-config-stream-pool-size | int | query server stream connection pool size, stream pool is used by stream queries: queries that return results to client in a streaming fashion (default 200) |
| -queryserver-config-strict-table-acl | | only allow queries that pass table acl checks |
| -queryserver-config-terse-errors | | prevent bind vars from escaping in returned errors |
| -queryserver-config-transaction-cap | int | query server transaction cap is the maximum number of transactions allowed to happen at any given point of a time for a single vttablet. E.g. by setting transaction cap to 100, there are at most 100 transactions will be processed by a vttablet and the 101th transaction will be blocked (and fail if it cannot get connection within specified timeout) (default 20) |
| -queryserver-config-transaction-prefill-parallelism | int | query server transaction prefill parallelism, a non-zero value will prefill the pool using the specified parallism. |
| -queryserver-config-transaction-timeout | int | query server transaction timeout (in seconds), a transaction will be killed if it takes longer than this value (default 30) |
| -queryserver-config-txpool-timeout | int | query server transaction pool timeout, it is how long vttablet waits if tx pool is full (default 1) |
| -queryserver-config-txpool-waiter-cap | int | query server transaction pool waiter limit, this is the maximum number of transactions that can be queued waiting to get a connection (default 5000) |
| -queryserver-config-warn-result-size | int | query server result size warning threshold, warn if number of rows returned from vttablet for non-streaming queries exceeds this |
| -redact-debug-ui-queries | | redact full queries and bind variables from debug UI |
| -remote_operation_timeout | duration | time to wait for a remote operation (default 30s) |
| -s3_backup_aws_endpoint | string | endpoint of the S3 backend (region must be provided) |
| -s3_backup_aws_region | string | AWS region to use (default "us-east-1") |
| -s3_backup_aws_retries | int | AWS request retries (default -1) |
| -s3_backup_force_path_style | | force the s3 path style |
| -s3_backup_log_level | string | determine the S3 loglevel to use from LogOff, LogDebug, LogDebugWithSigning, LogDebugWithHTTPBody, LogDebugWithRequestRetries, LogDebugWithRequestErrors (default "LogOff") |
| -s3_backup_server_side_encryption | string | server-side encryption algorithm (e.g., AES256, aws:kms) |
| -s3_backup_storage_bucket | string | S3 bucket to use for backups |
| -s3_backup_storage_root | string | root prefix for all backup-related object names |
| -s3_backup_tls_skip_verify_cert | | skip the 'certificate is valid' check for SSL connections |
| -security_policy | string | the name of a registered security policy to use for controlling access to URLs - empty means allow all for anyone (built-in policies: deny-all, read-only) |
| -service_map | value | comma separated list of services to enable (or disable if prefixed with '-') Example: grpc-vtworker |
| -sql-max-length-errors | int | truncate queries in error logs to the given length (default unlimited) |
| -sql-max-length-ui | int | truncate queries in debug UIs to the given length (default 512) (default 512) |
| -srv_topo_cache_refresh | duration | how frequently to refresh the topology for cached entries (default 1s) |
| -srv_topo_cache_ttl | duration | how long to use cached entries for topology (default 1s) |
| -stats_backend | string | The name of the registered push-based monitoring/stats backend to use |
| -stats_combine_dimensions | string | List of dimensions to be combined into a single "all" value in exported stats vars |
| -stats_drop_variables | string | Variables to be dropped from the list of exported variables. |
| -stats_emit_period | duration | Interval between emitting stats to all registered backends (default 1m0s) |
| -stderrthreshold | value | logs at or above this threshold go to stderr (default 1) |
| -tablet_dir | string | The directory within the vtdataroot to store vttablet/mysql files. Defaults to being generated by the tablet uid. |
| -tablet_grpc_ca | string | the server ca to use to validate servers when connecting |
| -tablet_grpc_cert | string | the cert to use to connect |
| -tablet_grpc_key | string | the key to use to connect |
| -tablet_grpc_server_name | string | the server name to use to validate server certificate |
| -tablet_manager_grpc_ca | string | the server ca to use to validate servers when connecting |
| -tablet_manager_grpc_cert | string | the cert to use to connect |
| -tablet_manager_grpc_concurrency | int | concurrency to use to talk to a vttablet server for performance-sensitive RPCs (like ExecuteFetchAs{Dba,AllPrivs,App}) (default 8) |
| -tablet_manager_grpc_key | string | the key to use to connect |
| -tablet_manager_grpc_server_name | string | the server name to use to validate server certificate |
| -tablet_manager_protocol | string | the protocol to use to talk to vttablet (default "grpc") |
| -tablet_protocol | string | how to talk to the vttablets (default "grpc") |
| -tablet_url_template | string | format string describing debug tablet url formatting. See the Go code for getTabletDebugURL() how to customize this. (default "http://{{.GetTabletHostPort}}") |
| -throttler_client_grpc_ca | string | the server ca to use to validate servers when connecting |
| -throttler_client_grpc_cert | string | the cert to use to connect |
| -throttler_client_grpc_key | string | the key to use to connect |
| -throttler_client_grpc_server_name | string | the server name to use to validate server certificate |
| -throttler_client_protocol | string | the protocol to use to talk to the integrated throttler service (default "grpc") |
| -topo_consul_watch_poll_duration | duration | time of the long poll for watch queries. (default 30s) |
| -topo_etcd_lease_ttl | int | Lease TTL for locks and master election. The client will use KeepAlive to keep the lease going. (default 30) |
| -topo_etcd_tls_ca | string | path to the ca to use to validate the server cert when connecting to the etcd topo server |
| -topo_etcd_tls_cert | string | path to the client cert to use to connect to the etcd topo server, requires topo_etcd_tls_key, enables TLS |
| -topo_etcd_tls_key | string | path to the client key to use to connect to the etcd topo server, enables TLS |
| -topo_global_root | string | the path of the global topology data in the global topology server |
| -topo_global_server_address | string | the address of the global topology server |
| -topo_implementation | string | the topology implementation to use |
| -topo_k8s_context | string | The kubeconfig context to use, overrides the 'current-context' from the config |
| -topo_k8s_kubeconfig | string | Path to a valid kubeconfig file. |
| -topo_k8s_namespace | string | The kubernetes namespace to use for all objects. Default comes from the context or in-cluster config |
| -topo_zk_auth_file | string | auth to use when connecting to the zk topo server, file contents should be <scheme>:<auth>, e.g., digest:user:pass |
| -topo_zk_base_timeout | duration | zk base timeout (see zk.Connect) (default 30s) |
| -topo_zk_max_concurrency | int | maximum number of pending requests to send to a Zookeeper server. (default 64) |
| -topo_zk_tls_ca | string | the server ca to use to validate servers when connecting to the zk topo server |
| -topo_zk_tls_cert | string | the cert to use to connect to the zk topo server, requires topo_zk_tls_key, enables TLS |
| -topo_zk_tls_key | string | the key to use to connect to the zk topo server, enables TLS |
| -tracer | string | tracing service to use (default "noop") |
| -tracing-sampling-rate | float | sampling rate for the probabilistic jaeger sampler (default 0.1) |
| -transaction-log-stream-handler | string | URL handler for streaming transactions log (default "/debug/txlog") |
| -transaction_limit_by_component | | Include CallerID.component when considering who the user is for the purpose of transaction limit. |
| -transaction_limit_by_principal | | Include CallerID.principal when considering who the user is for the purpose of transaction limit. (default true) |
| -transaction_limit_by_subcomponent | | Include CallerID.subcomponent when considering who the user is for the purpose of transaction limit. |
| -transaction_limit_by_username | | Include VTGateCallerID.username when considering who the user is for the purpose of transaction limit. (default true)|
| -transaction_limit_per_user | float | Maximum number of transactions a single user is allowed to use at any time, represented as fraction of -transaction_cap. (default 0.4) |
| -transaction_shutdown_grace_period | int | how long to wait (in seconds) for transactions to complete during graceful shutdown. |
| -twopc_abandon_age | float | time in seconds. Any unresolved transaction older than this time will be sent to the coordinator to be resolved. |
| -twopc_coordinator_address | string | address of the (VTGate) process(es) that will be used to notify of abandoned transactions. |
| -twopc_enable | | if the flag is on, 2pc is enabled. Other 2pc flags must be supplied.|
| -tx-throttler-config | string | The configuration of the transaction throttler as a text formatted throttlerdata.Configuration protocol buffer message (default "target_replication_lag_sec: 2 max_replication_lag_sec: 10 initial_rate: 100 max_increase: 1 emergency_decrease: 0.5 min_duration_between_increases_sec: 40 max_duration_between_increases_sec: 62 min_duration_between_decreases_sec: 20 spread_backlog_across_sec: 20 age_bad_rate_after_sec: 180 bad_rate_increase: 0.1 max_rate_approach_threshold: 0.9 ") |
| -tx-throttler-healthcheck-cells | value | A comma-separated list of cells. Only tabletservers running in these cells will be monitored for replication lag by the transaction throttler. |
| -v | value | log level for V logs |
| -version | | print binary version |
| -vmodule | value | comma-separated list of pattern=N settings for file-filtered logging |
| -vreplication_healthcheck_retry_delay | duration | healthcheck retry delay (default 5s) |
| -vreplication_healthcheck_timeout | duration | healthcheck retry delay (default 1m0s) |
| -vreplication_healthcheck_topology_refresh | duration | refresh interval for re-reading the topology (default 30s) |
| -vreplication_retry_delay | duration | delay before retrying a failed binlog connection (default 5s) |
| -vreplication_tablet_type | string | comma separated list of tablet types used as a source (default "REPLICA") |
| -vstream_packet_size | int | Suggested packet size for VReplication streamer. This is used only as a recommendation. The actual packet size may be more or less than this amount. (default 30000) |
| -vtctl_healthcheck_retry_delay | duration | delay before retrying a failed healthcheck (default 5s) |
| -vtctl_healthcheck_timeout | duration | the health check timeout period (default 1m0s) |
| -vtctl_healthcheck_topology_refresh | duration | refresh interval for re-reading the topology (default 30s) |
| -vtgate_grpc_ca | string | the server ca to use to validate servers when connecting |
| -vtgate_grpc_cert | string | the cert to use to connect |
| -vtgate_grpc_key | string | the key to use to connect |
| -vtgate_grpc_server_name | string | the server name to use to validate server certificate |
| -vtgate_protocol | string | how to talk to vtgate (default "grpc") |
| -wait-time | duration | time to wait on an action (default 24h0m0s) |
| -wait_for_drain_sleep_rdonly | duration | time to wait before shutting the query service on old RDONLY tablets during MigrateServedTypes (default 5s) |
| -wait_for_drain_sleep_replica | duration | time to wait before shutting the query service on old REPLICA tablets during MigrateServedTypes (default 15s) |
| -watch_replication_stream | | When enabled, vttablet will stream the MySQL replication stream from the local server, and use it to support the include_event_token ExecuteOptions. |
| -xbstream_restore_flags | string | flags to pass to xbstream command during restore. These should be space separated and will be added to the end of the command. These need to match the ones used for backup e.g. --compress / --decompress, --encrypt / --decrypt |
| -xtrabackup_backup_flags | string | flags to pass to backup command. These should be space separated and will be added to the end of the command |
| -xtrabackup_prepare_flags | string | flags to pass to prepare command. These should be space separated and will be added to the end of the command |
| -xtrabackup_root_path | string | directory location of the xtrabackup executable, e.g., /usr/bin |
| -xtrabackup_stream_mode | string | which mode to use if streaming, valid values are tar and xbstream (default "tar") |
| -xtrabackup_stripe_block_size | uint | Size in bytes of each block that gets sent to a given stripe before rotating to the next stripe (default 102400) |
| -xtrabackup_stripes | uint | If greater than 0, use data striping across this many destination files to parallelize data transfer and decompression |
| -xtrabackup_user | string | User that xtrabackup will use to connect to the database server. This user must have all necessary privileges. For details, please refer to xtrabackup documentation. |
