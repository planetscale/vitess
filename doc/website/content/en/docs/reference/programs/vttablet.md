---
title: vttablet
aliases: ['/docs/user-guides/vttablet-modes/','/docs/reference/vttablet-modes/']
---

A VTTablet server _controls_ a running MySQL server. VTTablet supports two primary types of deployments:

* Managed MySQL (most common)
* Unmanaged or Remote MySQL

In addition to these deployment types, a partially managed VTTablet is also possible by setting `-disable_active_reparents`. 


## Example Usage

### Managed MySQL

In this mode, Vitess actively manages MySQL:

```bash
export TOPOLOGY_FLAGS="-topo_implementation etcd2 -topo_global_server_address localhost:2379 -topo_global_root /vitess/global"
export VTDATAROOT="/tmp"

vttablet \
$TOPOLOGY_FLAGS
-tablet-path $alias
-init_keyspace $keyspace
-init_shard $shard
-init_tablet_type $tablet_type
-port $port
-grpc_port $grpc_port
-service_map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream'
```

`$alias` needs to be of the form: `<cell>-id`, and the cell should match one of the local cells that was created in the topology. The id can be left padded with zeroes: `cell-100` and `cell-000000100` are synonymous.

### Unmanaged or Remote MySQL

In this mode, an external MySQL can be used such as RDS, Aurora, CloudSQL:

```bash
mkdir -p $VTDATAROOT/vt_0000000401
vttablet \
 $TOPOLOGY_FLAGS \
 -logtostderr \
 -log_queries_to_file $VTDATAROOT/tmp/vttablet_0000000401_querylog.txt \
 -tablet-path "zone1-0000000401" \
 -init_keyspace legacy \
 -init_shard 0 \
 -init_tablet_type replica \
 -port 15401 \
 -grpc_port 16401 \
 -service_map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
 -pid_file $VTDATAROOT/vt_0000000401/vttablet.pid \
 -vtctld_addr http://localhost:15000/ \
 -db_host 127.0.0.1 \
 -db_port 5726 \
 -db_app_user msandbox \
 -db_app_password msandbox \
 -db_dba_user msandbox \
 -db_dba_password msandbox \
 -db_repl_user msandbox \
 -db_repl_password msandbox \
 -db_filtered_user msandbox \
 -db_filtered_password msandbox \
 -db_allprivs_user msandbox \
 -db_allprivs_password msandbox \
 -init_db_name_override legacy \
 -init_populate_metadata &

sleep 10
vtctlclient TabletExternallyReparented zone1-401
```

See [Unmanaged Tablet](../../../user-guides/configuration-advanced/unmanaged-tablet) for the full guide.

### Partially managed MySQL

Even if a MySQL is remote, you can still make vttablet perform some management functions. They are as follows:

* `-disable_active_reparents`: If this flag is set, then any reparent or replica commands will not be allowed. These are InitShardMaster, PlannedReparent, PlannedReparent, EmergencyReparent, and ReparentTablet. In this mode, you should use the TabletExternallyReparented command to inform vitess of the current master.
* `-master_connect_retry`: This value is give to mysql when it connects a replica to the master as the retry duration parameter.
* `-enable_replication_reporter`: If this flag is set, then vttablet will transmit replica lag related information to the vtgates, which will allow it to balance load better. Additionally, enabling this will also cause vttablet to restart replication if it was stopped. However, it will do this only if -disable_active_reparents was not turned on.
* `-enable_semi_sync`: This option will automatically enable semi-sync on new replicas as well as on any tablet that transitions into a replica type. This includes the demotion of a master to a replica.
* `-heartbeat_enable` and `-heartbeat interval duration`: cause vttablet to write heartbeats to the sidecar database. This information is also used by the replication reporter to assess replica lag.


## Options

The following global options apply to `vttablet`:

| Name | Type | Definition |
| :------------------------------------ | :--------- | :----------------------------------------------------------------------------------------- |
| -alsologtostderr | boolean | log to standard error as well as files |
| -app_idle_timeout | duration | Idle timeout for app connections (default 1m0s) |
| -app_pool_size | int | Size of the connection pool for app connections (default 40) |
| -azblob_backup_account_key_file | string | Path to a file containing the Azure Storage account key; if this flag is unset, the environment variable VT_AZBLOB_ACCOUNT_KEY will be used as the key itself (NOT a file path) |
| -azblob_backup_account_name | string | Azure Storage Account name for backups; if this flag is unset, the environment variable VT_AZBLOB_ACCOUNT_NAME will be used |
| -azblob_backup_container_name | string | Azure Blob Container Name |
| -azblob_backup_parallelism | int | Azure Blob operation parallelism (requires extra memory when increased) (default 1) |
| -azblob_backup_storage_root | string | Root prefix for all backup-related Azure Blobs; this should exclude both initial and trailing '/' (e.g. just 'a/b' not '/a/b/') |
| -backup_engine_implementation | string | Specifies which implementation to use for creating new backups (builtin or xtrabackup). Restores will always be done with whichever engine created a given backup. (default "builtin") |
| -backup_storage_block_size | int | if backup_storage_compress is true, backup_storage_block_size sets the byte size for each block while compressing (default is 250000). (default 250000) |
| -backup_storage_compress |  | if set, the backup files will be compressed (default is true). Set to false for instance if a backup_storage_hook is specified and it compresses the data. (default true) |
| -backup_storage_hook | string | if set, we send the contents of the backup files through this hook. |
| -backup_storage_implementation | string | which implementation to use for the backup storage feature |
| -backup_storage_number_blocks | int | if backup_storage_compress is true, backup_storage_number_blocks sets the number of blocks that can be processed, at once, before the writer blocks, during compression (default is 2). It should be equal to the number of CPUs available for compression (default 2) |
| -binlog_player_grpc_ca | string | the server ca to use to validate servers when connecting |
| -binlog_player_grpc_cert | string | the cert to use to connect |
| -binlog_player_grpc_key | string | the key to use to connect |
| -binlog_player_grpc_server_name | string | the server name to use to validate server certificate |
| -binlog_player_protocol | string | the protocol to download binlogs from a vttablet (default "grpc") |
| -binlog_use_v3_resharding_mode |  | True iff the binlog streamer should use V3-style sharding, which doesn't require a preset sharding key column. (default true) |
| -ceph_backup_storage_config | string | Path to JSON config file for ceph backup storage (default "ceph_backup_config.json") |
| -consul_auth_static_file | string | JSON File to read the topos/tokens from. |
| -cpu_profile | string | write cpu profile to file |
| -datadog-agent-host | string | host to send spans to. if empty, no tracing will be done |
| -datadog-agent-port | string | port to send spans to. if empty, no tracing will be done |
| -db-credentials-file | string | db credentials file; send SIGHUP to reload this file |
| -db-credentials-server | string | db credentials server type (use 'file' for the file implementation) (default "file") |
| -db_allprivs_password | string | db allprivs password |
| -db_allprivs_use_ssl |  | Set this flag to false to make the allprivs connection to not use ssl (default true) |
| -db_allprivs_user | string | db allprivs user userKey (default "vt_allprivs") |
| -db_app_password | string | db app password |
| -db_app_use_ssl |  | Set this flag to false to make the app connection to not use ssl (default true) |
| -db_app_user | string | db app user userKey (default "vt_app") |
| -db_appdebug_password | string | db appdebug password |
| -db_appdebug_use_ssl |  | Set this flag to false to make the appdebug connection to not use ssl (default true) |
| -db_appdebug_user | string | db appdebug user userKey (default "vt_appdebug") |
| -db_charset | string | Character set. Only utf8 or latin1 based character sets are supported. |
| -db_connect_timeout_ms | int | connection timeout to mysqld in milliseconds (0 for no timeout) |
| -db_dba_password | string | db dba password |
| -db_dba_use_ssl |  | Set this flag to false to make the dba connection to not use ssl (default true) |
| -db_dba_user | string | db dba user userKey (default "vt_dba") |
| -db_erepl_password | string | db erepl password |
| -db_erepl_use_ssl |  | Set this flag to false to make the erepl connection to not use ssl (default true) |
| -db_erepl_user | string | db erepl user userKey (default "vt_erepl") |
| -db_filtered_password | string | db filtered password |
| -db_filtered_use_ssl |  | Set this flag to false to make the filtered connection to not use ssl (default true) |
| -db_filtered_user | string | db filtered user userKey (default "vt_filtered") |
| -db_flags | uint | Flag values as defined by MySQL. |
| -db_flavor | string | Flavor overrid. Valid value is FilePos. |
| -db_host | string | The host name for the tcp connection. |
| -db_port | int | tcp port |
| -db_repl_password | string | db repl password |
| -db_repl_use_ssl |  | Set this flag to false to make the repl connection to not use ssl (default true) |
| -db_repl_user | string | db repl user userKey (default "vt_repl") |
| -db_server_name | string | server name of the DB we are connecting to. |
| -db_socket | string | The unix socket to connect on. If this is specified, host and port will not be used. |
| -db_ssl_ca | string | connection ssl ca |
| -db_ssl_ca_path | string | connection ssl ca path |
| -db_ssl_cert | string | connection ssl certificate |
| -db_ssl_key | string | connection ssl key |
| -dba_idle_timeout | duration | Idle timeout for dba connections (default 1m0s) |
| -dba_pool_size | int | Size of the connection pool for dba connections (default 20) |
| -degraded_threshold | duration | replication lag after which a replica is considered degraded (only used in status UI) (default 30s) |
| -demote_master_type | string | the tablet type a demoted master will transition to (default "REPLICA") |
| -disable_active_reparents |  | if set, do not allow active reparents. Use this to protect a cluster using external reparents. |
| -discovery_high_replication_lag_minimum_serving | duration | the replication lag that is considered too high when selecting the minimum num vttablets for serving (default 2h0m0s) |
| -discovery_low_replication_lag | duration | the replication lag that is considered low enough to be healthy (default 30s) |
| -emit_stats |  | true iff we should emit stats to push-based monitoring/stats backends |
| -enable-consolidator |  | This option enables the query consolidator. (default true) |
| -enable-consolidator-replicas |  | This option enables the query consolidator only on replicas. |
| -enable-query-plan-field-caching |  | This option fetches & caches fields (columns) when storing query plans (default true) |
| -enable-tx-throttler |  | If true replication-lag-based throttling on transactions will be enabled. |
| -enable_hot_row_protection |  | If true, incoming transactions for the same row (range) will be queued and cannot consume all txpool slots. |
| -enable_hot_row_protection_dry_run |  | If true, hot row protection is not enforced but logs if transactions would have been queued. |
| -enable_replication_reporter |  | Register the health check module that monitors MySQL replication |
| -enable_semi_sync |  | Enable semi-sync when configuring replication, on master and replica tablets only (rdonly tablets will not ack). |
| -enable_transaction_limit |  | If true, limit on number of transactions open at the same time will be enforced for all users. User trying to open a new transaction after exhausting their limit will receive an error immediately, regardless of whether there are available slots or not. |
| -enable_transaction_limit_dry_run |  | If true, limit on number of transactions open at the same time will be tracked for all users, but not enforced. |
| -enforce-tableacl-config |  | if this flag is true, vttablet will fail to start if a valid tableacl config does not exist |
| -enforce_strict_trans_tables |  | If true, vttablet requires MySQL to run with STRICT_TRANS_TABLES or STRICT_ALL_TABLES on. It is recommended to not turn this flag off. Otherwise MySQL may alter your supplied values before saving them to the database. (default true) |
| -file_backup_storage_root | string | root directory for the file backup storage |
| -filecustomrules | string | file based custom rule path |
| -finalize_external_reparent_timeout | duration | Timeout for the finalize stage of a fast external reparent reconciliation. (default 30s) |
| -gcs_backup_storage_bucket | string | Google Cloud Storage bucket to use for backups |
| -gcs_backup_storage_root | string | root prefix for all backup-related object names |
| -grpc_auth_mode | string | Which auth plugin implementation to use (eg: static) |
| -grpc_auth_mtls_allowed_substrings | string | List of substrings of at least one of the client certificate names (separated by colon). |
| -grpc_auth_static_client_creds | string | when using grpc_static_auth in the server, this file provides the credentials to use to authenticate with server |
| -grpc_auth_static_password_file | string | JSON File to read the users/passwords from. |
| -grpc_ca | string | ca to use, requires TLS, and enforces client cert check |
| -grpc_cert | string | certificate to use, requires grpc_key, enables TLS |
| -grpc_compression | string | how to compress gRPC, default: nothing, supported: snappy |
| -grpc_enable_tracing |  | Enable GRPC tracing |
| -grpc_initial_conn_window_size | int | grpc initial connection window size |
| -grpc_initial_window_size | int | grpc initial window size |
| -grpc_keepalive_time | duration | After a duration of this time if the client doesn't see any activity it pings the server to see if the transport is still alive. (default 10s) |
| -grpc_keepalive_timeout | duration | After having pinged for keepalive check, the client waits for a duration of Timeout and if no activity is seen even after that the connection is closed. (default 10s) |
| -grpc_key | string | key to use, requires grpc_cert, enables TLS |
| -grpc_max_connection_age | duration | Maximum age of a client connection before GoAway is sent. (default 2562047h47m16.854775807s) |
| -grpc_max_connection_age_grace | duration | Additional grace period after grpc_max_connection_age, after which connections are forcibly closed. (default 2562047h47m16.854775807s) |
| -grpc_max_message_size | int | Maximum allowed RPC message size. Larger messages will be rejected by gRPC with the error 'exceeding the max size'. (default 16777216) |
| -grpc_port | int | Port to listen on for gRPC calls |
| -grpc_prometheus |  | Enable gRPC monitoring with Prometheus |
| -grpc_server_initial_conn_window_size | int | grpc server initial connection window size |
| -grpc_server_initial_window_size | int | grpc server initial window size |
| -grpc_server_keepalive_enforcement_policy_min_time | duration | grpc server minimum keepalive time (default 5m0s) |
| -grpc_server_keepalive_enforcement_policy_permit_without_stream |  | grpc server permit client keepalive pings even when there are no active streams (RPCs) |
| -health_check_interval | duration | Interval between health checks (default 20s) |
| -heartbeat_enable |  | If true, vttablet records (if master) or checks (if replica) the current time of a replication heartbeat in the table _vt.heartbeat. The result is used to inform the serving state of the vttablet via healthchecks. |
| -heartbeat_interval | duration | How frequently to read and write replication heartbeat. (default 1s) |
| -hot_row_protection_concurrent_transactions | int | Number of concurrent transactions let through to the txpool/MySQL for the same hot row. Should be > 1 to have enough 'ready' transactions in MySQL and benefit from a pipelining effect. (default 5) |
| -hot_row_protection_max_global_queue_size | int | Global queue limit across all row (ranges). Useful to prevent that the queue can grow unbounded. (default 1000) |
| -hot_row_protection_max_queue_size | int | Maximum number of BeginExecute RPCs which will be queued for the same row (range). (default 20) |
| -init_db_name_override | string | (init parameter) override the name of the db used by vttablet. Without this flag, the db name defaults to vt_<keyspacename> |
| -init_keyspace | string | (init parameter) keyspace to use for this tablet |
| -init_populate_metadata |  | (init parameter) populate metadata tables even if restore_from_backup is disabled. If restore_from_backup is enabled, metadata tables are always populated regardless of this flag. |
| -init_shard | string | (init parameter) shard to use for this tablet |
| -init_tablet_type | string | (init parameter) the tablet type to use for this tablet. |
| -init_tags | value | (init parameter) comma separated list of key:value pairs used to tag the tablet |
| -init_timeout | duration | (init parameter) timeout to use for the init phase. (default 1m0s) |
| -jaeger-agent-host | string | host and port to send spans to. if empty, no tracing will be done |
| -keep_logs | duration | keep logs for this long (using ctime) (zero to keep forever) |
| -keep_logs_by_mtime | duration | keep logs for this long (using mtime) (zero to keep forever) |
| -lameduck-period | duration | keep running at least this long after SIGTERM before stopping (default 50ms) |
| -legacy_replication_lag_algorithm |  | use the legacy algorithm when selecting the vttablets for serving (default true) |
| -lock_tables_timeout | duration | How long to keep the table locked before timing out (default 1m0s) |
| -log_backtrace_at | value | when logging hits line file:N, emit a stack trace |
| -log_dir | string | If non-empty, write log files in this directory |
| -log_err_stacks |  | log stack traces for errors |
| -log_queries |  | Enable query logging to syslog. |
| -log_queries_to_file | string | Enable query logging to the specified file |
| -log_rotate_max_size | uint | size in bytes at which logs are rotated (glog.MaxSize) (default 1887436800) |
| -logtostderr |  | log to standard error instead of files |
| -master_connect_retry | duration | how long to wait in between replica reconnect attempts. Only precise to the second. (default 10s) |
| -mem-profile-rate | int | profile every n bytes allocated (default 524288) |
| -min_number_serving_vttablets | int | the minimum number of vttablets that will be continue to be used even with low replication lag (default 2) |
| -mutex-profile-fraction | int | profile every n mutex contention events (see runtime.SetMutexProfileFraction) |
| -mycnf-file | string | path to my.cnf, if reading all config params from there |
| -mycnf_bin_log_path | string | mysql binlog path |
| -mycnf_data_dir | string | data directory for mysql |
| -mycnf_error_log_path | string | mysql error log path |
| -mycnf_general_log_path | string | mysql general log path |
| -mycnf_innodb_data_home_dir | string | Innodb data home directory |
| -mycnf_innodb_log_group_home_dir | string | Innodb log group home directory |
| -mycnf_master_info_file | string | mysql master.info file |
| -mycnf_mysql_port | int | port mysql is listening on |
| -mycnf_pid_file | string | mysql pid file |
| -mycnf_relay_log_index_path | string | mysql relay log index path |
| -mycnf_relay_log_info_path | string | mysql relay log info path |
| -mycnf_relay_log_path | string | mysql relay log path |
| -mycnf_server_id | int | mysql server id of the server (if specified, mycnf-file will be ignored) |
| -mycnf_slow_log_path | string | mysql slow query log path |
| -mycnf_socket_file | string | mysql socket file |
| -mycnf_tmp_dir | string | mysql tmp directory |
| -mysql_auth_server_static_file | string | JSON File to read the users/passwords from. |
| -mysql_auth_server_static_string | string | JSON representation of the users/passwords config. |
| -mysql_auth_static_reload_interval | duration | Ticker to reload credentials |
| -mysql_clientcert_auth_method | string | client-side authentication method to use. Supported values: mysql_clear_password, dialog. (default "mysql_clear_password") |
| -mysql_server_flush_delay | duration | Delay after which buffered response will flushed to client. (default 100ms) |
| -mysqlctl_client_protocol | string | the protocol to use to talk to the mysqlctl server (default "grpc") |
| -mysqlctl_mycnf_template | string | template file to use for generating the my.cnf file during server init |
| -mysqlctl_socket | string | socket file to use for remote mysqlctl actions (empty for local actions) |
| -onterm_timeout | duration | wait no more than this for OnTermSync handlers before stopping (default 10s) |
| -opentsdb_uri | string | URI of opentsdb /api/put method |
| -orc_api_password | string | (Optional) Basic auth password to authenticate with Orchestrator's HTTP API. |
| -orc_api_url | string | Address of Orchestrator's HTTP API (e.g. http://host:port/api/). Leave empty to disable Orchestrator integration. |
| -orc_api_user | string | (Optional) Basic auth username to authenticate with Orchestrator's HTTP API. Leave empty to disable basic auth. |
| -orc_discover_interval | duration | How often to ping Orchestrator's HTTP API endpoint to tell it we exist. 0 means never. |
| -orc_timeout | duration | Timeout for calls to Orchestrator's HTTP API (default 30s) |
| -pid_file | string | If set, the process will write its pid to the named file, and delete it on graceful shutdown. |
| -pool_hostname_resolve_interval | duration | if set force an update to all hostnames and reconnect if changed, defaults to 0 (disabled) |
| -port | int | port for the server |
| -purge_logs_interval | duration | how often try to remove old logs (default 1h0m0s) |
| -query-log-stream-handler | string | URL handler for streaming queries log (default "/debug/querylog") |
| -querylog-filter-tag | string | string that must be present in the query as a comment for the query to be logged, works for both vtgate and vttablet |
| -querylog-format | string | format for query logs ("text" or "json") (default "text") |
| -queryserver-config-acl-exempt-acl | string | an acl that exempt from table acl checking (this acl is free to access any vitess tables). |
| -queryserver-config-enable-table-acl-dry-run |  | If this flag is enabled, tabletserver will emit monitoring metrics and let the request pass regardless of table acl check results |
| -queryserver-config-idle-timeout | int | query server idle timeout (in seconds), vttablet manages various mysql connection pools. This config means if a connection has not been used in given idle timeout, this connection will be removed from pool. This effectively manages number of connection objects and optimize the pool performance. (default 1800) |
| -queryserver-config-max-dml-rows | int | query server max dml rows per statement, maximum number of rows allowed to return at a time for an update or delete with either 1) an equality where clauses on primary keys, or 2) a subselect statement. For update and delete statements in above two categories, vttablet will split the original query into multiple small queries based on this configuration value.  |
| -queryserver-config-max-result-size | int | query server max result size, maximum number of rows allowed to return from vttablet for non-streaming queries. (default 10000) |
| -queryserver-config-message-postpone-cap | int | query server message postpone cap is the maximum number of messages that can be postponed at any given time. Set this number to substantially lower than transaction cap, so that the transaction pool isn't exhausted by the message subsystem. (default 4) |
| -queryserver-config-passthrough-dmls |  | query server pass through all dml statements without rewriting |
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
| -queryserver-config-strict-table-acl |  | only allow queries that pass table acl checks |
| -queryserver-config-terse-errors |  | prevent bind vars from escaping in returned errors |
| -queryserver-config-transaction-cap | int | query server transaction cap is the maximum number of transactions allowed to happen at any given point of a time for a single vttablet. E.g. by setting transaction cap to 100, there are at most 100 transactions will be processed by a vttablet and the 101th transaction will be blocked (and fail if it cannot get connection within specified timeout) (default 20) |
| -queryserver-config-transaction-prefill-parallelism | int | query server transaction prefill parallelism, a non-zero value will prefill the pool using the specified parallism. |
| -queryserver-config-transaction-timeout | int | query server transaction timeout (in seconds), a transaction will be killed if it takes longer than this value (default 30) |
| -queryserver-config-txpool-timeout | int | query server transaction pool timeout, it is how long vttablet waits if tx pool is full (default 1) |
| -queryserver-config-txpool-waiter-cap | int | query server transaction pool waiter limit, this is the maximum number of transactions that can be queued waiting to get a connection (default 5000) |
| -queryserver-config-warn-result-size | int | query server result size warning threshold, warn if number of rows returned from vttablet for non-streaming queries exceeds this |
| -redact-debug-ui-queries |  | redact full queries and bind variables from debug UI |
| -remote_operation_timeout | duration | time to wait for a remote operation (default 30s) |
| -restore_concurrency | int | (init restore parameter) how many concurrent files to restore at once (default 4) |
| -restore_from_backup |  | (init restore parameter) will check BackupStorage for a recent backup at startup and start there |
| -s3_backup_aws_endpoint | string | endpoint of the S3 backend (region must be provided) |
| -s3_backup_aws_region | string | AWS region to use (default "us-east-1") |
| -s3_backup_aws_retries | int | AWS request retries (default -1) |
| -s3_backup_force_path_style |  | force the s3 path style |
| -s3_backup_log_level | string | determine the S3 loglevel to use from LogOff, LogDebug, LogDebugWithSigning, LogDebugWithHTTPBody, LogDebugWithRequestRetries, LogDebugWithRequestErrors (default "LogOff") |
| -s3_backup_server_side_encryption | string | server-side encryption algorithm (e.g., AES256, aws:kms) |
| -s3_backup_storage_bucket | string | S3 bucket to use for backups |
| -s3_backup_storage_root | string | root prefix for all backup-related object names |
| -s3_backup_tls_skip_verify_cert |  | skip the 'certificate is valid' check for SSL connections |
| -security_policy | string | the name of a registered security policy to use for controlling access to URLs - empty means allow all for anyone (built-in policies: deny-all, read-only) |
| -service_map | value | comma separated list of services to enable (or disable if prefixed with '-') Example: grpc-vtworker |
| -serving_state_grace_period | duration | how long to pause after broadcasting health to vtgate, before enforcing a new serving state |
| -shard_sync_retry_delay | duration | delay between retries of updates to keep the tablet and its shard record in sync (default 30s) |
| -sql-max-length-errors | int | truncate queries in error logs to the given length (default unlimited) |
| -sql-max-length-ui | int | truncate queries in debug UIs to the given length (default 512) (default 512) |
| -srv_topo_cache_refresh | duration | how frequently to refresh the topology for cached entries (default 1s) |
| -srv_topo_cache_ttl | duration | how long to use cached entries for topology (default 1s) |
| -stats_backend | string | The name of the registered push-based monitoring/stats backend to use |
| -stats_combine_dimensions | string | List of dimensions to be combined into a single "all" value in exported stats vars |
| -stats_drop_variables | string | Variables to be dropped from the list of exported variables. |
| -stats_emit_period | duration | Interval between emitting stats to all registered backends (default 1m0s) |
| -stderrthreshold | value | logs at or above this threshold go to stderr (default 1) |
| -table-acl-config | string | path to table access checker config file; send SIGHUP to reload this file |
| -table-acl-config-reload-interval | duration | Ticker to reload ACLs |
| -tablet-path | string | tablet alias |
| -tablet_config | string | YAML file config for tablet |
| -tablet_dir | string | The directory within the vtdataroot to store vttablet/mysql files. Defaults to being generated by the tablet uid. |
| -tablet_grpc_ca | string | the server ca to use to validate servers when connecting |
| -tablet_grpc_cert | string | the cert to use to connect |
| -tablet_grpc_key | string | the key to use to connect |
| -tablet_grpc_server_name | string | the server name to use to validate server certificate |
| -tablet_hostname | string | if not empty, this hostname will be assumed instead of trying to resolve it |
| -tablet_manager_grpc_ca | string | the server ca to use to validate servers when connecting |
| -tablet_manager_grpc_cert | string | the cert to use to connect |
| -tablet_manager_grpc_concurrency | int | concurrency to use to talk to a vttablet server for performance-sensitive RPCs (like ExecuteFetchAs{Dba,AllPrivs,App}) (default 8) |
| -tablet_manager_grpc_key | string | the key to use to connect |
| -tablet_manager_grpc_server_name | string | the server name to use to validate server certificate |
| -tablet_manager_protocol | string | the protocol to use to talk to vttablet (default "grpc") |
| -tablet_protocol | string | how to talk to the vttablets (default "grpc") |
| -tablet_url_template | string | format string describing debug tablet url formatting. See the Go code for getTabletDebugURL() how to customize this. (default "http://{{.GetTabletHostPort}}") |
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
| -topocustomrule_cell | string | topo cell for customrules file. (default "global") |
| -topocustomrule_path | string | path for customrules file. Disabled if empty. |
| -tracer | string | tracing service to use (default "noop") |
| -tracing-sampling-rate | float | sampling rate for the probabilistic jaeger sampler (default 0.1) |
| -transaction-log-stream-handler | string | URL handler for streaming transactions log (default "/debug/txlog") |
| -transaction_limit_by_component |  | Include CallerID.component when considering who the user is for the purpose of transaction limit. |
| -transaction_limit_by_principal |  | Include CallerID.principal when considering who the user is for the purpose of transaction limit. (default true) |
| -transaction_limit_by_subcomponent |  | Include CallerID.subcomponent when considering who the user is for the purpose of transaction limit. |
| -transaction_limit_by_username |  | Include VTGateCallerID.username when considering who the user is for the purpose of transaction limit. (default true) |
| -transaction_limit_per_user | float | Maximum number of transactions a single user is allowed to use at any time, represented as fraction of -transaction_cap. (default 0.4) |
| -shutdown_grace_period | float | how long to wait (in seconds) for queries and transactions to complete during graceful shutdown. |
| -twopc_abandon_age | float | time in seconds. Any unresolved transaction older than this time will be sent to the coordinator to be resolved. |
| -twopc_coordinator_address | string | address of the (VTGate) process(es) that will be used to notify of abandoned transactions. |
| -twopc_enable |  | if the flag is on, 2pc is enabled. Other 2pc flags must be supplied. |
| -tx-throttler-config | string | The configuration of the transaction throttler as a text formatted throttlerdata.Configuration protocol buffer message (default "target_replication_lag_sec: 2 max_replication_lag_sec: 10 initial_rate: 100 max_increase: 1 emergency_decrease: 0.5 min_duration_between_increases_sec: 40 max_duration_between_increases_sec: 62 min_duration_between_decreases_sec: 20 spread_backlog_across_sec: 20 age_bad_rate_after_sec: 180 bad_rate_increase: 0.1 max_rate_approach_threshold: 0.9 ") |
| -tx-throttler-healthcheck-cells | value | A comma-separated list of cells. Only tabletservers running in these cells will be monitored for replication lag by the transaction throttler. |
| -unhealthy_threshold | duration | replication lag  after which a replica is considered unhealthy (default 2h0m0s) |
| -use_super_read_only |  | Set super_read_only flag when performing planned failover. |
| -v | value | log level for V logs |
| -version |  | print binary version |
| -vmodule | value | comma-separated list of pattern=N settings for file-filtered logging |
| -vreplication_healthcheck_retry_delay | duration | healthcheck retry delay (default 5s) |
| -vreplication_healthcheck_timeout | duration | healthcheck retry delay (default 1m0s) |
| -vreplication_healthcheck_topology_refresh | duration | refresh interval for re-reading the topology (default 30s) |
| -vreplication_retry_delay | duration | delay before retrying a failed binlog connection (default 5s) |
| -vreplication_tablet_type | string | comma separated list of tablet types used as a source (default "REPLICA") |
| -vstream_packet_size | int | Suggested packet size for VReplication streamer. This is used only as a recommendation. The actual packet size may be more or less than this amount. (default 30000) |
| -vtctld_addr | string | address of a vtctld instance |
| -vtgate_protocol | string | how to talk to vtgate (default "grpc") |
| -wait_for_backup_interval | duration | (init restore parameter) if this is greater than 0, instead of starting up empty when no backups are found, keep checking at this interval for a backup to appear |
| -watch_replication_stream |  | When enabled, vttablet will stream the MySQL replication stream from the local server, and use it to support the include_event_token ExecuteOptions. |
| -xbstream_restore_flags | string | flags to pass to xbstream command during restore. These should be space separated and will be added to the end of the command. These need to match the ones used for backup e.g. --compress / --decompress, --encrypt / --decrypt |
| -xtrabackup_backup_flags | string | flags to pass to backup command. These should be space separated and will be added to the end of the command |
| -xtrabackup_prepare_flags | string | flags to pass to prepare command. These should be space separated and will be added to the end of the command |
| -xtrabackup_root_path | string | directory location of the xtrabackup executable, e.g., /usr/bin |
| -xtrabackup_stream_mode | string | which mode to use if streaming, valid values are tar and xbstream (default "tar") |
| -xtrabackup_stripe_block_size | uint | Size in bytes of each block that gets sent to a given stripe before rotating to the next stripe (default 102400) |
| -xtrabackup_stripes | uint | If greater than 0, use data striping across this many destination files to parallelize data transfer and decompression |
| -xtrabackup_user | string | User that xtrabackup will use to connect to the database server. This user must have all necessary privileges. For details, please refer to xtrabackup documentation. |

### Key Options

* -restore_from_backup: The default value for this flag is false. If set to true, and the my.cnf file was successfully loaded, then vttablet can perform automatic restores as follows:

	* If started against a mysql instance that has no data files, it will search the list of backups for the latest one, and initiate a restore. After this, it will point the mysql to the current master and wait for replication to catch up. Once replication is caught up to the specified tolerance limit, it will advertise itself as serving. This will cause the vtgates to add it to the list of healthy tablets to serve queries from.
	* If this flag is true, but my.cnf was not loaded, then vttablet will fatally exit with an error message.
	* You can additionally control the level of concurrency for a restore with the `-restore_concurrency` flag. This is typically useful in cloud environments to prevent the restore process from becoming a 'noisy' neighbor by consuming all available disk IOPS.
