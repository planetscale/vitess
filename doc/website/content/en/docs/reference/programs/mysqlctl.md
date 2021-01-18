---
title: mysqlctl
---

`mysqlctl` is a command-line tool used for starting `mysqld` binaries. It is responsible for bootstrapping tasks such as generating a configuration file for `mysqld` and ensuring that `mysql_upgrade` is run in the data directory when restoring from backup.

`mysqld_safe` will be also be utilized when present. This helps ensure that `mysqld` is automatically restarted after failures.

## Commands

### init [-wait_time=5m] [-init_db_sql_file=(default)]

Bootstraps a new `mysqld` instance. The MySQL version and flavor will be auto-detected, with a minimal configuration file applied. For example:

```bash
export VTDATAROOT=/tmp
mysqlctl \
 -alsologtostderr \
 -tablet_uid 101 \
 -mysql_port 12345 \
 init
```

### init_config

Bootstraps the configuration for a new `mysqld` instance. This command is the same as `init` except the `mysqld` server will not be started. For example:

```bash
export VTDATAROOT=/tmp
mysqlctl \
 -alsologtostderr \
 -tablet_uid 101 \
 -mysql_port 12345 \
 init_config
```

### reinit_config

Regenerate new configuration files for an existing `mysqld` instance. This could be helpful to revert configuration changes, or to pick up changes made to the bundled config in newer Vitess versions. For example:

```bash
export VTDATAROOT=/tmp
mysqlctl \
 -alsologtostderr \
 -tablet_uid 101 \
 -mysql_port 12345 \
 reinit_config
```

### teardown [-wait_time=5m] [-force]

Remove the data files for a previously shutdown `mysqld` instance. This is a destructive operation:

```bash
export VTDATAROOT=/tmp
mysqlctl -tablet_uid 101 -alsologtostderr teardown
```

### start [-wait_time=5m]

Resume an existing `mysqld` instance that was previously bootstrapped with `init` or `init_config`:

```bash
export VTDATAROOT=/tmp
mysqlctl -tablet_uid 101 -alsologtostderr start
```

### shutdown [-wait_time=5m]

Stop a `mysqld` instance that was previously started with `init` or `start`.

For large `mysqld` instances, you may need to extend the -`wait_time` as flushing dirty pages.

```bash
export VTDATAROOT=/tmp
mysqlctl -tablet_uid 101 -alsologtostderr shutdown
```

## Options

The following global parameters apply to `mysqlctl`:

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| alsologtostderr | boolean | log to standard error as well as files |
| app_idle_timeout | duration | Idle timeout for app connections (default 1m0s) |
| app_pool_size | int | Size of the connection pool for app connections (default 40) |
| backup_engine_implementation | string | Specifies which implementation to use for creating new backups (builtin or xtrabackup). Restores will always be done with whichever engine created a given backup. (default "builtin") |
| backup_storage_block_size | int | if backup_storage_compress is true, backup_storage_block_size sets the byte size for each block while compressing (default is 250000). (default 250000) |
| backup_storage_compress | boolean | if set, the backup files will be compressed (default is true). Set to false for instance if a backup_storage_hook is specified and it compresses the data. (default true) |
| backup_storage_hook | string | if set, we send the contents of the backup files through this hook. |
| backup_storage_implementation | string | which implementation to use for the backup storage feature |
| backup_storage_number_blocks | int | if backup_storage_compress is true, backup_storage_number_blocks sets the number of blocks that can be processed, at once, before the writer blocks, during compression (default is 2). It should be equal to the number of CPUs available for compression (default 2) |
| cpu_profile | string | write cpu profile to file |
| datadog-agent-host | string | host to send spans to. if empty, no tracing will be done |
| datadog-agent-port | string | port to send spans to. if empty, no tracing will be done |
| db-credentials-file | string | db credentials file; send SIGHUP to reload this file |
| db-credentials-server | string | db credentials server type (use 'file' for the file implementation) (default "file") |
| db_charset | string | Character set. Only utf8 or latin1 based character sets are supported. |
| db_connect_timeout_ms | int | connection timeout to mysqld in milliseconds (0 for no timeout) |
| db_dba_password | string | db dba password |
| db_dba_use_ssl | boolean | Set this flag to false to make the dba connection to not use ssl (default true) |
| db_dba_user | string | db dba user userKey (default "vt_dba") |
| db_flags | uint | Flag values as defined by MySQL. |
| db_flavor | string | Flavor overrid. Valid value is FilePos. |
| db_host | string | The host name for the tcp connection. |
| db_port | int | tcp port |
| db_server_name | string | server name of the DB we are connecting to. |
| db_socket | string | The unix socket to connect on. If this is specified, host and port will not be used. |
| db_ssl_ca | string | connection ssl ca |
| db_ssl_ca_path | string | connection ssl ca path |
| db_ssl_cert | string | connection ssl certificate |
| db_ssl_key | string | connection ssl key |
| dba_idle_timeout | duration | Idle timeout for dba connections (default 1m0s) |
| dba_pool_size | int | Size of the connection pool for dba connections (default 20) |
| disable_active_reparents | boolean |  if set, do not allow active reparents. Use this to protect a cluster using external reparents. |
| emit_stats | boolean | true iff we should emit stats to push-based monitoring/stats backends |
| grpc_auth_mode | string | Which auth plugin implementation to use (eg: static) |
| grpc_auth_mtls_allowed_substrings | string | List of substrings of at least one of the client certificate names (separated by colon). |
| grpc_auth_static_client_creds | string | when using grpc_static_auth in the server, this file provides the credentials to use to authenticate with server |
| grpc_auth_static_password_file | string | JSON File to read the users/passwords from. |
| grpc_ca | string | ca to use, requires TLS, and enforces client cert check |
| grpc_cert | string | certificate to use, requires grpc_key, enables TLS |
| grpc_compression | string | how to compress gRPC, default: nothing, supported: snappy |
| grpc_enable_tracing | boolean | Enable GRPC tracing |
| grpc_initial_conn_window_size | int | grpc initial connection window size |
| grpc_initial_window_size | int | grpc initial window size |
| grpc_keepalive_time | duration | After a duration of this time, if the client doesn't see any activity, it pings the server to see if the transport is still alive. (default 10s) | 
| grpc_keepalive_timeout | duration | After having pinged for keepalive check, the client waits for a duration of Timeout and if no activity is seen even after that the connection is closed. (default 10s) |
| grpc_key | string | key to use, requires grpc_cert, enables TLS |
| grpc_max_connection_age | duration | Maximum age of a client connection before GoAway is sent. (default 2562047h47m16.854775807s) |
| grpc_max_connection_age_grace | duration | Additional grace period after grpc_max_connection_age, after which connections are forcibly closed. (default 2562047h47m16.854775807s) |
| grpc_max_message_size | int | Maximum allowed RPC message size. Larger messages will be rejected by gRPC with the error 'exceeding the max size'. (default 16777216) |
| grpc_port | int | Port to listen on for gRPC calls |
| grpc_prometheus | boolean | Enable gRPC monitoring with Prometheus |
| grpc_server_initial_conn_window_size | int | gRPC server initial connection window size |
| grpc_server_initial_window_size | int | gRPC server initial window size |
| grpc_server_keepalive_enforcement_policy_min_time | duration | gRPC server minimum keepalive time (default 5m0s) |
| grpc_server_keepalive_enforcement_policy_permit_without_stream | boolean |  gRPC server permit client keepalive pings even when there are no active streams (RPCs) |
| jaeger-agent-host | string | host and port to send spans to. if empty, no tracing will be done |
| keep_logs | duration | keep logs for this long (using ctime) (zero to keep forever) |
| keep_logs_by_mtime | duration | keep logs for this long (using mtime) (zero to keep forever) |
| lameduck-period | duration | keep running at least this long after SIGTERM before stopping (default 50ms) |
| log_backtrace_at | value | when logging hits line file:N, emit a stack trace |
| log_dir | string | If non-empty, write log files in this directory |
| log_err_stacks | boolean | log stack traces for errors |
| log_rotate_max_size | uint | size in bytes at which logs are rotated (glog.MaxSize) (default 1887436800) |
| logtostderr | boolean | log to standard error instead of files |
| master_connect_retry | duration | how long to wait in between replica reconnect attempts. Only precise to the second. (default 10s) |
| mem-profile-rate | int | profile every n bytes allocated (default 524288) |
| mutex-profile-fraction | int | profile every n mutex contention events (see runtime.SetMutexProfileFraction) |
| mysql_auth_server_static_file | string | JSON File to read the users/passwords from. |
| mysql_auth_server_static_string | string | JSON representation of the users/passwords config. |
| mysql_auth_static_reload_interval | duration | Ticker to reload credentials |
| mysql_clientcert_auth_method | string | client-side authentication method to use. Supported values: mysql_clear_password, dialog. (default "mysql_clear_password") |
| mysql_port | int | mysql port (default 3306) |
| mysql_server_flush_delay | duration | Delay after which buffered response will be flushed to the client. (default 100ms) |
| mysql_socket | string | path to the mysql socket |
| mysqlctl_client_protocol | string | the protocol to use to talk to the mysqlctl server (default "grpc") |
| mysqlctl_mycnf_template | string | template file to use for generating the my.cnf file during server init |
| mysqlctl_socket | string | socket file to use for remote mysqlctl actions (empty for local actions) |
| onterm_timeout | duration | wait no more than this for OnTermSync handlers before stopping (default 10s) |
| pid_file | string | If set, the process will write its pid to the named file, and delete it on graceful shutdown. |
| pool_hostname_resolve_interval | duration | if set force an update to all hostnames and reconnect if changed, defaults to 0 (disabled) |
| port | int | vttablet port (default 6612) |
| purge_logs_interval | duration | how often try to remove old logs (default 1h0m0s) |
| remote_operation_timeout | duration | time to wait for a remote operation (default 30s) |
| security_policy | string | the name of a registered security policy to use for controlling access to URLs - empty means allow all for anyone (built-in policies: deny-all, read-only) |
| service_map | value | comma separated list of services to enable (or disable if prefixed with '-') Example: grpc-vtworker |
| sql-max-length-errors | int | truncate queries in error logs to the given length (default unlimited) |
| sql-max-length-ui | int | truncate queries in debug UIs to the given length (default 512) (default 512) |
| stats_backend | string | The name of the registered push-based monitoring/stats backend to use |
| stats_combine_dimensions | string | List of dimensions to be combined into a single "all" value in exported stats vars |
| stats_drop_variables | string | Variables to be dropped from the list of exported variables. |
| stats_emit_period | duration | Interval between emitting stats to all registered backends (default 1m0s) |
| stderrthreshold | value | logs at or above this threshold go to stderr (default 1) |
| tablet_dir | string | The directory within the vtdataroot to store vttablet/mysql files. Defaults to being generated by the tablet uid. |
| tablet_manager_protocol | string | the protocol to use to talk to vttablet (default "grpc") |
| tablet_uid | uint | tablet uid (default 41983) |
| topo_global_root | string | the path of the global topology data in the global topology server |
| topo_global_server_address | string | the address of the global topology server |
| topo_implementation | string | the topology implementation to use |
| tracer | string | tracing service to use (default "noop") |
| tracing-sampling-rate | float | sampling rate for the probabilistic jaeger sampler (default 0.1) |
| v | value | log level for V logs |
| version | boolean | print binary version |
| vmodule | value | comma-separated list of pattern=N settings for file-filtered logging |
| xbstream_restore_flags | string | flags to pass to xbstream command during restore. These should be space separated and will be added to the end of the command. These need to match the ones used for backup e.g. --compress / --decompress, --encrypt / --decrypt |
| xtrabackup_backup_flags | string | flags to pass to backup command. These should be space separated and will be added to the end of the command |
| xtrabackup_prepare_flags | string | flags to pass to prepare command. These should be space separated and will be added to the end of the command |
| xtrabackup_root_path | string | directory location of the xtrabackup executable, e.g., /usr/bin |
| xtrabackup_stream_mode | string | which mode to use if streaming, valid values are tar and xbstream (default "tar") |
| xtrabackup_stripe_block_size | uint | Size in bytes of each block that gets sent to a given stripe before rotating to the next stripe (default 102400) |
| xtrabackup_stripes | uint | If greater than 0, use data striping across this many destination files to parallelize data transfer and decompression |
| xtrabackup_user | string | User that xtrabackup will use to connect to the database server. This user must have all necessary privileges. For details, please refer to xtrabackup documentation.
