---
title: vtgate
---

VTGate is a stateless proxy responsible for accepting requests from applications and routing them to the appropriate tablet server(s) for query execution. It speaks both the MySQL Protocol and a gRPC protocol.

## Example Usage

Start a vtgate proxy:

```bash
export TOPOLOGY_FLAGS="-topo_implementation etcd2 -topo_global_server_address localhost:2379 -topo_global_root /vitess/global"
export VTDATAROOT="/tmp"

vtgate \
  $TOPOLOGY_FLAGS \
  -log_dir $VTDATAROOT/tmp \
  -port 15001 \
  -grpc_port 15991 \
  -mysql_server_port 15306 \
  -cell test \
  -cells_to_watch test \
  -tablet_types_to_wait MASTER,REPLICA \
  -gateway_implementation discoverygateway \
  -service_map 'grpc-vtgateservice' \
  -pid_file $VTDATAROOT/tmp/vtgate.pid \
  -mysql_auth_server_impl none
```

## Options

The following global options apply to `vtgate`:

| Name | Type | Definition |
| :------------------------------------ | :--------- | :----------------------------------------------------------------------------------------- |
| -allowed_tablet_types | value | Specifies the tablet types this vtgate is allowed to route queries to |
| -alsologtostderr | boolean | log to standard error as well as files |
| -buffer_drain_concurrency | int | Maximum number of requests retried simultaneously. More concurrency will increase the load on the MASTER vttablet when draining the buffer. (default 1) |
| -buffer_keyspace_shards | string | If not empty, limit buffering to these entries (comma separated). Entry format: keyspace or keyspace/shard. Requires --enable_buffer=true. |
| -buffer_max_failover_duration | duration | Stop buffering completely if a failover takes longer than this duration. (default 20s) |
| -buffer_min_time_between_failovers | duration | Minimum time between the end of a failover and the start of the next one (tracked per shard). Faster consecutive failovers will not trigger buffering. (default 1m0s) |
| -buffer_size | int | Maximum number of buffered requests in flight (across all ongoing failovers). (default 10) |
| -buffer_window | duration | Duration for how long a request should be buffered at most. (default 10s) |
| -cell | string | cell to use (default "test_nj") |
| -cells_to_watch | string | comma-separated list of cells for watching tablets |
| -consul_auth_static_file | string | JSON File to read the topos/tokens from. |
| -cpu_profile | string | write cpu profile to file |
| -datadog-agent-host | string | host to send spans to. if empty, no tracing will be done |
| -datadog-agent-port | string | port to send spans to. if empty, no tracing will be done |
| -default_tablet_type | value | The default tablet type to set for queries, when one is not explicitly selected (default MASTER) |
| -discovery_high_replication_lag_minimum_serving | duration | the replication lag that is considered too high when selecting the minimum num vttablets for serving (default 2h0m0s) |
| -discovery_low_replication_lag | duration | the replication lag that is considered low enough to be healthy (default 30s) |
| -emit_stats | boolean | true iff we should emit stats to push-based monitoring/stats backends |
| -enable_buffer | boolean | Enable buffering (stalling) of master traffic during failovers. |
| -enable_buffer_dry_run | boolean | Detect and log failover events, but do not actually buffer requests. |
| -gate_query_cache_size | int | gate server query cache size, maximum number of queries to be cached. vtgate analyzes every incoming query and generate a query plan, these plans are being cached in a lru cache. This config controls the capacity of the lru cache. (default 10000) |
| -gateway_implementation | string | The implementation of gateway (default "discoverygateway") |
| -gateway_initial_tablet_timeout | duration | At startup, the gateway will wait up to that duration to get one tablet per keyspace/shard/tablettype (default 30s) |
| -grpc_auth_mode | string | Which auth plugin implementation to use (eg: static) |
| -grpc_auth_mtls_allowed_substrings | string | List of substrings of at least one of the client certificate names (separated by colon). |
| -grpc_auth_static_client_creds | string | when using grpc_static_auth in the server, this file provides the credentials to use to authenticate with server |
| -grpc_auth_static_password_file | string | JSON File to read the users/passwords from. |
| -grpc_ca | string | ca to use, requires TLS, and enforces client cert check |
| -grpc_cert | string | certificate to use, requires grpc_key, enables TLS |
| -grpc_compression | string | how to compress gRPC, default: nothing, supported: snappy |
| -grpc_enable_tracing | boolean | Enable GRPC tracing |
| -grpc_initial_conn_window_size | int | grpc initial connection window size |
| -grpc_initial_window_size | int | grpc initial window size |
| -grpc_keepalive_time | duration | After a duration of this time if the client doesn't see any activity it pings the server to see if the transport is still alive. (default 10s) |
| -grpc_keepalive_timeout | duration | After having pinged for keepalive check, the client waits for a duration of Timeout and if no activity is seen even after that the connection is closed. (default 10s) |
| -grpc_key | string | key to use, requires grpc_cert, enables TLS |
| -grpc_max_connection_age | duration | Maximum age of a client connection before GoAway is sent. (default 2562047h47m16.854775807s) |
| -grpc_max_connection_age_grace | duration | Additional grace period after grpc_max_connection_age, after which connections are forcibly closed. (default 2562047h47m16.854775807s) |
| -grpc_max_message_size | int | Maximum allowed RPC message size. Larger messages will be rejected by gRPC with the error 'exceeding the max size'. (default 16777216) |
| -grpc_port | int | Port to listen on for gRPC calls |
| -grpc_prometheus | boolean | Enable gRPC monitoring with Prometheus |
| -grpc_server_initial_conn_window_size | int | grpc server initial connection window size |
| -grpc_server_initial_window_size | int | grpc server initial window size |
| -grpc_server_keepalive_enforcement_policy_min_time | duration | grpc server minimum keepalive time (default 5m0s) |
| -grpc_server_keepalive_enforcement_policy_permit_without_stream | boolean | grpc server permit client keepalive pings even when there are no active streams (RPCs) |
| -grpc_use_effective_callerid | boolean | If set, and SSL is not used, will set the immediate caller id from the effective caller id's principal. |
| -healthcheck_retry_delay | duration | health check retry delay (default 2ms) |
| -healthcheck_timeout | duration | the health check timeout period (default 1m0s) |
| -jaeger-agent-host | string | host and port to send spans to. if empty, no tracing will be done |
| -keep_logs | duration | keep logs for this long (using ctime) (zero to keep forever) |
| -keep_logs_by_mtime | duration | keep logs for this long (using mtime) (zero to keep forever) |
| -keyspaces_to_watch | value | Specifies which keyspaces this vtgate should have access to while routing queries or accessing the vschema |
| -lameduck-period | duration | keep running at least this long after SIGTERM before stopping (default 50ms) |
| -legacy_replication_lag_algorithm | boolean | use the legacy algorithm when selecting the vttablets for serving (default true) |
| -log_backtrace_at | value | when logging hits line file:N, emit a stack trace |
| -log_dir | string | If non-empty, write log files in this directory |
| -log_err_stacks | boolean | log stack traces for errors |
| -log_queries_to_file | string | Enable query logging to the specified file |
| -log_rotate_max_size | uint | size in bytes at which logs are rotated (glog.MaxSize) (default 1887436800) |
| -logtostderr | boolean | log to standard error instead of files |
| -max_memory_rows | int | Maximum number of rows that will be held in memory for intermediate results as well as the final result. (default 300000) |
| -mem-profile-rate | int | profile every n bytes allocated (default 524288) |
| -message_stream_grace_period | duration | the amount of time to give for a vttablet to resume if it ends a message stream, usually because of a reparent. (default 30s) |
| -min_number_serving_vttablets | int | the minimum number of vttablets that will be continue to be used even with low replication lag (default 2) |
| -mutex-profile-fraction | int | profile every n mutex contention events (see runtime.SetMutexProfileFraction) |
| -mysql_allow_clear_text_without_tls | boolean | If set, the server will allow the use of a clear text password over non-SSL connections. |
| -mysql_auth_server_impl | string | Which auth server implementation to use. (default "static") |
| -mysql_auth_server_static_file | string | JSON File to read the users/passwords from. |
| -mysql_auth_server_static_string | string | JSON representation of the users/passwords config. |
| -mysql_auth_static_reload_interval | duration | Ticker to reload credentials |
| -mysql_clientcert_auth_method | string | client-side authentication method to use. Supported values: mysql_clear_password, dialog. (default "mysql_clear_password") |
| -mysql_default_workload | string | Default session workload (OLTP, OLAP, DBA) (default "UNSPECIFIED") |
| -mysql_ldap_auth_config_file | string | JSON File from which to read LDAP server config. |
| -mysql_ldap_auth_config_string | string | JSON representation of LDAP server config. |
| -mysql_ldap_auth_method | string | client-side authentication method to use. Supported values: mysql_clear_password, dialog. (default "mysql_clear_password") |
| -mysql_server_bind_address | string | Binds on this address when listening to MySQL binary protocol. Useful to restrict listening to 'localhost' only for instance. |
| -mysql_server_flush_delay | duration | Delay after which buffered response will flushed to client. (default 100ms) |
| -mysql_server_port | int | If set, also listen for MySQL binary protocol connections on this port. (default -1) |
| -mysql_server_query_timeout | duration | mysql query timeout |
| -mysql_server_read_timeout | duration | connection read timeout |
| -mysql_server_require_secure_transport | boolean | Reject insecure connections but only if mysql_server_ssl_cert and mysql_server_ssl_key are provided |
| -mysql_server_socket_path | string | This option specifies the Unix socket file to use when listening for local connections. By default it will be empty and it won't listen to a unix socket |
| -mysql_server_ssl_ca | string | Path to ssl CA for mysql server plugin SSL. If specified, server will require and validate client certs. |
| -mysql_server_ssl_cert | string | Path to the ssl cert for mysql server plugin SSL |
| -mysql_server_ssl_key | string | Path to ssl key for mysql server plugin SSL |
| -mysql_server_version | string | MySQL server version to advertise. (default "5.7.9-Vitess") |
| -mysql_server_write_timeout | duration | connection write timeout |
| -mysql_slow_connect_warn_threshold | duration | Warn if it takes more than the given threshold for a mysql connection to establish |
| -mysql_tcp_version | string | Select tcp, tcp4, or tcp6 to control the socket type. (default "tcp") |
| -normalize_queries | boolean | Rewrite queries with bind vars. Turn this off if the app itself sends normalized queries with bind vars. (default true) |
| -onterm_timeout | duration | wait no more than this for OnTermSync handlers before stopping (default 10s) |
| -opentsdb_uri | string | URI of opentsdb /api/put method |
| -pid_file | string | If set, the process will write its pid to the named file, and delete it on graceful shutdown. |
| -port | int | port for the server |
| -proxy_protocol | boolean | Enable HAProxy PROXY protocol on MySQL listener socket |
| -purge_logs_interval | duration | how often try to remove old logs (default 1h0m0s) |
| -querylog-filter-tag | string | string that must be present in the query as a comment for the query to be logged, works for both vtgate and vttablet |
| -querylog-format | string | format for query logs ("text" or "json") (default "text") |
| -redact-debug-ui-queries | boolean | redact full queries and bind variables from debug UI |
| -remote_operation_timeout | duration | time to wait for a remote operation (default 30s) |
| -retry-count | int | retry count (default 2) |
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
| -stream_buffer_size | int | the number of bytes sent from vtgate for each stream call. It's recommended to keep this value in sync with vttablet's query-server-config-stream-buffer-size. (default 32768) |
| -tablet_filters | value | Specifies a comma-separated list of 'keyspace|shard_name or keyrange' values to filter the tablets to watch |
| -tablet_grpc_ca | string | the server ca to use to validate servers when connecting |
| -tablet_grpc_cert | string | the cert to use to connect |
| -tablet_grpc_key | string | the key to use to connect |
| -tablet_grpc_server_name | string | the server name to use to validate server certificate |
| -tablet_protocol | string | how to talk to the vttablets (default "grpc") |
| -tablet_refresh_interval | duration | tablet refresh interval (default 1m0s) |
| -tablet_refresh_known_tablets | boolean | tablet refresh reloads the tablet address/port map from topo in case it changes (default true) |
| -tablet_types_to_wait | string | wait till connected for specified tablet types during Gateway initialization |
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
| -topo_read_concurrency | int | concurrent topo reads (default 32) |
| -topo_zk_auth_file | string | auth to use when connecting to the zk topo server, file contents should be <scheme>:<auth>, e.g., digest:user:pass |
| -topo_zk_base_timeout | duration | zk base timeout (see zk.Connect) (default 30s) |
| -topo_zk_max_concurrency | int | maximum number of pending requests to send to a Zookeeper server. (default 64) |
| -topo_zk_tls_ca | string | the server ca to use to validate servers when connecting to the zk topo server |
| -topo_zk_tls_cert | string | the cert to use to connect to the zk topo server, requires topo_zk_tls_key, enables TLS |
| -topo_zk_tls_key | string | the key to use to connect to the zk topo server, enables TLS |
| -tracer | string | tracing service to use (default "noop") |
| -tracing-sampling-rate | float | sampling rate for the probabilistic jaeger sampler (default 0.1) |
| -transaction_mode | string | SINGLE: disallow multi-db transactions, MULTI: allow multi-db transactions with best effort commit, TWOPC: allow multi-db transactions with 2pc commit (default "MULTI") |
| -v | value | log level for V logs |
| -version | boolean | print binary version |
| -vmodule | value | comma-separated list of pattern=N settings for file-filtered logging |
| -vschema_ddl_authorized_users | string | List of users authorized to execute vschema ddl operations, or '%' to allow all users. |
| -vtctld_addr | string | address of a vtctld instance |
| -vtgate-config-terse-errors | boolean | prevent bind vars from escaping in returned errors |
| -warn_memory_rows | int | Warning threshold for in-memory results. A row count higher than this amount will cause the VtGateWarnings.ResultsExceeded counter to be incremented. (default 30000) |

