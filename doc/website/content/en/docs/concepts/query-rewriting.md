---
title: Query Rewriting
---

Vitess works hard to create an illusion of the user having a single connection to a single database. 
In reality, a single query might interact with multiple databases and may use multiple connections to the same database.
Here we'll go over what Vitess does and how it might impact you.

### Query splitting

A complicated query with a cross shard join might need to first fetch information from a tablet keeping vindex lookup tables. Then use this information to query two different shards for more data and subsequently join the incoming results into a single result that the user receives.
The queries that MySQL gets are often just pieces of the original query, and the final result will get assembled at the vtgate level.

### Connection Pooling

When a tablet talks with a MySQL to execute a query on behalf of a user, it does not use a dedicated connection per user, and instead will share the underlying connection between users. 
This means that it's not safe to store any state in the session as you can't be sure it will continue executing queries on the same connection, and you can't be sure if this connection will be used by other users later on.

### User-Defined Variables

User defined variables are kept in the session state when working with MySQL.
You can assign values to them using SET:

```sql
SET @my_user_variable = 'foobar'
```

And later there can be queries using for example SELECT:

```sql
> SELECT @my_user_variable;
+-------------------+
| @my_user_variable |
+-------------------+
| foobar            |
+-------------------+
```

If you execute these queries against a VTGate, the first `SET` query is not sent to MySQL.
Instead, it is evaluated in the VTGate, and VTGate will keep this state for you.
The second query is also not sent down. Trivial queries such as this one are actually fully executed on VTGate.

If we try a more complicated query that requires data from MySQL, VTGate will rewrite the query before sending it down.
If we were to write something like:

```sql
WHERE col = @my_user_variable
```

What MySQL will see is:

```sql
WHERE col = 'foobar'
```

This way, no session state is needed to evaluate the query in MySQL.

### Server System Variables

A user might also want to change one of the many different system variables that MySQL exposes.
Vitess handles system variables in one of four different ways:

  * *No op*. For some settings, Vitess will just silently ignore the setting. This is for system variables that don't make much sense in a sharded setting, and don't change the behaviour of MySQL in an interesting way. 
  * *Check and fail if not already set*. These are settings that should not change, but Vitess will allow SET statements that try to set the variable to whatever it already is.
  * *Not supported*. For these settings, attempting to change them will always result in an error.
  * *Vitess aware*. These are settings that change Vitess' behaviour, and are not sent down to MySQL
  * *Reserved connection*. For some settings, it makes sense to allow them to be set, but it also means that we can't use a shared connection for this user. What this means is that every connection done on this users behalf will need to first have these system variables set, and then keep the connection dedicated. Connection pooling is important for the performance of Vitess, and reserved connections can't be pooled, so this should not be the normal way to run applications on Vitess. Just make sure that the global variable is set to the same value the application will set it to, and Vitess can use connection pooling.

### Special functions

There are a few special functions that Vitess handles without delegating to MySQL.

  * `DATABASE()` - The keyspace name and the underlying database names do not have to be equal. Vitess will rewrite these calls to use the literal string for the keyspace name. (This also applies to the synonym `SCHEMA()`) 
  * `ROW_COUNT()` and `FOUND_ROWS()` - These functions returns how many rows the last query affected/returned. Since this might have been executed on a different connection, these get rewritten to use the literal value of the number of returned rows.
  * `LAST_INSERT_ID()` - Much like `FOUND_ROWS()`, we can't trust a pooled connection for these function calls, so they get rewritten before hitting MySQL.

### Reference

Here is a list of all the system variables that are handled by Vitess and how they are handled.

| *System variable*  | *Handled* |
|---|---|
|autocommit|VitessAware|
|client_found_rows|VitessAware|
|skip_query_plan_cache|VitessAware|
|tx_read_only|VitessAware|
|transaction_read_only|VitessAware|
|sql_select_limit|VitessAware|
|transaction_mode|VitessAware|
|workload|VitessAware|
|charset|VitessAware|
|names|VitessAware|
|big_tables| NoOp |
|bulk_insert_buffer_size| NoOp |
|debug| NoOp |
|default_storage_engine| NoOp |
|default_tmp_storage_engine| NoOp |
|innodb_strict_mode| NoOp |
|innodb_support_xa| NoOp |
|innodb_table_locks| NoOp |
|innodb_tmpdir| NoOp |
|join_buffer_size| NoOp |
|keep_files_on_create| NoOp |
|lc_messages| NoOp |
|long_query_time| NoOp |
|low_priority_updates| NoOp |
|max_delayed_threads| NoOp |
|max_insert_delayed_threads| NoOp |
|multi_range_count| NoOp |
|net_buffer_length| NoOp |
|new| NoOp |
|query_cache_type| NoOp |
|query_cache_wlock_invalidate| NoOp |
|query_prealloc_size| NoOp |
|sql_buffer_result| NoOp |
|transaction_alloc_block_size| NoOp |
|wait_timeout| NoOp |
|audit_log_read_buffer_size| NotSupported|
|auto_increment_increment| NotSupported|
|auto_increment_offset| NotSupported|
|binlog_direct_non_transactional_updates| NotSupported|
|binlog_row_image| NotSupported|
|binlog_rows_query_log_events| NotSupported|
|innodb_ft_enable_stopword| NotSupported|
|innodb_ft_user_stopword_table| NotSupported|
|max_points_in_geometry| NotSupported|
|max_sp_recursion_depth| NotSupported|
|myisam_repair_threads| NotSupported|
|myisam_sort_buffer_size| NotSupported|
|myisam_stats_method| NotSupported|
|ndb_allow_copying_alter_table| NotSupported|
|ndb_autoincrement_prefetch_sz| NotSupported|
|ndb_blob_read_batch_bytes| NotSupported|
|ndb_blob_write_batch_bytes| NotSupported|
|ndb_deferred_constraints| NotSupported|
|ndb_force_send| NotSupported|
|ndb_fully_replicated| NotSupported|
|ndb_index_stat_enable| NotSupported|
|ndb_index_stat_option| NotSupported|
|ndb_join_pushdown| NotSupported|
|ndb_log_bin| NotSupported|
|ndb_log_exclusive_reads| NotSupported|
|ndb_row_checksum| NotSupported|
|ndb_use_exact_count| NotSupported|
|ndb_use_transactions| NotSupported|
|ndbinfo_max_bytes| NotSupported|
|ndbinfo_max_rows| NotSupported|
|ndbinfo_show_hidden| NotSupported|
|ndbinfo_table_prefix| NotSupported|
|old_alter_table| NotSupported|
|preload_buffer_size| NotSupported|
|rbr_exec_mode| NotSupported|
|sql_log_off| NotSupported|
|thread_pool_high_priority_connection| NotSupported|
|thread_pool_prio_kickup_timer| NotSupported|
|transaction_write_set_extraction| NotSupported|
|default_week_format|ReservedConn|
|end_markers_in_json|ReservedConn|
|eq_range_index_dive_limit|ReservedConn|
|explicit_defaults_for_timestamp|ReservedConn|
|foreign_key_checks|ReservedConn|
|group_concat_max_len|ReservedConn|
|max_heap_table_size|ReservedConn|
|max_seeks_for_key|ReservedConn|
|max_tmp_tables|ReservedConn|
|min_examined_row_limit|ReservedConn|
|old_passwords|ReservedConn|
|optimizer_prune_level|ReservedConn|
|optimizer_search_depth|ReservedConn|
|optimizer_switch|ReservedConn|
|optimizer_trace|ReservedConn|
|optimizer_trace_features|ReservedConn|
|optimizer_trace_limit|ReservedConn|
|optimizer_trace_max_mem_size|ReservedConn|
|transaction_isolation|ReservedConn|
|tx_isolation|ReservedConn|
|optimizer_trace_offset|ReservedConn|
|parser_max_mem_size|ReservedConn|
|profiling|ReservedConn|
|profiling_history_size|ReservedConn|
|query_alloc_block_size|ReservedConn|
|range_alloc_block_size|ReservedConn|
|range_optimizer_max_mem_size|ReservedConn|
|read_buffer_size|ReservedConn|
|read_rnd_buffer_size|ReservedConn|
|show_create_table_verbosity|ReservedConn|
|show_old_temporals|ReservedConn|
|sort_buffer_size|ReservedConn|
|sql_big_selects|ReservedConn|
|sql_mode|ReservedConn|
|sql_notes|ReservedConn|
|sql_quote_show_create|ReservedConn|
|sql_safe_updates|ReservedConn|
|sql_warnings|ReservedConn|
|tmp_table_size|ReservedConn|
|transaction_prealloc_size|ReservedConn|
|unique_checks|ReservedConn|
|updatable_views_with_limit|ReservedConn|
|binlog_format|CheckAndIgnore|
|block_encryption_mode|CheckAndIgnore|
|character_set_client|CheckAndIgnore|
|character_set_connection|CheckAndIgnore|
|character_set_database|CheckAndIgnore|
|character_set_filesystem|CheckAndIgnore|
|character_set_results|CheckAndIgnore|
|character_set_server|CheckAndIgnore|
|collation_connection|CheckAndIgnore|
|collation_database|CheckAndIgnore|
|collation_server|CheckAndIgnore|
|completion_type|CheckAndIgnore|
|div_precision_increment|CheckAndIgnore|
|innodb_lock_wait_timeout|CheckAndIgnore|
|interactive_timeout|CheckAndIgnore|
|lc_time_names|CheckAndIgnore|
|lock_wait_timeout|CheckAndIgnore|
|max_allowed_packet|CheckAndIgnore|
|max_error_count|CheckAndIgnore|
|max_execution_time|CheckAndIgnore|
|max_join_size|CheckAndIgnore|
|max_length_for_sort_data|CheckAndIgnore|
|max_sort_length|CheckAndIgnore|
|max_user_connections|CheckAndIgnore|
|net_read_timeout|CheckAndIgnore|
|net_retry_count|CheckAndIgnore|
|net_write_timeout|CheckAndIgnore|
|session_track_gtids|CheckAndIgnore|
|session_track_schema", boolean:|CheckAndIgnore| 
|session_track_state_change", boolean:|CheckAndIgnore| 
|session_track_system_variables|CheckAndIgnore|
|session_track_transaction_info|CheckAndIgnore|
|sql_auto_is_null", boolean:|CheckAndIgnore| 
|time_zone|CheckAndIgnore|
|version_tokens_session|CheckAndIgnore|

**Related Vitess Documentation**

* [VTGate](../vtgate)
