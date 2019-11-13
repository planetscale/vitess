#!/bin/bash
### BEGIN INIT INFO
# Provides:
# Required-Start:    $remote_fs $syslog
# Required-Stop:     $remote_fs $syslog
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Start daemon at boot time
# Description:       Enable service provided by daemon.
### END INIT INFO

name="vttablet"
source ./env.sh

# TODO: check VTDATAROOT is set!

cell=${CELL:-'test'}
keyspace=${KEYSPACE:-'test_keyspace'}
shard=${SHARD:-'0'}
uid=${uid:-'100'}

port=$[15000 + $uid]
grpc_port=$[16000 + $uid]
mysql_port=$[17000 + $uid]

printf -v alias '%s-%010d' $cell $uid
printf -v tablet_dir 'vt_%010d' $uid
printf -v tablet_logfile 'vttablet_%010d_querylog.txt' $uid

tablet_type=replica
if [[ "${uid: -1}" -gt 1 ]]; then
tablet_type=rdonly
fi
export TABLET_TYPE=$tablet_type

tablet_hostname=''
pid_file="${VTDATAROOT}/tmp/${name}_$uid.pid"
stdout_log="${VTDATAROOT}/tmp/${name}_$uid.log"
stderr_log="${VTDATAROOT}/tmp/${name}_$uid.err"
init_db_sql_file="$VTROOT/config/init_db.sql"

# Travis hostnames are too long for MySQL, so we use IP.
# Otherwise, blank hostname means the tablet auto-detects FQDN.
if [ "$TRAVIS" == true ]; then
  tablet_hostname=`hostname -i`
fi

mkdir -p $VTDATAROOT/backups

get_pid() {
    cat "$pid_file"
}

is_running() {
    [ -f "$pid_file" ] && ps -p `get_pid` > /dev/null 2>&1
}

case "$1" in
    start)
    if is_running; then
        echo "Already started"
    else

        echo "Starting vttablet for $uid"
        vttablet \
          $TOPOLOGY_FLAGS \
          -log_dir $VTDATAROOT/tmp \
          -log_queries_to_file $VTDATAROOT/tmp/$tablet_logfile \
          -tablet-path $alias \
          -tablet_hostname "$tablet_hostname" \
          -init_keyspace $keyspace \
          -init_shard $shard \
          -init_tablet_type $tablet_type \
          -health_check_interval 5s \
          -enable_semi_sync \
          -enable_replication_reporter \
          -backup_storage_implementation file \
          -file_backup_storage_root $VTDATAROOT/backups \
          -restore_from_backup \
          -port $port \
          -grpc_port $grpc_port \
          -service_map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
          -pid_file $pid_file \
          -vtctld_addr http://$hostname:$vtctld_web_port/ > $stderr_log 2>&1 &

        echo "Access tablet $alias at http://$hostname:$port/debug/status"

        # Todo, break after so many seconds.
        for i in 1 2 3 4 5 6 7 8 9 10; do
      	  curl -I "http://$hostname:$port/debug/status" >/dev/null 2>&1 && break
          echo -n "."
	  sleep 1
        done;

        if ! is_running; then
            echo "Unable to start, see $stdout_log and $stderr_log"
            exit 1
        fi
    fi
    ;;
   stop)
    if is_running; then
        echo -n "Stopping $name.."
        kill `get_pid`
        for i in 1 2 3 4 5 6 7 8 9 10
        # for i in `seq 10`
        do
            if ! is_running; then
                break
            fi

            echo -n "."
            sleep 1
        done
        echo

        if is_running; then
            echo "Not stopped; may still be shutting down or shutdown may have failed"
            exit 1
        else
            echo "Stopped"
            if [ -f "$pid_file" ]; then
                rm "$pid_file"
            fi
        fi
    else
        echo "Not running"
    fi
    ;;
    restart)
    $0 stop
    if is_running; then
        echo "Unable to stop, will not attempt to start"
        exit 1
    fi
    $0 start
    ;;
    status)
    if is_running; then
        echo "Running"
    else
        echo "Stopped"
        exit 1
    fi
    ;;
    init)
    echo "Running init procedure.."
    ;;
    *)
    echo "Usage: $0 {start|stop|restart|status}"
    exit 1
    ;;
esac

exit 0

