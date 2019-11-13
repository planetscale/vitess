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

name="vtgate"
source ./env.sh

set -x
# TODO: check VTDATAROOT is set!

cell=${CELL:-'test'}
web_port=15001
grpc_port=15991
mysql_server_port=15306
mysql_server_socket_path="/tmp/mysql.sock"
pid_file="${VTDATAROOT}/tmp/$name.pid"
stdout_log="${VTDATAROOT}/tmp/$name.log"
stderr_log="${VTDATAROOT}/tmp/$name.err"
optional_tls_args=''
optional_auth_args='-mysql_auth_server_impl none'
optional_grpc_auth_args=''

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
        echo "Starting $name"
        vtgate \
          $TOPOLOGY_FLAGS \
          -log_dir $VTDATAROOT/tmp \
          -log_queries_to_file $VTDATAROOT/tmp/vtgate_querylog.txt \
          -port $web_port \
          -grpc_port $grpc_port \
          -mysql_server_port $mysql_server_port \
          -mysql_server_socket_path $mysql_server_socket_path \
          -cell $cell \
          -cells_to_watch $cell \
          -tablet_types_to_wait MASTER,REPLICA \
          -gateway_implementation discoverygateway \
          -service_map 'grpc-vtgateservice' \
	   $optional_auth_args \
	   $optional_grpc_auth_args \
	   $optional_tls_args \
          -pid_file $pid_file >> "$stdout_log" 2>> "$stderr_log" &

	echo "Waiting for vtgate to be up..."
	for i in `seq 1 30`; do
	 curl -I "http://127.0.0.1:$web_port/debug/status" >/dev/null 2>&1 && break
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

