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

source ./env.sh

# TODO: check VTDATAROOT is set!

name="vtctld"
grpc_port=15999
cell=${CELL:-'test'}
pid_file="${VTDATAROOT}/tmp/$name.pid"
stdout_log="${VTDATAROOT}/tmp/$name.log"
stderr_log="${VTDATAROOT}/tmp/$name.err"

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

        echo "Starting $name with $TOPOLOGY_FLAGS"
        vtctld \
          $TOPOLOGY_FLAGS \
          -cell $cell \
          -web_dir $VTTOP/web/vtctld \
          -web_dir2 $VTTOP/web/vtctld2/app \
          -workflow_manager_init \
          -workflow_manager_use_election \
          -service_map 'grpc-vtctl' \
          -backup_storage_implementation file \
          -file_backup_storage_root $VTDATAROOT/backups \
          -log_dir $VTDATAROOT/tmp \
          -port $vtctld_web_port \
          -grpc_port $grpc_port \
          -pid_file $pid_file >> "$stdout_log" 2>> "$stderr_log" &

        sleep 0.1 # There is a race to write a pid file.

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

