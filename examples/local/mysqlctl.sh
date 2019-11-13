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

name="mysqlctl"
source ./env.sh

# TODO: check VTDATAROOT is set!

uid=${uid:-'100'}
mysql_port=$[17000 + $uid]

printf -v tablet_dir 'vt_%010d' $uid

stdout_log="${VTDATAROOT}/tmp/${name}_$uid.log"
stderr_log="${VTDATAROOT}/tmp/${name}_$uid.err"
init_db_sql_file="$VTROOT/config/init_db.sql"
startup_timeout=30

mkdir -p $VTDATAROOT/backups

is_running() {
  mysql --no-defaults -h 127.0.0.1 -uvt_repl --port=$mysql_port -e 'select 1' >/dev/null 2>&1
}

case "$1" in
    start)
    if is_running; then
        echo "Already started"
    else

        echo "Starting mysqlctl for $uid"
        action="init -init_db_sql_file $init_db_sql_file"
        if [ -d $VTDATAROOT/$tablet_dir ]; then
          echo "Resuming from existing vttablet dir:"
          echo "    $VTDATAROOT/$tablet_dir"
          action='start'
	fi

        mysqlctl \
         -log_dir $VTDATAROOT/tmp \
         -tablet_uid $uid \
         -mysql_port $mysql_port \
          $action >> "$stdout_log" 2>> "$stderr_log"

	echo "Waiting for MySQL to start"
        for i in `seq 1 $startup_timeout`; do
          is_running && break
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
	echo "TODO: kill mysqld"
	for i in 1 2 3 4 5 6 7 8 9 10
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

