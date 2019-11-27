#!/bin/bash

source ./dev.env

pgrep -f -l '(vtdataroot|VTDATAROOT)' # list Vitess processes
pkill -f '(vtdataroot|VTDATAROOT)' # kill Vitess processes

export GOBIN=/home/morgo/vitess/bin
export PATH=~/vitess/bin:$PATH

for process in `pgrep -f '(vtdataroot|VTDATAROOT)'`; do 
 kill -9 $process
done;
killall -9 vttablet

make build

rm -rf /tmp/vtdataroot
set -xe

# Step 1: Load external MySQL
~/sandboxes/rsandbox_5_7_26/clear_all
~/sandboxes/rsandbox_5_7_26/restart_all

# dbdeployer deploy replication --gtid --repl-crash-safe 5.7.26
~/sandboxes/rsandbox_5_7_26/m -uroot -e 'create user vt_dba; grant all on *.* to vt_dba' || echo 'user probably existed'
~/sandboxes/rsandbox_5_7_26/m -uroot -e 'create user vt_repl; grant all on *.* to vt_repl' || echo 'user probably existed'

~/sandboxes/rsandbox_5_7_26/m -e 'drop database if exists _vt; create database _vt'
~/sandboxes/rsandbox_5_7_26/m _vt < examples/local/_vt.sql

~/sandboxes/rsandbox_5_7_26/m -e 'drop database if exists vt_legacy'
~/sandboxes/rsandbox_5_7_26/m -e 'create database vt_legacy DEFAULT CHARACTER SET utf8' 
~/sandboxes/rsandbox_5_7_26/m vt_legacy -e 'create table t1 (id int not null primary key auto_increment, b varchar(255))'
~/sandboxes/rsandbox_5_7_26/m vt_legacy -e 'create table t2 (id int not null primary key auto_increment, b varchar(255))'

~/sandboxes/rsandbox_5_7_26/m vt_legacy -e "INSERT INTO t1 (b) VALUES ('aaa')";
~/sandboxes/rsandbox_5_7_26/m vt_legacy -e "INSERT INTO t1 (b) VALUES ('bbbb')"; 
~/sandboxes/rsandbox_5_7_26/m vt_legacy -e "INSERT INTO t2 (b) VALUES ('ccc')";

cd examples/local

./kubecon-step1.sh
./kubecon-step2.sh
./kubecon-step3.sh
./kubecon-step4.sh

