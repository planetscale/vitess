#!/bin/bash

sleep 60

echo "running query against legacy"
mysql -vv legacy -e "SELECT * FROM t2"

echo "running query against vitess"
mysql -vv vitess -e "SELECT * FROM t1"

echo "running cross keyspace query"
mysql -vv vitess -e "SELECT t1.b FROM t1 INNER JOIN legacy.t2 ON t1.id=legacy.t2.id"

