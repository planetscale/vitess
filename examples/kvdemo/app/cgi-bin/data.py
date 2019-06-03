#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""This module allows you to bring up and tear down keyspaces."""

import cgi
import json
import subprocess
import sys
import threading
import time

import MySQLdb as db


def exec_query(conn, title, query, response, keyspace=None, kr=None):  # pylint: disable=missing-docstring
  cursor = conn.cursor()
  try:
    if kr:
      cursor.execute('use `%s:%s`' % (keyspace, kr))
    cursor.execute(query)
    response[title] = {
        "title": title,
        "description": cursor.description,
        "rowcount": cursor.rowcount,
        "lastrowid": cursor.lastrowid,
        "results": cursor.fetchall(),
        }
  except Exception as e:  # pylint: disable=broad-except
    response[title] = {
        "title": title,
        "error": str(e),
        }
  finally:
    cursor.close()


def capture_log(port, db, queries):  # pylint: disable=missing-docstring
  p = subprocess.Popen(
      ["curl", "-s", "-N", "http://zone1-%s-replica-0.vttablet:%d/debug/querylog" % (db, port)],
      stdout=subprocess.PIPE)
  def collect():
    for line in iter(p.stdout.readline, ""):
      query = line.split("\t")[12].strip('"')
      if not query:
        continue
      querylist = query.split(";")
      querylist = [x for x in querylist if "1 != 1" not in x]
      queries.append(db+": "+";".join(querylist))
  t = threading.Thread(target=collect)
  t.daemon = True
  t.start()
  return p


def main():
  print "Content-Type: application/json\n"
  try:
    conn = db.connect(
      host="vtgate-zone1",
      port=3306)
    rconn = db.connect(
      host="vtgate-zone1",
      port=3306)

    args = cgi.FieldStorage()
    query = args.getvalue("query")
    response = {}

    try:
      queries = []
      stats1 = capture_log(15002, "product-0", queries)
      stats2 = capture_log(15002, "customer-x-80", queries)
      stats3 = capture_log(15002, "customer-80-x", queries)
      stats4 = capture_log(15002, "merchant-x-80", queries)
      stats5 = capture_log(15002, "merchant-80-x", queries)
      time.sleep(0.25)
      if query and query != "undefined":
        exec_query(conn, "result", query, response)
    finally:
      stats1.terminate()
      stats2.terminate()
      stats3.terminate()
      stats4.terminate()
      stats5.terminate()
      time.sleep(0.25)
      response["queries"] = queries

    exec_query(
        rconn, "product",
        "select * from product", response, keyspace="product", kr="0")
    exec_query(
        rconn, "sales",
        "select * from sales", response, keyspace="product", kr="0")

    exec_query(
        rconn, "customer0",
        "select * from customer", response, keyspace="customer", kr="-80")
    exec_query(
        rconn, "customer1",
        "select * from customer", response, keyspace="customer", kr="80-")

    exec_query(
        rconn, "uorder0",
        "select * from orders", response, keyspace="customer", kr="-80")
    exec_query(
        rconn, "uorder1",
        "select * from orders", response, keyspace="customer", kr="80-")

    exec_query(
        rconn, "uproduct0",
        "select * from product", response, keyspace="customer", kr="-80")
    exec_query(
        rconn, "uproduct1",
        "select * from product", response, keyspace="customer", kr="80-")

    exec_query(
        rconn, "merchant0",
        "select * from merchant", response, keyspace="merchant", kr="-80")
    exec_query(
        rconn, "merchant1",
        "select * from merchant", response, keyspace="merchant", kr="80-")

    exec_query(
        rconn, "morder0",
        "select * from orders", response, keyspace="merchant", kr="-80")
    exec_query(
        rconn, "morder1",
        "select * from orders", response, keyspace="merchant", kr="80-")

    if response.get("error"):
      print >> sys.stderr, response["error"]
    print json.dumps(response)
  except Exception as e:  # pylint: disable=broad-except
    print >> sys.stderr, str(e)
    print json.dumps({"error": str(e)})

  conn.close()
  rconn.close()


if __name__ == "__main__":
  main()
