/*
Copyright 2020 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vttest"
)

var cluster *vttest.LocalCluster
var querylog <-chan string

func main() {
	flag.Parse()

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.Dir("./")))
	mux.HandleFunc("/exec", exec)
	go http.ListenAndServe(":8000", mux)

	wait()
}

func wait() {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
}

func exec(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(true)
	response := make(map[string]interface{})
	defer func() {
		if response["error"] != nil {
			log.Errorf("error: %v", response["error"])
		}
		enc.Encode(response)
	}()

	var err error
	var query string
	switch {
	case req.FormValue("product") != "":
		sku := req.FormValue("sku")
		desc := req.FormValue("desc")
		price := req.FormValue("price")
		if sku == "" || desc == "" || price == "" {
			err = errors.New("sku, desc or price not specified")
			break
		}
		query = fmt.Sprintf("insert into product(sku, description, price) values('%s', '%s', %s) on duplicate key update sku=values(sku), description=values(description), price=values(price)", sku, desc, price)
	case req.FormValue("customer") != "":
		name := req.FormValue("name")
		if name == "" {
			err = errors.New("name not specified")
			break
		}
		query = fmt.Sprintf("insert into customer(email) values('%s')", name)
	case req.FormValue("order") != "":
		cid := req.FormValue("cid")
		sku := req.FormValue("sku")
		if cid == "" || sku == "" {
			err = errors.New("cid or sku not specified")
			break
		}
		query = fmt.Sprintf("insert into corder(customer_id, sku, price) values(%s, '%s', 10)", cid, sku)
	}
	if err != nil {
		response["error"] = err.Error()
		return
	}
	response["query"] = query

	cp := &mysql.ConnParams{
		Host: "127.0.0.1",
		Port: 15306,
	}
	conn, err := mysql.Connect(context.Background(), cp)
	if err != nil {
		response["error"] = err.Error()
		return
	}
	defer conn.Close()

	var queries []string
	// Clear existing log.
	for {
		select {
		case <-querylog:
			continue
		default:
		}
		break
	}
	execQuery(conn, "result", query, response)
	// Collect
	time.Sleep(250 * time.Millisecond)
	for {
		select {
		case val := <-querylog:
			queries = append(queries, val)
			continue
		default:
		}
		break
	}

	execQuery(conn, "product", "select * from product order by sku desc limit 20", response)
	execQuery(conn, "customer", "select customer_id, email from customer order by customer_id desc limit 20", response)
	execQuery(conn, "corder", "select order_id, customer_id, sku, price from corder order by order_id desc limit 20", response)
}

func execQuery(conn *mysql.Conn, title, query string, response map[string]interface{}) {
	if query == "" || query == "undefined" {
		return
	}
	qr, err := conn.ExecuteFetch(query, 10000, true)
	if err != nil {
		response["error"] = err.Error()
		return
	}
	response[title] = resultToMap(title, qr)
}

func resultToMap(title string, qr *sqltypes.Result) map[string]interface{} {
	fields := make([]string, 0, len(qr.Fields))
	for _, field := range qr.Fields {
		fields = append(fields, field.Name)
	}
	rows := make([][]string, 0, len(qr.Rows))
	for _, row := range qr.Rows {
		srow := make([]string, 0, len(row))
		for _, value := range row {
			srow = append(srow, value.ToString())
		}
		rows = append(rows, srow)
	}
	return map[string]interface{}{
		"title":        title,
		"fields":       fields,
		"rows":         rows,
		"rowsaffected": int64(qr.RowsAffected),
		"insertid":     int64(qr.InsertID),
	}
}
