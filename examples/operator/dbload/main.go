package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var insCustQuery = "INSERT into customer (email) values (?)"
var insOrdQuery = "INSERT into corder (customer_id, sku, price) values (?, ?, ?, ?)"
var selQuery = "select price from corder where order_id=?"
var rowsPer = 50000

func main() {

	numThreads := flag.Int("num_threads", 20, "number of concurrent readers/writers")
	mode := flag.String("mode", "read", "read/write")

	flag.Parse()
	if *mode != "read" && *mode != "write" {
		fmt.Printf("Unknown mode: %v\n", *mode)
		os.Exit(1)
	}

	if *numThreads < 1 {
		fmt.Println("numThreads must be >= 1")
		os.Exit(1)
	}
	dsn, ok := os.LookupEnv("DATABASE_URL")
	if !ok {
		fmt.Println("DATABASE_URL not specified")
		os.Exit(2)
	}

	for i := 0; i < *numThreads; i++ {
		switch *mode {
		case "write":
			fmt.Printf("launching insert loop %d\n", i)
			go insertLoop(dsn, i)
		case "read":
			fmt.Printf("launching select loop %d\n", i)
			go selectLoop(dsn, i)
		}
	}

	done := make(chan struct{})

	go func() {
		c := make(chan os.Signal, 1) // we need to reserve to buffer size 1, so the notifier are not blocked
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)

		<-c
		close(done)
	}()

	<-done
	fmt.Println("i'm done")
}

func insertLoop(dsn string, index int) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Printf("error connecting to db: %v\n", dsn)
		return err
	}

	if err := db.Ping(); err != nil {
		return err
	}

	// first insert a customer record
	q, err := db.Prepare(insCustQuery)
	if err != nil {
		fmt.Printf("error preparing query: %v\n", insCustQuery)
		return err
	}
	email := randData(10) + "@mydomain.com"
	if _, err := q.Exec(email); err != nil {
		fmt.Printf("error inserting data: %v\n", err)
		return err
	}
	// allow enough time for all threads to finish inserting 1 customer
	time.Sleep(time.Second)

	q, err = db.Prepare(insOrdQuery)
	if err != nil {
		fmt.Printf("error preparing query: %v\n", insOrdQuery)
		return err
	}
	for i := index * rowsPer; i < (index+1)*rowsPer; i++ {
		// generate a random id between 1 & 20
		// TODO: this should be same as numThreads
		customerId := rand.Int31n(20) + 1
		fmt.Printf("inserting new order for customer_id=%v\n", customerId)
		// sku = random 10-char string
		// price = random number between 100 & 1100
		if _, err := q.Exec(customerId, randData(10), rand.Int31n(1000)+100); err != nil {
			fmt.Printf("error inserting data: %v\n", err)
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

func selectLoop(dsn string, index int) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Printf("error connecting to db: %v\n", dsn)
		return err
	}

	if err := db.Ping(); err != nil {
		return err
	}

	q, err := db.Prepare(selQuery)
	if err != nil {
		fmt.Printf("error preparing query: %v\n", selQuery)
		return err
	}
	for {
		// generate random id in range
		id := index*rowsPer + rand.Intn(rowsPer)
		fmt.Printf("selecting id=%v\n", id)
		if _, err := q.Exec(id); err != nil {
			fmt.Printf("error selecting data: %v", err)
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randData(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}
