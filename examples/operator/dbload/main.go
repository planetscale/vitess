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

var insQuery = "INSERT into users (user_id, user_data) values (?, ?)"
var selQuery = "SELECT user_data from users where user_id = ?"
var rowsPer = 1000

func main() {

	numThreads := flag.Int("num_threads", 20, "number of concurrent readers/writers")
	mode := flag.String("mode", "read", "read/write")

	flag.Parse()
	if *mode != "read" && *mode != "write" {
		os.Exit(1)
	}

	if *numThreads < 1 {
		os.Exit(1)
	}
	dsn, ok := os.LookupEnv("DATABASE_URL")
	if !ok {
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

	q, err := db.Prepare(insQuery)
	if err != nil {
		fmt.Printf("error preparing query: %v\n", insQuery)
		return err
	}
	for i := index * rowsPer; i < (index+1)*rowsPer; i++ {
		data := randData(20)
		fmt.Printf("inserting id=%v, data=%s\n", i, data)
		if _, err := q.Exec(i, data); err != nil {
			fmt.Printf("error inserting data: %v\n", err)
		}
		time.Sleep(time.Second)
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
