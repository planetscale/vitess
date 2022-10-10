package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/paulbellamy/ratecounter"
	"math/rand"
	"time"
)

func PrepareTx(db *sql.DB, qry string) (tx *sql.Tx, s *sql.Stmt, e error) {
	if tx, e = db.Begin(); e != nil {
		return
	}

	if s, e = tx.Prepare(qry); e != nil {
		panic(e.Error())
	}
	return
}

type customer struct {
	id          int
	email      string
	creditCard string
}
type product struct {
	sku   string
	price int
}

var customers []*customer
var products []*product

func init() {
	customers = []*customer{
		{1, "albert@example.com", "4111 1111 1111 1111"},
		{2, "barry@gmail.com", "4242 4242 4242 4242"},
		{3, "carla@yahoo.com", "5431 1111 1111 1301"},
		{4, "daryl@email.com", "4026 1111 1111 1115"},
		{5, "eileen@example.com", "6011 3099 0000 1248"},
	}
	products = []*product{
		{"SKU-1001", 100},
		{"SKU-1002", 30},
	}
	rand.Seed(time.Now().Unix())

}

func getParams() (id int, sku string, price, qty int, email, creditCard string) {
	cust := customers[rand.Intn(len(customers))]
	pr := products[rand.Intn(len(products))]
	qty = rand.Intn(10) + 1
	return cust.id, pr.sku, pr.price, qty, cust.email, cust.creditCard
}

func main() {
	counter := ratecounter.NewRateCounter(1 * time.Second)
	done := false
	var i int64
	query := "insert into corder(customer_id, sku, price, qty, email, credit_card) values(?, ?, ?, ?, ?, ?)"
	i = 0
	db, err := sql.Open("mysql", "root@tcp(127.0.0.1:15306)/customer")
	fmt.Println("connection opened")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()
	tx, stmt, err := PrepareTx(db, query)
	if err != nil {
		panic(err.Error())
	}
	for !done {
		time.Sleep(1 * time.Millisecond)
		i++
		counter.Incr(1)
		if _, err := stmt.Exec(getParams()); err != nil {
			panic(err)
		}
		if i%1000 == 0 {
			fmt.Printf("QPS: %d\n", counter.Rate())
			if err := tx.Commit(); err != nil {
				panic(err)
			}
			tx, stmt, err = PrepareTx(db, query)
			if err != nil {
				panic(err.Error())
			}
		}
	}
}
