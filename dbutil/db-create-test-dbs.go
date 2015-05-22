package main

/**
 * Utility for creating sqlite3 databases from db.json config file.
 * Each database consists of two tables:
 * users: user_id, name
 * sales: order_id, sale_id, order_amount.
 *
 * db.json example:
 * [{"name":"db1", "users":[{"user_id":1, "name":"John"},{"user_id":2,"name":"Anna"}], "sales":[{"order_id":1,"user_id":2,"order_amount":123.45}]}]
 * This config instructs utility to create db file "db1.db", 
 * create tables `users` and `sales`, insert two rows into `users` 
 * and one row into `sales`.
 */

import (
    "database/sql"
    "fmt"
  _ "github.com/mattn/go-sqlite3"
    "log"
    "os"
    "encoding/json"
)


type DbInstance struct {
    Name string `json:name`
    Users []UserRow `json:users`
    Sales []SaleRow `json:sales`
}

type UserRow struct {
    UserId int `json:"user_id"`
    Name string `json:name`
}

type SaleRow struct {
    OrderId int `json:"order_id"`
    UserId int `json:"user_id"`
    OrderAmount float64 `json:"order_amount"`
}

func main() {
    path := "db.json"
    dbFile,err := os.Open(path)
    if err != nil {
        panic(fmt.Errorf("failed to open file '%s': '%s'", path, err))
    }

    fileStat, _ := dbFile.Stat()
    if size := fileStat.Size(); size > (0x20000) {
        panic(fmt.Errorf("file '%s' have size greater than 128k (%d)", path, size))
    }

    if fileStat.Size() == 0 {
        panic(fmt.Errorf("file '%s' is empty", path))
    }

    buffer := make([]byte, fileStat.Size())
    _, err = dbFile.Read(buffer)

    var dbConfig []DbInstance

    err = json.Unmarshal(buffer, &dbConfig)
    if err != nil {
        panic(fmt.Errorf("Failed load JSON from '%s': %s", path, err))
    }


    for i,_ := range dbConfig {
        instance := dbConfig[i]
        CreateDb(instance)
    }

    return
}

func CreateDb(instance DbInstance) {
    dbFileName := fmt.Sprintf("./%s.db", instance.Name)

    os.Remove(dbFileName)

    fmt.Printf("Create new database %s\n", dbFileName)

    db, err := sql.Open("sqlite3", dbFileName)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    sqlQuery := `
    create table users (user_id integer not null primary key, name text);
    delete from users;
    create table sales (order_id integer not null primary key, user_id integer, order_amount float);
    delete from sales;
    `
    _, err = db.Exec(sqlQuery)
    if err != nil {
        log.Printf("%q: %s\n", err, sqlQuery)
        return
    }

    tx, err := db.Begin()
    if err != nil {
        log.Fatal(err)
    }

    stmtU, err := tx.Prepare("insert into users(user_id, name) values(?, ?)")
    if err != nil {
        log.Fatal(err)
    }
    defer stmtU.Close()

    for i,_ := range instance.Users {
        user := instance.Users[i]
        fmt.Printf("user: %d, '%s'\n", user.UserId, user.Name)
        _, err = stmtU.Exec(user.UserId, user.Name)
        if err != nil {
            log.Fatal(err)
        }
    }

    stmtS, err := tx.Prepare("insert into sales(order_id, user_id, order_amount) values(?, ?, ?)")
    if err != nil {
        log.Fatal(err)
    }
    defer stmtS.Close()

    for i,_ := range instance.Sales {
        sale := instance.Sales[i]
        fmt.Printf("sale: %d, %d, %f\n",sale.OrderId, sale.UserId, sale.OrderAmount )
        _, err = stmtS.Exec(sale.OrderId, sale.UserId, sale.OrderAmount)
        if err != nil {
            log.Fatal(err)
        }
    }

    tx.Commit()
}
