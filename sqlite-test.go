package main

import (
    "database/sql"
    "fmt"
  _ "github.com/mattn/go-sqlite3"
    "log"
    "os"
)

func main() {
    os.Remove("./foo.db")

    db, err := sql.Open("sqlite3", "./foo.db")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    sqlStmt := `
    create table users (user_id integer not null primary key, name text);
    delete from users;
    create table sales (order_id integer not null primary key, user_id integer, order_amount float);
    delete from sales;
    `
    _, err = db.Exec(sqlStmt)
    if err != nil {
        log.Printf("%q: %s\n", err, sqlStmt)
        return
    }

    tx, err := db.Begin()
    if err != nil {
        log.Fatal(err)
    }
    stmt, err := tx.Prepare("insert into users(user_id, name) values(?, ?)")
    if err != nil {
        log.Fatal(err)
    }
    defer stmt.Close()
    for i := 0; i < 100; i++ {
        _, err = stmt.Exec(i, fmt.Sprintf("sqlite_user_num_%03d", i))
        if err != nil {
            log.Fatal(err)
        }
    }
    tx.Commit()

    rows, err := db.Query("select user_id, name from users")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    for rows.Next() {
        var user_id int
        var name string
        rows.Scan(&user_id, &name)
        fmt.Println(user_id, name)
    }
    rows.Close()

/*    stmt, err = db.Prepare("select name from foo where user_id = ?")
    if err != nil {
        log.Fatal(err)
    }
    defer stmt.Close()
    var name string
    err = stmt.QueryRow("3").Scan(&name)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(name)
*/
  /*  _, err = db.Exec("delete from foo")
    if err != nil {
        log.Fatal(err)
    }

    _, err = db.Exec("insert into foo(id, name) values(1, 'foo'), (2, 'bar'), (3, 'baz')")
    if err != nil {
        log.Fatal(err)
    }

    rows, err = db.Query("select id, name from foo")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    for rows.Next() {
        var id int
        var name string
        rows.Scan(&id, &name)
        fmt.Println(id, name)
    }
*/
}
