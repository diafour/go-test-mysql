package main

import (
	"log"
	"os"
    "fmt"
    "database/sql"
  _ "github.com/mattn/go-sqlite3"
)


type DumpFeedback struct {
    Id int
    InstanceName string
    UsersCount int
    SalesCount int
}

func main() {
    ParseOptions()

    if options.help {
        printHelp()
        return
    }

    //if options.verbose {
    //    (&logger).SetVerbose(true)
    //}

    config, err := LoadConfig(options.configFile)
    if err != nil {
        exit(2, "Cannot load file, aborting: %s\n", err)
    }

    if err = validateArchivePath(config.ArchivePath); err != nil {
        exit(2, "Archive path problem: %s\n", err)
    }

    instanceCount := len(config.Instances)

    // feedbacks from dumper jobs
    feedbacks := make([]DumpFeedback, instanceCount, instanceCount)

    // make chans for dumpers
    feedbackChan := make(chan DumpFeedback)

    // 
    for i,_ := range config.Instances {
        go func(instance InstanceConfig, id int) {
            log.Printf("Dump from instance '%s' (%s:%d)\n", instance.Name, instance.Host, instance.Port)
            feedback := DumpDatabase(config.ArchivePath, instance)
            feedback.Id = id
            feedbackChan<-feedback
        }(config.Instances[i], i)
    }

    // TODO make watchdog chan, implement timeout
    feedbackCount := 0
    for {

        select {
        case dumpFeedback := <- feedbackChan:
            feedbacks[dumpFeedback.Id] = dumpFeedback
            feedbackCount++
        }
        if feedbackCount == instanceCount {
            break
        }
    }


    for i,_ := range feedbacks {
        f := feedbacks[i]
        ins := config.Instances[i]
        fmt.Printf("Instance '%s' (%s:%d):\n", ins.Name, ins.Host, ins.Port)
        fmt.Printf("    TABLE USERS %d row(s)\n", f.UsersCount)
        fmt.Printf("    TABLE SALES %d row(s)\n", f.SalesCount)
    }

}

/** 
 * path must be a path to directory
 */
func validateArchivePath(path string) (_ error) {
    fileStat,err := os.Stat(path)
    if os.IsNotExist(err) {
        return fmt.Errorf("Path '%s' is not exists. Check config parameter archive_path.", path)
    }
    if fileStat != nil && !fileStat.IsDir() {
        return fmt.Errorf("Path '%s' is not a directory. Check config parameter archive_path", path)
    }
    return
}

func prepareDumpFile(archivePath string, instanceName string, tableName string) string {
    dirPath := fmt.Sprintf("%s/%s", archivePath, instanceName)
    if _,err := os.Stat(dirPath); os.IsNotExist(err) {
        os.Mkdir(dirPath, 0775)
    }
    return fmt.Sprintf("%s/%s/%s", archivePath, instanceName, tableName)
}


func exit(stat int, msgfmt string, args ...interface{}) {
	log.Printf(msgfmt, args...)
	os.Exit(stat)
}


func DumpDatabase(archivePath string, instance InstanceConfig) (feedback DumpFeedback) {
    dbFileName := fmt.Sprintf("%s.db", instance.Name)
    db, err := sql.Open("sqlite3", dbFileName)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    stopU:=false
    stopS:=false
    chU := make(chan int)
    chS := make(chan int)

    go func() {
        count := DumpUsers(db, archivePath, instance)
        chU<-count
    }()

    go func() {
        count := DumpSales(db, archivePath, instance)
        chS<-count
    }()

    // wait for success messages from jobs
    for {
        select {
        case countU := <-chU:
            feedback.UsersCount = countU
            stopU = true
        case countS := <-chS:
            feedback.SalesCount = countS
            stopS = true
        }
        if stopU && stopS {
            break
        }
    }
    return
}

func DumpUsers(db *sql.DB, archivePath string, instance InstanceConfig) (count int) {
    log.Println("DumpUsers")

    var csw CsvGzipWriter
    csw.NewCsvGzipWriter(prepareDumpFile(archivePath,instance.Name,"users"))
    defer func (p *CsvGzipWriter){
        log.Println("Close CsvGzipWriter")
        p.Close()
    }(&csw)

    rows, err := db.Query("select user_id, name from users")
    if err != nil {
        log.Fatal(err)
    }
    defer func (rows *sql.Rows) {
        log.Println("Close rows")
        rows.Close()
    }(rows)


    for rows.Next() {
        var user_id int
        var name string
        rows.Scan(&user_id, &name)
        log.Printf("user_id: %d, name: '%s'", user_id, name)
        csvFields := []string{fmt.Sprintf("%d",user_id), name}
        csw.Write(csvFields)
        count++
    }
    rows.Close()

    return

}

func DumpSales(db *sql.DB, archivePath string, instance InstanceConfig) (count int) {
    log.Println("DumpSales")

    var csw CsvGzipWriter
    csw.NewCsvGzipWriter(prepareDumpFile("./archive",instance.Name,"sales"))
    defer func (p *CsvGzipWriter){
        log.Println("Close CsvGzipWriter")
        p.Close()
    }(&csw)

    rows, err := db.Query("select order_id, user_id, order_amount from sales")
    if err != nil {
        log.Fatal(err)
    }
    defer func (rows *sql.Rows) {
        log.Printf("Close rows sales")
        rows.Close()
    }(rows)

    for rows.Next() {
        var order_id int
        var user_id int
        var order_amount float64
        rows.Scan(&order_id, &user_id, &order_amount)
        log.Printf("order_id: %d, user_id: %d, order_amount: '%f'", order_id, user_id, order_amount)
        csvFields := []string{
        fmt.Sprintf("%d", order_id),
        fmt.Sprintf("%d",user_id),
        fmt.Sprintf("%f", order_amount)}
        csw.Write(csvFields)
        count++
    }
    rows.Close()
    return
}




/**

1 instance
2 tables:

open connection
prepare 2 statements
execute 2 SELECTs -> Need Reader for reading result. DumpReader

go routine:
pipeline: copy from DumpReader to CsvWriter to GzipWriter to FileWriter

success -> prompt for feedback, save it to descript.ion


multiinstance: run previous procedure in parallel.

1. Try to connect to all instances.
Prompt:
Couldn't connect to all instances:
ru: error
en: err
Proceed dumping anyway for only de instance? (y/n) [y] _

2. parallel preparing, execution
if one execution failed Prompt:
Couldn't execute query "SELECT .... FROM users". Error was:
SQL ERROR
Dump table sales anyway? (y/n) [y] _

3. success execution -> run a pipeline: reader of table rows to scvWriter to gzipwriter to filewriter
Success one node -> Print:
Instance ru:
  table users 1212 rows
  table sales 1231112 rows
Instance en:
  table users 121
...


do i need Queue?

Queue




*/
