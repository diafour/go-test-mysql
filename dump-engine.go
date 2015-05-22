package main


import (
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

/**
 * Execute instance dumper jobs, wait until they finish.
 * Return array of DumpFeedback - rows summary for printing for each instance
 */
func runDumper(config Config) (feedbacks []DumpFeedback) {
    instanceCount := len(config.Instances)

    // array for feedbacks from dumper jobs
    feedbacks = make([]DumpFeedback, instanceCount, instanceCount)

    // make chan for receiving DumpFeedback objects
    feedbackChan := make(chan DumpFeedback)

    // Run dumper jobs
    // Each job takes instance config
    for i,_ := range config.Instances {
        go func(instance InstanceConfig, id int) {
            logger.Printf("Dump from instance '%s' (%s:%d)\n", instance.Name, instance.Host, instance.Port)
            feedback := DumpDatabase(config.ArchivePath, instance)
            feedback.Id = id
            feedbackChan<-feedback
        }(config.Instances[i], i)
    }

    // Wait until all feedbacks recieved
    feedbackCount := 0
    for {
        select {
        case dumpFeedback := <-feedbackChan:
            feedbacks[dumpFeedback.Id] = dumpFeedback
            feedbackCount++
        }
        if feedbackCount == instanceCount {
            break
        }
    }
    return
}


/**
 * instance dumper job
 * Open db, run table dumpers, wait until they done.
 * return feedback with rows count
 */
func DumpDatabase(archivePath string, instance InstanceConfig) (feedback DumpFeedback) {
    dbFileName := fmt.Sprintf("%s.db", instance.Name)
    db, err := sql.Open("sqlite3", dbFileName)
    if err != nil {
        logger.Fatal(err)
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

/**
 * USERS dumper:
 * creates  CSV->Gzip->File pipeline, opens query for users table,
 * writes columns as csv fields
 * return dumped rows count
 */
func DumpUsers(db *sql.DB, archivePath string, instance InstanceConfig) (count int) {
    logger.Println("DumpUsers")

    var csw CsvGzipWriter
    csw.NewCsvGzipWriter(prepareDumpFile(archivePath,instance.Name,"users"))
    defer func (p *CsvGzipWriter){
        logger.Println("Close CsvGzipWriter")
        p.Close()
    }(&csw)

    rows, err := db.Query("select user_id, name from users")
    if err != nil {
        logger.Fatal(err)
    }
    defer func (rows *sql.Rows) {
        logger.Println("Close rows")
        rows.Close()
    }(rows)


    for rows.Next() {
        var user_id int
        var name string
        rows.Scan(&user_id, &name)
        logger.Printf("user_id: %d, name: '%s'", user_id, name)
        csvFields := []string{fmt.Sprintf("%d",user_id), name}
        csw.Write(csvFields)
        count++
    }
    rows.Close()

    return

}

/**
 * SALES dumper:
 * creates  CSV->Gzip->File pipeline, opens query for sales table,
 * writes columns as csv fields
 * return dumped rows count
 */
func DumpSales(db *sql.DB, archivePath string, instance InstanceConfig) (count int) {
    logger.Println("DumpSales")

    var csw CsvGzipWriter
    csw.NewCsvGzipWriter(prepareDumpFile("./archive",instance.Name,"sales"))
    defer func (p *CsvGzipWriter){
        logger.Println("Close CsvGzipWriter")
        p.Close()
    }(&csw)

    rows, err := db.Query("select order_id, user_id, order_amount from sales")
    if err != nil {
        logger.Fatal(err)
    }
    defer func (rows *sql.Rows) {
        logger.Printf("Close rows sales")
        rows.Close()
    }(rows)

    for rows.Next() {
        var order_id int
        var user_id int
        var order_amount float64
        rows.Scan(&order_id, &user_id, &order_amount)
        logger.Printf("order_id: %d, user_id: %d, order_amount: '%f'", order_id, user_id, order_amount)
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
 * validate archivePath directory:
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

/**
 * Dumps for instance stored in a separate directory.
 * Create directory and return a full file name
 */
func prepareDumpFile(archivePath string, instanceName string, tableName string) string {
    dirPath := fmt.Sprintf("%s/%s", archivePath, instanceName)
    if _,err := os.Stat(dirPath); os.IsNotExist(err) {
        os.Mkdir(dirPath, 0775)
    }
    return fmt.Sprintf("%s/%s/%s", archivePath, instanceName, tableName)
}

