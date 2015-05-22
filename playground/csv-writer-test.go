package main

import (
    "encoding/csv"
    "compress/gzip"
    "log"
    "fmt"
    "os"
)

func main() {
    var csw CsvGzipWriter
    (&csw).NewCsvGzipWriter("./uuuu.csv.gz")
    defer csw.Close()

    csvFields := []string{"1", "3"}
    csw.Write(csvFields)
}

type CsvGzipWriter struct {
    gzFile *os.File
    gzWriter *gzip.Writer
    csvWriter *csv.Writer
}

func (p *CsvGzipWriter) NewCsvGzipWriter(archivePath string) {
    //if os.IsExist()
    var err error
    p.gzFile,err = os.Create(archivePath)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("file created")
    p.gzWriter = gzip.NewWriter(p.gzFile)
    p.csvWriter = csv.NewWriter(p.gzWriter)
    //p.csvWriter = csv.NewWriter(p.gzFile)
    //header := []string{"h1","h2"}
    //p.csvWriter.Write(header)
}

func (p *CsvGzipWriter) Close() {
    if p.csvWriter == nil {
        fmt.Println("csvWriter is nil!")
        return
    }
    p.csvWriter.Flush()

    if p.gzWriter == nil {
        fmt.Println("gzWriter is nil!")
        return
    }
    p.gzWriter.Close()

    if p.gzFile == nil {
        fmt.Println("gzFile is nil!")
        return
    }
    p.gzFile.Close()
}

func (cgw *CsvGzipWriter) Write(records []string ) {
    (*cgw).csvWriter.Write(records)
}

