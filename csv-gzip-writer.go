package main

import (
    "encoding/csv"
    "compress/gzip"
    "fmt"
    "log"
    "os"
)

/**
 * Example usage:
 * 
 *  var csw CsvGzipWriter
 *  (&csw).NewCsvGzipWriter("./uuuu.csv.gz")
 *  defer csw.Close()
 *
 *  csvFields := []string{"1", "3"}
 *   csw.Write(csvFields)
 */

type CsvGzipWriter struct {
    gzFile *os.File
    gzWriter *gzip.Writer
    csvWriter *csv.Writer
    fileName string
}

func (p *CsvGzipWriter) NewCsvGzipWriter(path string) (err error) {
    p.fileName = fmt.Sprintf("%s.csv.gz", path)
    p.gzFile,err = os.Create(p.fileName)
    if err != nil {
        return fmt.Errorf("Cannot create file '%s': %s", err)
    }
    log.Printf("file '%s' created", p.fileName)
    p.gzWriter = gzip.NewWriter(p.gzFile)
    p.csvWriter = csv.NewWriter(p.gzWriter)
    return
}

func (p *CsvGzipWriter) Close() (_ error) {
    if p.csvWriter == nil {
        return fmt.Errorf("Cannot flush null csvWriter for '%s'.", p.fileName)
    }
    p.csvWriter.Flush()

    if p.gzWriter == nil {
        return fmt.Errorf("Cannot close null gzWriter for '%s'.", p.fileName)
    }
    p.gzWriter.Close()

    if p.gzFile == nil {
        return fmt.Errorf("Cannot close null gzFile for '%s'.", p.fileName)
    }
    p.gzFile.Close()
    return
}

func (p *CsvGzipWriter) Write(records []string ) (_ error) {
    if p.csvWriter == nil {
        return fmt.Errorf("Cannot write to null csvWriter for '%s'.", p.fileName)
    }
    p.csvWriter.Write(records)
    return
}

