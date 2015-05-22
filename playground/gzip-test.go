package main

import (
//	"fmt"
    "os"
    "compress/gzip"
    "log"
    "io"
)

type RepeatByte byte

func (r RepeatByte) Read(p []byte) (n int, err error) {
    for i:= range p {
        p[i] = byte(r)
    }
    return len(p), nil
}

func main() {
}

func SaveToArchive(r *Reader, fname string) () {
    gzipFile, err := os.Create("aaa.zip")
    if err != nil {
        log.Fatal(err)
    }
    gzipWriter := gzip.NewWriter(gzipFile)
    repeater := RepeatByte('a')
    io.CopyN(gzipWriter, repeater, 1000)
    gzipWriter.Close()
    gzipFile.Close()
}




