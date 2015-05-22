package main

/** 
 * standard log wrapper for verbose flag
 */

import (
    "log"
)

type Logger struct {
    verbose bool
}

var logger Logger

func (l *Logger) Verbose(v bool) {
    l.verbose = v
}

func (l Logger) Println(v ...interface{}) {
    if l.verbose {
       log.Println(v...)
    }
}

func (l Logger) Printf(format string, v ...interface{}) {
    if l.verbose {
        log.Printf(format, v...)
    }
}

func (l Logger) Fatal(v ...interface{}) {
    if l.verbose {
       log.Fatal(v...)
    }
}
