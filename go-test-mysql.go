package main

import (
	"log"
	"os"
)

func main() {
    exit(0, "Could not use -config of '%s': %s", "quiet", "null")
}


func exit(stat int, msgfmt string, args ...interface{}) {
	log.Printf(msgfmt, args...)
	os.Exit(stat)
}
