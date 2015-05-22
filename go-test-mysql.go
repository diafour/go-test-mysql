package main

import (
	"os"
    "fmt"
    "log"
)


func main() {
    ParseOptions()

    if options.help {
        printHelp()
        return
    }

    if options.verbose {
        (&logger).Verbose(true)
    }

    config, err := LoadConfig(options.configFile)
    if err != nil {
        exit(2, "Cannot load config file '%s', aborting: %s\n", options.configFile, err)
    }

    if err = validateArchivePath(config.ArchivePath); err != nil {
        exit(2, "Archive path problem: %s\n", err)
    }

    feedbacks := runDumper(config)
    // Print out dump result summary
    for i,_ := range feedbacks {
        f := feedbacks[i]
        ins := config.Instances[i]
        fmt.Printf("Instance '%s' (%s:%d):\n", ins.Name, ins.Host, ins.Port)
        fmt.Printf("    TABLE USERS %d row(s)\n", f.UsersCount)
        fmt.Printf("    TABLE SALES %d row(s)\n", f.SalesCount)
    }


}


func exit(stat int, msgfmt string, args ...interface{}) {
	log.Printf(msgfmt, args...)
	os.Exit(stat)
}
