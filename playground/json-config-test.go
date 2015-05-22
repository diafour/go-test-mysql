package main

import (
    "encoding/json"
    "fmt"
    "os"
    "flag"
    "log"
)

func printHelp() {
    flag.PrintDefaults()
}

var options = &struct {
    configFile   string
    help         bool
}{
    configFile: "config.json",
    help: false,
}

func init() {
    flag.StringVar(&options.configFile, "config", options.configFile, "path to configuraion file")
    flag.BoolVar(&options.help, "help", options.help, "print help and usage")

}

type Config struct {
    Archive ArchiveConfig `json:archive`
    CommonInstance []InstanceConfig `json:common_instance`
    Instances []InstanceConfig `json:instances`
}

type ArchiveConfig struct {
    Path string `json:path`
}

type InstanceConfig struct {
    Host string `json:host`
    Port int64 `json:port`
    Db string `json:db`
    User string `json:user`
    Password string `json:password`
    Tables []TableConfig `json:tables`
}

type TableConfig struct {
    Table string `json:table`
    Columns []ColumnConfig
}

type ColumnConfig struct {
    Column string `json:column`
    Type string `json:type`
}


func main() {
    flag.Parse()

    if options.help {
        printHelp()
        return
    }

    var config Config
    var err error

    config, err = LoadConfig(options.configFile)
    if err != nil {
        log.Printf("Cannot load file, aborting: %s\n", err)
        os.Exit(2)
    }

    for i, _ := range config.Instances {
        log.Printf("instance host: %s\n", config.Instances[i].Host)
    }

}

// Open json file and load to struct Config
func LoadConfig(path string) (config Config, err error) {
    configFile, err := os.Open(path)
    if err != nil {
        return config,fmt.Errorf("Failed to open config file '%s': '%s'", path, err);
    }

    fileStat, _ := configFile.Stat()
    if size := fileStat.Size(); size > (0x100000) {
        return config,fmt.Errorf("config file '%s' have size greater than 128k (%d)\n", path, size)
    }

    if fileStat.Size() == 0 {
        return config,fmt.Errorf("config file '%s' is empty", path)
    }

    buffer := make([]byte, fileStat.Size())
    _, err = configFile.Read(buffer)

    err = json.Unmarshal(buffer, &config)
    if err != nil {
        return config,fmt.Errorf("Failed load JSON from '%s': %s\n", path, err)
    }

    // merge common instance config to instances
    return
}
