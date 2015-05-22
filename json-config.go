package main

import (
    "encoding/json"
    "fmt"
    "os"
    "flag"
)

/**
 * declate command line options and loader for json config
 */

func printHelp() {
    flag.PrintDefaults()
}

var options = &struct {
    configFile   string
    help         bool
    verbose      bool
}{
    configFile: "config.json",
    help: false,
    verbose: false,
}

func init() {
    flag.StringVar(&options.configFile, "config", options.configFile, "path to configuraion file")
    flag.BoolVar(&options.help, "help", options.help, "print help and usage")
    flag.BoolVar(&options.help, "h", options.help, "print help and usage")
    flag.BoolVar(&options.verbose, "verbose", options.verbose, "print help and usage")
    flag.BoolVar(&options.verbose, "v", options.verbose, "print help and usage")
}

func ParseOptions() {
    flag.Parse()
}

type Config struct {
    ArchivePath string `json:"archive_path"`
    CommonInstance InstanceConfig `json:common_instance`
    Instances []InstanceConfig `json:instances`
}

type InstanceConfig struct {
    Name string `json:name`
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


// Open json file and load to struct Config
// config size must be greater than 0 bytes and less than 128kB
func LoadConfig(path string) (config Config, err error) {
    configFile, err := os.Open(path)
    if err != nil {
        err = fmt.Errorf("failed to open config file '%s': '%s'", path, err);
        return
    }

    fileStat,_ := configFile.Stat()
    if size := fileStat.Size(); size > 0x20000 {
        err = fmt.Errorf("config file '%s' have size greater than 128k (%d)", path, size)
        return
    }

    if fileStat.Size() == 0 {
        err = fmt.Errorf("config file '%s' is empty", path)
        return
    }

    buffer := make([]byte, fileStat.Size())
    _,err = configFile.Read(buffer)

    err = json.Unmarshal(buffer, &config)
    if err != nil {
        err = fmt.Errorf("Failed load JSON from '%s': %s", path, err)
        return
    }

    // merge common instance config to instances
    MergeCommonInstance(config)
    return
}

func MergeCommonInstance(config Config) {
    common := config.CommonInstance
    for i := range config.Instances {
        instance := config.Instances[i]
        if instance.Host == "" && common.Host != "" {
            instance.Host = common.Host
        }
        if instance.Port == 0 && common.Port > 0 && common.Port < 65536 {
            instance.Port = common.Port
        }
        if instance.Db == "" && common.Db != "" {
            instance.Db = common.Db
        }
        if instance.User == "" && common.User != "" {
            instance.User = common.User
        }
        if instance.Password == "" && common.Password != "" {
            instance.Password = common.Password
        }
        if instance.Tables == nil {
            instance.Tables = common.Tables
        }
    }
}
