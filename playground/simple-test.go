package main

import (
    "fmt"
    "os"
    "strconv"
)

var SiBytes = []string{"B","kB","MB","GB","TB","PB"}

func BytesHumanReadable(val int) string {
    v := val
    m := 0
    // calculate v * 2^m, v<1024
    for v>=1024 {
        if m == len(SiBytes)-1 {
            break
        }
        v = v/1024
        m++
    }

    return fmt.Sprintf("%d%s", v, SiBytes[m])
}

func main() {
    var a int
    if len(os.Args) > 1 {
        a,_ = strconv.Atoi(os.Args[1])
    } else {
        a = 0x100000
    }
    fmt.Printf("0x%X %d\n", a, a)
    fmt.Println(BytesHumanReadable(a))
}
