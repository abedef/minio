package main

import (
	"fmt"
	"os"

	"gitlab.com/genieindex/minio"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Println("usage: store 'path/to/object' 'text to store'")
		os.Exit(1)
	}
	minio.SaveText(os.Args[1], os.Args[2])
}
