package main

import "os"

func f() int {
	defer println("lul")
	panic("hey")
	return 1
}

func main() {
	os.Exit(f())
}
