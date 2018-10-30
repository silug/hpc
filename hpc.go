package hpc

import (
	"fmt"
)

type Job struct {
	cmd string
}

func test() {
	fmt.Println("Test")
}

func (j *Job) Run() {

	fmt.Println(j.cmd)

}
