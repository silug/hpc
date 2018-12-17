package hpc

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func RunCobalt(j *Job) (err error, out string) {
	//Usage: cqsub [-d] [-v] -p <project> -q <queue> -C <working directory>
	//         --dependencies <jobid1>:<jobid2> --preemptable
	//         -e envvar1=value1:envvar2=value2 -k <kernel profile>
	//         -K <kernel options> -O <outputprefix> -t time <in minutes>
	//         -E <error file path> -o <output file path> -i <input file path>
	//         -n <number of nodes> -h -c <processor count> -m <mode co/vn>
	//         -u <umask> --debuglog <cobaltlog file path>
	//         --attrs <attr1=val1:attr2=val2> --run-users <user1>:<user2>
	//         --run-project <command> <args>

	//Create Script Check for Errors
	err, Script := BuildScript(j.ScriptContents, "batch_script", j.UID, j.GID, j.OutputScriptPth)
	if err != nil {
		return err, ""
	}

	//Create empty command var
	var cmd *exec.Cmd

	//Determine if script to be run should be done locally or through the batch system
	if j.BatchExecution == false {
		cmd = exec.Command("/bin/bash", Script)

		//Assign setUID information and env. vars
		cmd.SysProcAttr = &syscall.SysProcAttr{}
		cmd.SysProcAttr.Credential = &syscall.Credential{Uid: uint32(j.UID), Gid: uint32(j.GID)}
		cmd.Env = append(os.Environ())

		//Handle Std out and Std err
		var stdBuffer bytes.Buffer
		mw := io.MultiWriter(os.Stdout, &stdBuffer)
		cmd.Stdout = mw
		cmd.Stderr = mw
		//Run the command, check for errors
		if err := cmd.Run(); err != nil {
			return err, ""
		}

		//Create empty output var
		var commandOut string

		//Return output
		return nil, commandOut
	} else {
		//Get output script paths
		var outputScriptPath, errorScriptPath, logScriptPath string
		var err error

		outputScriptPath, err = j.mkTempFile("cobalt_out-*.log")
		if err != nil {
			return err, ""
		}
		defer os.Remove(outputScriptPath)

		errorScriptPath, err = j.mkTempFile("cobalt_err-*.log")
		if err != nil {
			return err, ""
		}
		defer os.Remove(errorScriptPath)

		logScriptPath, err = j.mkTempFile("cobalt_debug-*.log")
		if err != nil {
			return err, ""
		}
		defer os.Remove(logScriptPath)

		//Handle Native Specs
		var Specs []string
		if len(j.NativeSpecs) != 0 {
			//Defines an array of illegal arguments which will not be passed in as native specifications
			illegalArguments := []string{"-E", "-o", "--debuglog"}
			Specs = RemoveIllegalParams(j.NativeSpecs, illegalArguments)
		}
		//If native specs were defined attach them to the end. Assemble bash command
		batchCommand := "cqsub"
		execArgs := []string{"-o", outputScriptPath, "-E", errorScriptPath, "--debuglog", logScriptPath}

		if j.Bank != "" {
			// Note: -p <project> may not map to "Bank"
			execArgs = append(execArgs, "-p", j.Bank)
		}

		if len(Specs) != 0 {
			execArgs = append(execArgs, Specs...)
		}

		execArgs = append(execArgs, Script)

		cmd = exec.Command(batchCommand, execArgs...)

		//Assign setUID information and env. vars
		cmd.SysProcAttr = &syscall.SysProcAttr{}
		cmd.SysProcAttr.Credential = &syscall.Credential{Uid: uint32(j.UID), Gid: uint32(j.GID)}
		cmd.Env = append(os.Environ())

		//Handle stdout and stderr
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		//Run the command, check for errors
		err = cmd.Run()

		errStr := string(stderr.Bytes())
		if errStr != "" {
			errMsg := fmt.Sprintf("Command '%s' failed.", strings.Join(cmd.Args, " "))
			j.PrintToParent(errMsg)
			j.PrintToParent(errStr)
			log.Printf(errMsg)
			log.Print(errStr)
		}

		if err != nil {
			return err, ""
		}

		jobid, err := strconv.Atoi(strings.TrimSpace(string(stdout.Bytes())))
		if err != nil {
			j.PrintToParent(fmt.Sprintf("Failed to read job ID: %#v", err))
			return err, ""
		}

		log.Printf("Waiting for job %d to complete.", jobid)
		j.PrintToParent(fmt.Sprintf("Waiting for job %d to complete.", jobid))

		//Create empty output var
		var commandOut string

		//Build a command to check job status
		var status *exec.Cmd

		status = exec.Command("cqstat", fmt.Sprintf("%d", jobid))
		//Assign setUID information and env. vars
		status.SysProcAttr = &syscall.SysProcAttr{}
		status.SysProcAttr.Credential = &syscall.Credential{Uid: uint32(j.UID), Gid: uint32(j.GID)}
		status.Env = append(os.Environ())

		//Get output
		err, commandOut = j.GetOutputCobalt(status, outputScriptPath, errorScriptPath, logScriptPath)
		if err != nil {
			return err, ""
		}

		//Return output
		return nil, commandOut
	}
}

//Gets contents of a cobalt output file and returns it when available
func (j *Job) GetOutputCobalt(statusCmd *exec.Cmd, outputFile string, errorFile string, logFile string) (err error, output string) {
	sleepTime := 10 * time.Second

	done := make(chan bool)
	go j.tailFile(logFile, done)

	cmd := *statusCmd
	ret := cmd.Run()

	//Loop until cqstat returns non-zero
	for ret == nil {
		time.Sleep(sleepTime)

		cmd = *statusCmd
		ret = cmd.Run()
	}
	close(done)

	buf := []byte("\nJob stdout:\n")

	file, err := ioutil.ReadFile(outputFile)
	if err != nil {
		return err, ""
	}
	buf = append(buf, file...)

	buf = append(buf, []byte("\nJob stderr:\n")...)

	file, err = ioutil.ReadFile(errorFile)
	if err != nil {
		return err, ""
	}
	buf = append(buf, file...)

	//Return the debug log contents
	return nil, string(buf)
}
