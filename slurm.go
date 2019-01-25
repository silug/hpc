package hpc

import (
	"bytes"
	"fmt"
	"log"
	"strconv"
	"strings"
)

type SlurmJob struct {
	*Job
	batchCommand string
	args         []string
	out          string
	statusCmd    string
}

func (j SlurmJob) New(job *Job) (error, SlurmJob) {
	//Create Script Check for Errors
	err, Script := BuildScript(job.ScriptContents, "batch_script", job.UID, job.GID, job.OutputScriptPth)
	if err != nil {
		return err, SlurmJob{}
	}

	//Get output script paths
	outputScriptPath, err := job.mkTempFile(job, "slurm_out-*.log")
	if err != nil {
		return err, SlurmJob{}
	}

	var execArgs []string

	//Handle Native Specs
	var Specs []string
	if len(job.NativeSpecs) != 0 {
		//Defines an array of illegal arguments which will not be passed in as native specifications
		illegalArguments := []string{" "}
		Specs = RemoveIllegalParams(job.NativeSpecs, illegalArguments)
	}

	if len(job.NativeSpecs) != 0 {
		execArgs = append(execArgs, Specs...)
	}

	execArgs = append(execArgs, Script)

	return nil, SlurmJob{job, "sbatch", execArgs, outputScriptPath, "squeue"}
}

func (j *SlurmJob) RunJob() (err error, out string) {
	cmd := j.Job.setUid(append([]string{j.batchCommand}, j.args...))
	//Handle stdout and stderr
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	//Run the command, check for errors
	err = cmd.Run()

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

	jobid, err := strconv.Atoi(strings.TrimPrefix(string(stdout.Bytes()), "Submitted batch job "))
	if err != nil {
		j.PrintToParent(fmt.Sprintf("Failed to read job ID: %#v", err))
		return err, ""
	}

	fmt.Println("Job ID is ", jobid)

	return
}
