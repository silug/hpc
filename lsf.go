package hpc

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

type LSFJob struct {
	*Job
	batchCommand string
	args         []string
	jobID        string
}

func (j LSFJob) New(job *Job) (error, LSFJob) {
	//Create Script Check for Errors
	err, Script := BuildScript(job.ScriptContents, "batch_script", job.UID, job.GID, job.OutputScriptPth)
	if err != nil {
		return err, LSFJob{}
	}

	//Assemble bash command
	execArgs := []string{"-I"}

	//Handle Native Specs
	var Specs []string
	if len(job.NativeSpecs) != 0 {
		//Defines an array of illegal arguments which will not be passed in as native specifications
		illegalArguments := []string{"-e", "-o", "-eo", " "}
		Specs = RemoveIllegalParams(job.NativeSpecs, illegalArguments)
	}

	if len(Specs) != 0 {
		execArgs = append(execArgs, Specs...)
	}

	execArgs = append(execArgs, Script)

	return nil, LSFJob{job, "bsub", execArgs, ""}
}

func (j *LSFJob) RunJob() (err error, out string) {
	cmd := j.Job.setUid(append([]string{j.batchCommand}, j.args...))

	var stdout, stderr io.ReadCloser

	stdout, err = cmd.StdoutPipe()
	if err != nil {
		return
	}

	stderr, err = cmd.StderrPipe()
	if err != nil {
		return
	}

	err = cmd.Start()
	if err != nil {
		return
	}

	go j.getJobID(stdout)
	go j.Job.tailPipe(stdout)
	go j.Job.tailPipe(stderr)

	err = cmd.Wait()
	if err != nil {
		return err, ""
	}

	return
}

func (j *LSFJob) getJobID(pipe io.ReadCloser) {
	scanner := bufio.NewScanner(pipe)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "Job <") {
			j.PrintToParent(line)
			line = line[:strings.Index(line, ">")] //Trim suffix
			line = line[strings.Index(line, "Job <")+5:]
			j.jobID = line
			return
		}
	}
}

func (j *LSFJob) KillJob() (err error) {
	fmt.Println("Killing LSF Job with ID", j.jobID)
	return nil
}
