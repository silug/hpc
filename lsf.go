package hpc

import (
	"io"
)

type LSFJob struct {
	*Job
	batchCommand string
	args         []string
}

func (j LSFJob) New(job *Job) (error, LSFJob) {
	//Create Script Check for Errors
	err, Script := BuildScript(j.ScriptContents, "batch_script", j.UID, j.GID, j.OutputScriptPth)
	if err != nil {
		return err, LSFJob{}
	}

	//Assemble bash command
	execArgs := []string{"-I"}

	if j.Bank != "" {
		execArgs = append(execArgs, "-G", j.Bank)
	}

	//Handle Native Specs
	var Specs []string
	if len(j.NativeSpecs) != 0 {
		//Defines an array of illegal arguments which will not be passed in as native specifications
		illegalArguments := []string{"-e", "-o", "-eo"}
		Specs = RemoveIllegalParams(j.NativeSpecs, illegalArguments)
	}

	if len(Specs) != 0 {
		execArgs = append(execArgs, Specs...)
	}

	execArgs = append(execArgs, Script)

	return nil, LSFJob{job, "bsub", execArgs}
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

	j.Job.tailPipe(stdout)
	j.Job.tailPipe(stderr)

	err = cmd.Wait()

	return
}
