package hpc

import (
	"io"
)

type SlurmJob struct {
	*Job
	batchCommand string
	args         []string
	out          []string
}

func (j SlurmJob) New(job *Job) (error, SlurmJob) {
	//Create Script Check for Errors
	err, Script := BuildScript(job.ScriptContents, "batch_script", job.UID, job.GID, job.OutputScriptPth)
	if err != nil {
		return err, SlurmJob{}
	}

	//Get output script paths
	var outputScriptPath string

	//Get output script paths
	outputScriptPath, err = job.mkTempFile(job, "slurm_out-*.log")
	if err != nil {
		return err, SlurmJob{}
	}

	files := []string{outputScriptPath}
	//	execArgs := []string{"-W", "-o", outputScriptPath}
	execArgs := []string{}

	//Handle Native Specs
	var Specs []string
	if len(job.NativeSpecs) != 0 {
		//Defines an array of illegal arguments which will not be passed in as native specifications
		illegalArguments := []string{"-o"}
		Specs = RemoveIllegalParams(job.NativeSpecs, illegalArguments)
	}

	if len(Specs) != 0 {
		execArgs = append(execArgs, Specs...)
	}

	execArgs = append(execArgs, Script)

	return nil, SlurmJob{job, "salloc", execArgs, files}
}

func (j *SlurmJob) RunJob() (err error, out string) {
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

	go j.Job.tailPipe(stdout)
	go j.Job.tailPipe(stderr)

	err = cmd.Wait()

	return
}
