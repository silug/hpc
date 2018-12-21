package hpc

import (
	"io"
	"os"
)

type SlurmJob struct {
	*Job
	batchCommand string
	args         []string
	out          []string
}

func (j SlurmJob) New(job *Job) (error, SlurmJob) {
	//Create Script Check for Errors
	err, Script := BuildScript(j.ScriptContents, "batch_script", j.UID, j.GID, j.OutputScriptPth)
	if err != nil {
		return err, SlurmJob{}
	}

	//Get output script paths
	var outputScriptPath string

	//Get output script paths
	outputScriptPath, err = j.Job.mkTempFile("slurm_out-*.log")
	if err != nil {
		return err, SlurmJob{}
	}

	files := []string{outputScriptPath}
	execArgs := []string{"-W", "-o", outputScriptPath}

	if j.Bank != "" {
		execArgs = append(execArgs, "-A", j.Bank)
	}

	//Handle Native Specs
	var Specs []string
	if len(j.NativeSpecs) != 0 {
		//Defines an array of illegal arguments which will not be passed in as native specifications
		illegalArguments := []string{"-o"}
		Specs = RemoveIllegalParams(j.NativeSpecs, illegalArguments)
	}

	if len(Specs) != 0 {
		execArgs = append(execArgs, Specs...)
	}

	execArgs = append(execArgs, Script)

	return nil, SlurmJob{job, "sbatch", execArgs, files}
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

	j.Job.tailPipe(stdout)
	j.Job.tailPipe(stderr)

	done := make(chan bool)
	for _, file := range j.out {
		go j.Job.tailFile(file, done)
		defer os.Remove(file)
	}

	err = cmd.Wait()
	close(done)

	return
}
