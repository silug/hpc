package hpc

import (
	log "github.com/sirupsen/logrus"
	"io"
)

type LSFJob struct {
	*Job
	batchCommand string
	args         []string
}

func (j LSFJob) New(job *Job) (error, LSFJob) {
	//Create Script Check for Errors
	err, Script := BuildScript(job.ScriptContents, "batch_script", job.UID, job.GID, job.OutputScriptPth)
	if err != nil {
		log.Warn("Failed to create script")
		job.PrintToParent("Failed to create script")
		return err, LSFJob{}
	}

	//Assemble bash command
	execArgs := []string{"-I"}

	//Handle Native Specs
	var Specs []string
	if len(job.NativeSpecs) != 0 {
		//Defines an array of illegal arguments which will not be passed in as native specifications
		illegalArguments := []string{"-e", "-o", "-eo"," "}
		Specs = RemoveIllegalParams(job.NativeSpecs, illegalArguments)
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
		log.WithFields(log.Fields{
			"error": err,
		}).Warn("Error creating stdout pipe")
		j.PrintToParent("Error creating stdout pipe")
		return
	}

	stderr, err = cmd.StderrPipe()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Warn("Error creating stderr pipe")
		j.PrintToParent("Error creating stderr pipe")
		return
	}

	err = cmd.Start()
	if err != nil {
		log.WithFields(log.Fields{
			"args":  cmd.Args,
			"error": err,
		}).Warn("Failed to start command")
		j.PrintToParent("Failed to start command")
		return
	}

	go j.Job.tailPipe(stdout)
	go j.Job.tailPipe(stderr)

	err = cmd.Wait()
	if err != nil {
		log.WithFields(log.Fields{
			"args":  cmd.Args,
			"error": err,
		}).Warn("Command failed")
		return err, ""
	}

	log.Debug("Job completed.")

	return
}
