package hpc

import (
	"bytes"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"strings"
	"time"
)

type CobaltJob struct {
	*Job
	batchCommand string
	args         []string
	out          []string
	statusCmd    string
	jobID        string
}

func (j CobaltJob) New(job *Job) (error, CobaltJob) {
	//Create Script Check for Errors
	err, Script := BuildScript(job.ScriptContents, "batch_script", job.UID, job.GID, job.OutputScriptPth)
	if err != nil {
		log.Warn("Failed to create script")
		job.PrintToParent("Failed to create script")
		return err, CobaltJob{}
	}

	//Get output script paths
	var outputScriptPath, errorScriptPath, logScriptPath string

	outputScriptPath, err = job.mkTempFile(job, "cobalt_out-*.log")
	if err != nil {
		log.Warn("Failed to create output")
		job.PrintToParent("Failed to create output")
		return err, CobaltJob{}
	}

	errorScriptPath, err = job.mkTempFile(job, "cobalt_err-*.log")
	if err != nil {
		log.Warn("Failed to create error output")
		job.PrintToParent("Failed to create error output")
		return err, CobaltJob{}
	}

	logScriptPath, err = job.mkTempFile(job, "cobalt_debug-*.log")
	if err != nil {
		log.Warn("Failed to create debug output")
		job.PrintToParent("Failed to create debug output")
		return err, CobaltJob{}
	}

	files := []string{outputScriptPath, errorScriptPath, logScriptPath}
	execArgs := []string{"-o", outputScriptPath, "-e", errorScriptPath, "--debuglog", logScriptPath}

	//Handle Native Specs
	var Specs []string
	if len(job.NativeSpecs) != 0 {
		//Defines an array of illegal arguments which will not be passed in as native specifications
		illegalArguments := []string{"-o", "-e", "--debuglog", " "}
		Specs = RemoveIllegalParams(job.NativeSpecs, illegalArguments)
	}

	//If native specs were defined attach them to the end. Assemble bash command
	if len(Specs) != 0 {
		execArgs = append(execArgs, Specs...)
	}

	execArgs = append(execArgs, Script)

	return nil, CobaltJob{job, "qsub", execArgs, files, "qstat", ""}
}

func (j *CobaltJob) RunJob() (err error, out string) {
	cmd := j.Job.setUid(append([]string{j.batchCommand}, j.args...))

	//Handle stdout and stderr
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	//Run the command, check for errors
	err = cmd.Run()

	errStr := string(stderr.Bytes())
	if errStr != "" {
		log.Warn(errStr)
		j.PrintToParent(errStr)
	}

	if err != nil {
		log.WithFields(log.Fields{
			"args": cmd.Args,
		}).Warn("Command failed.")
		j.PrintToParent(fmt.Sprintf("Command '%s' failed.", strings.Join(cmd.Args, " ")))
		return err, ""
	}

	jobid, err := strconv.Atoi(strings.TrimSpace(string(stdout.Bytes())))
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Warn("Failed to read job ID")
		j.PrintToParent(fmt.Sprintf("Failed to read job ID: %#v", err))
		return err, ""
	}
	j.jobID = strconv.Itoa(jobid)

	log.WithFields(log.Fields{
		"jobid": jobid,
	}).Info("Waiting for job to complete.")
	j.PrintToParent(fmt.Sprintf("Waiting for job %d to complete.", jobid))

	//Build a command to check job status
	statusCmd := j.Job.setUid([]string{j.statusCmd, fmt.Sprintf("%d", jobid)})

	done := make(chan bool)
	for _, file := range j.out {
		log.WithFields(log.Fields{
			"file": file,
		}).Debug("Watching file")
		go j.Job.tailFile(file, done)
		defer os.Remove(file)
	}

	sleepTime := 10 * time.Second
	status := *statusCmd
	ret := status.Run()

	//Loop until qstat returns non-zero
	for ret == nil {
		time.Sleep(sleepTime)

		status = *statusCmd
		log.WithFields(log.Fields{
			"args": status.Args,
		}).Debug("Checking job")
		ret = status.Run()
	}
	close(done)

	log.WithFields(log.Fields{
		"jobid": jobid,
	}).Debug("Job completed.")

	return nil, ""
}

func (j *CobaltJob) KillJob() (err error) {
	//Build command to kill job with ID
	cmd := j.Job.setUid([]string{"qdel", j.jobID})
	fmt.Println(cmd)
	ret, err := cmd.Output()
	fmt.Println(string(ret))
	if err != nil {
		return fmt.Errorf("Cannot kill job.", err)
	}
	return nil
}
