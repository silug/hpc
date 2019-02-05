package hpc

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"strings"
	"time"
)

type SlurmJob struct {
	*Job
	batchCommand string
	args         []string
	out          []string
	statusCmd    string
	exitCodeCmd  string
}

func (j SlurmJob) New(job *Job) (error, SlurmJob) {
	//Create Script Check for Errors
	err, Script := BuildScript(job.ScriptContents, "batch_script", job.UID, job.GID, job.OutputScriptPth)
	if err != nil {
		log.Warn("Failed to create script")
		job.PrintToParent("Failed to create script")
		return err, SlurmJob{}
	}

	//Get output script paths
	var outputScriptPath string
	outputScriptPath, err = job.mkTempFile(job, "slurm_out-*.log")
	if err != nil {
		log.Warn("Failed to create output")
		job.PrintToParent("Failed to create output")
		return err, SlurmJob{}
	}

	execArgs := []string{"-o", outputScriptPath}

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

	files := []string{outputScriptPath}
	execArgs = append(execArgs, Script)

	return nil, SlurmJob{job, "sbatch", execArgs, files, "squeue", "sacct"}
}

func (j *SlurmJob) RunJob() (err error, out string) {
	cmd := j.Job.setUid(append([]string{j.batchCommand}, j.args...))
	stdOut, err := cmd.Output()
	j.PrintToParent(string(stdOut))
	jobid, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(string(stdOut), "Submitted batch job "), "\n"))

	//Build a command to check job status
	statusCmd := j.Job.setUid([]string{j.statusCmd, "--job", fmt.Sprintf("%d", jobid)})
	done := make(chan bool)
	for _, file := range j.out {
		log.WithFields(log.Fields{
			"file": file,
		}).Debug("Watching file")
		go j.Job.tailFile(file, done)
		defer os.Remove(file)
	}

	sleepTime := 1 * time.Second
	status := *statusCmd
	ret, err := status.Output()

	//Loop until squeue returns non-zero
	for strings.Contains(string(ret), fmt.Sprint(jobid)) {
		time.Sleep(sleepTime)

		status = *statusCmd
		log.WithFields(log.Fields{
			"args": status.Args,
		}).Debug("Checking job")
		ret, err = status.Output()
	}
	close(done)

	log.Debug("Job completed.")

	//Build a command to get exit status of the Job
	jobid_int := strconv.Itoa(jobid)
	cmd = j.Job.setUid([]string{j.exitCodeCmd, "-p", "-j", jobid_int, "--format=state,exitcode"})
	ret, err = cmd.Output()
	if err != nil {
		log.WithFields(log.Fields{
			"args":  cmd.Args,
			"error": err,
		}).Warn("Failed to get job status")
		return fmt.Errorf("Cannot get output from sacct", err), ""
	}
	lines := strings.Split(string(ret), "\n")
	jobResult := lines[1]
	jobResult = jobResult[:len(jobResult)-3]                //Trim Suffix
	jobResult = jobResult[strings.Index(jobResult, "|")+1:] //Trim Prefix
	exitCode, _ := strconv.Atoi(jobResult)
	if exitCode != 0 {
		log.WithFields(log.Fields{
			"args":  cmd.Args,
			"error": err,
		}).Warn("Command failed")
		return fmt.Errorf("Job exited with exit code %d", exitCode), ""
	}

	log.WithFields(log.Fields{
		"exitCode": exitCode,
	}).Debug("Success.")

	return
}
