package hpc

import (
	"io"
)

type SlurmJob struct {
	*Job
	batchCommand string
	args         []string
	out          []string
	statusCmd    string
}

func (j SlurmJob) New(job *Job) (error, SlurmJob) {
	//Create Script Check for Errors
	err, Script := BuildScript(job.ScriptContents, "batch_script", job.UID, job.GID, job.OutputScriptPth)
	if err != nil {
		return err, SlurmJob{}
	}

	// We need to run sacct to find out if accounting is enabled.  If it is, use
	// sbatch + sstat/squeue + sacct.  Otherwise, do the simple thing and just use salloc.
	var execArgs []string

	//Handle Native Specs
	if len(job.NativeSpecs) != 0 {
                //Defines an array of illegal arguments which will not be passed in as native specifications
                illegalArguments := []string{" "}
                Specs = RemoveIllegalParams(job.NativeSpecs, illegalArguments)
        }

	if len(job.NativeSpecs) != 0 {
		execArgs = append(execArgs, job.NativeSpecs...)
	}

	execArgs = append(execArgs, Script)

	return nil, SlurmJob{job, "sbatch", execArgs}
}

func (j *SlurmJob) RunJob() (err error, out string) {
	cmd := j.Job.setUid(append([]string{j.batchCommand}, j.args...))

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

	var jobid int

	items, err := fmt.Sscanf(string(stdout.Bytes()), "Submitted batch job %d", &jobid)
	if err != nil {
		j.PrintToParent(fmt.Sprintf("Failed to read job ID: %#v", err))
		return err, ""
	}

	log.Printf("Waiting for job %d to complete.", jobid)
	j.PrintToParent(fmt.Sprintf("Waiting for job %d to complete.", jobid))

	//Build a command to check job status
	statusCmd := j.Job.setUid([]string{j.statusCmd, fmt.Sprintf("%d", jobid)})

	done := make(chan bool)
	for _, file := range j.out {
		go j.Job.tailFile(file, done)
		defer os.Remove(file)
	}

	sleepTime := 10 * time.Second
	status := *statusCmd
	ret := status.Run()

	//Loop until sstat returns an error
	for ret == nil {
		time.Sleep(sleepTime)

		status = *statusCmd
		ret = status.Run()
	}
	close(done)

	return nil, ""
}
