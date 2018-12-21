package hpc

func (j *Job) New(job *Job) (error, *Job) {
	return nil, job
}

func (j *Job) RunJob() (err error, out string) {
	//Create Script Check for Errors
	err, Script := BuildScript(j.ScriptContents, "batch_script", j.UID, j.GID, j.OutputScriptPth)
	if err != nil {
		return err, ""
	}

	cmd := j.setUid([]string{"/bin/bash", Script})

	//Run the command
	ret, execerr := cmd.CombinedOutput()

	//Return output
	return execerr, string(ret)
}
