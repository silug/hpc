package hpc

import (
	"os"
	"os/exec"
	"syscall"
)

func (j *Job) New(job *Job) (error, *Job) {
	return nil, job
}

func (j *Job) RunJob() (err error, out string) {
	//Create Script Check for Errors
	err, Script := BuildScript(j.ScriptContents, "batch_script", j.UID, j.GID, j.OutputScriptPth)
	if err != nil {
		return err, ""
	}

	//Create empty command var
	var cmd *exec.Cmd

	cmd = exec.Command("/bin/bash", Script)

	//Assign setUID information and env. vars
	cmd.SysProcAttr = &syscall.SysProcAttr{}
	cmd.SysProcAttr.Credential = &syscall.Credential{Uid: uint32(j.UID), Gid: uint32(j.GID)}
	cmd.Env = append(os.Environ())

	//Run the command
	ret, execerr := cmd.CombinedOutput()

	//Return output
	return execerr, string(ret)
}

func (j *Job) WaitForJob() {
	return
}

//Gets contents of a cobalt output file and returns it when available
func (j *Job) GetOutput() (err error, output string) {
	return
}
