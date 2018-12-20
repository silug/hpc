package hpc

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"syscall"
	"time"
)

type SlurmJob struct {
	*Job
	batchCommand string
	args         []string
}

func (j SlurmJob) New(job *Job) (error, SlurmJob) {
	return nil, SlurmJob{
		job,
		"sbatch",
		[]string{},
	}
}

func (j *SlurmJob) RunJob() (err error, out string) {
	//Create Script Check for Errors
	err, Script := BuildScript(j.ScriptContents, "batch_script", j.UID, j.GID, j.OutputScriptPth)
	if err != nil {
		return err, ""
	}

	//Create empty command var
	var cmd *exec.Cmd

	//Get output script paths
	var outputScriptPath string

	//Determin if script to be run should be done locally or through the batch system
	if j.BatchExecution == false {
		cmd = exec.Command("/bin/bash", Script)
	} else {
		var err error

		//Get output script paths
		outputScriptPath, err = j.Job.mkTempFile("slurm_out-*.log")
		if err != nil {
			return err, ""
		}
		defer os.Remove(outputScriptPath)

		//Handle Native Specs
		var Specs []string
		if len(j.NativeSpecs) != 0 {
			//Defines an array of illegal arguments which will not be passed in as native specifications
			illegalArguments := []string{"-o"}
			Specs = RemoveIllegalParams(j.NativeSpecs, illegalArguments)
		}

		//Assemble bash command
		batchCommand := "sbatch"
		execArgs := []string{"-o", outputScriptPath}

		if j.Bank != "" {
			execArgs = append(execArgs, "-A", j.Bank)
		}

		if len(Specs) != 0 {
			execArgs = append(execArgs, Specs...)
		}

		execArgs = append(execArgs, Script)

		cmd = exec.Command(batchCommand, execArgs...)
	}

	//Assign setUID information and env. vars
	cmd.SysProcAttr = &syscall.SysProcAttr{}
	cmd.SysProcAttr.Credential = &syscall.Credential{Uid: uint32(j.UID), Gid: uint32(j.GID)}
	cmd.Env = append(os.Environ())

	//Handle Std out and Std err
	//var stdBuffer bytes.Buffer
	//mw := io.MultiWriter(os.Stdout, &stdBuffer)
	//cmd.Stdout = mw
	//cmd.Stderr = mw
	//Run the command, check for errors
	//j.Print := cmd.Output();
	fmt.Println("Got this far")

	cmdResults, err := cmd.Output()
	if err != nil {
		return err, ""
	}
	j.PrintToParent(string(cmdResults))

	//Create empty output var
	var commandOut string

	//If command was run with Batch system get output
	if outputScriptPath != "" {
		err, commandOut = j.GetOutput(outputScriptPath)
		if err != nil {
			return err, ""
		}
	}
	//Return output
	return nil, commandOut
}

func (j *SlurmJob) WaitForJob() {
	return
}

//Gets contents of a slurm output file and returns it when availible
func (j *SlurmJob) GetOutput(outputFile string) (err error, output string) {
	retry := true
	for retry {
		if _, err := os.Stat(outputFile); os.IsNotExist(err) {
			time.Sleep(10 * time.Millisecond)
			continue
		} else {
			time.Sleep(50 * time.Millisecond)
			file, err := ioutil.ReadFile(outputFile)
			if err != nil {
				return err, ""
			}
			return nil, string(file)
		}
	}
	return nil, ""
}
