package hpc

import (
	"bufio"
	"os"
	"os/exec"
	"strings"
	"syscall"
)

type LSFJob struct {
	*Job
	batchCommand string
	args         []string
}

func (j *LSFJob) New(job *Job) (error, *LSFJob) {
	return nil, LSFJob{
		job,
		"bsub",
		[]string{},
	}
}

func (j *LSFJob) RunJob() (err error, out string) {
	//Create Script Check for Errors
	err, Script := BuildScript(j.ScriptContents, "batch_script", j.UID, j.GID, j.OutputScriptPth)
	if err != nil {
		return err, ""
	}

	//Create empty command var
	var cmd *exec.Cmd

	//Get output script paths
	var outputScriptPath, errorScriptPath string

	//Determin if script to be run should be done locally or through the batch system
	if j.BatchExecution == false {
		cmd = exec.Command("/bin/bash", Script)
	} else {
		var err error

		outputScriptPath, err = j.Job.mkTempFile("lsf_out-*.log")
		if err != nil {
			return err, ""
		}
		defer os.Remove(outputScriptPath)

		errorScriptPath, err = j.Job.mkTempFile("lsf_err-*.log")
		if err != nil {
			return err, ""
		}
		defer os.Remove(errorScriptPath)

		//Handle Native Specs
		var Specs []string
		if len(j.NativeSpecs) != 0 {
			//Defines an array of illegal arguments which will not be passed in as native specifications
			illegalArguments := []string{"-e", "-o", "-eo"}
			Specs = RemoveIllegalParams(j.NativeSpecs, illegalArguments)
		}

		//Assemble bash command
		batchCommand := "bsub"
		execArgs := []string{"-o", outputScriptPath, "-e", errorScriptPath}

		if j.Bank != "" {
			execArgs = append(execArgs, "-G", j.Bank)
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
	//if err := cmd.Run(); err != nil {
	//	return err, ""
	//}

	cmdResults, err := cmd.Output()
	if err != nil {
		return err, ""
	}
	j.PrintToParent(string(cmdResults))

	//Create empty output var
	var commandOut string

	//If command was run with Batch system get output
	if outputScriptPath != "" {
		err, commandOut = GetOutput(Script, outputScriptPath)
		if err != nil {
			return err, ""
		}
	}
	//Return output
	return nil, commandOut
}

func (j *LSFJob) WaitForJob() {
	return
}

//Parses lsf output file, finds the script that was just run, returns output if any when availible
func (j *LSFJob) GetOutput(scriptName string, outputFile string) (err error, output string) {
	retry := true
	var lineArray []string

	for retry {

		file, err := os.Open(outputFile)
		if err != nil {
			continue
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			lineArray = append(lineArray, scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			return err, ""
		}

		if len(lineArray) != 0 {
			retry = false
		}
	}

	file, err := os.Open(outputFile)
	if err != nil {
		return err, ""
	}

	var subLineArray []string
	var startingLine int = 0
	var endingLine int

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lineArray = append(lineArray, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return err, ""
	}

	for i, line := range lineArray {
		if strings.Contains(line, "The output (if any) follows:") {
			startingLine = i
			break
		}
	}

	for i, line := range lineArray[startingLine:] {
		if strings.Contains(line, "PS:") {
			endingLine = startingLine + i
			break
		}
	}

	for _, line := range lineArray[startingLine+34 : endingLine-1] {
		subLineArray = append(subLineArray, line)
	}

	file.Close()
	file = nil
	return nil, strings.Join(subLineArray, "\n")

}
