package hpc

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

func RunSlurm(j *Job) (err error, out string) {
	//Create Script Check for Errors
	err, Script := BuildScript(j.ScriptContents, "batch_script", j.UID, j.GID, j.OutputScriptPth)
	if err != nil {
		return err, ""
	}

	//Open script and get its contents
	file, err := ioutil.ReadFile(Script)
	if err != nil {
		return err, ""
	}
	fileText := string(file)

	//Create empty command var
	var cmd *exec.Cmd

	//Determin if script to be run should be done locally or through the batch system
	if j.BatchExecution == false {
		cmd = exec.Command("/bin/bash", Script)
	} else {
		//Get output script paths
		outputScriptPath := fmt.Sprint(j.OutputScriptPth, "/slurm_out.log")
		//Hancle Native Specs
		var Specs []string
		if len(j.NativeSpecs) != 0 {
			//Defines an array of illegal arguments which will not be passed in as native specifications
			illegalArguments := []string{"-o"}
			Specs = RemoveIllegalParams(j.NativeSpecs, illegalArguments)
		}
		//If native specs were defined attach them to the end. Assemble bash command
		if j.Bank == "" {
			if len(Specs) != 0 {
				cmd = exec.Command("sbatch", append(append([]string{"-o", outputScriptPath}, Specs...), Script)...)
			} else {
				cmd = exec.Command("sbatch", "-o", outputScriptPath, Script)
			}
		} else {
			if len(Specs) != 0 {
				cmd = exec.Command("sbatch", append(append([]string{"-A", j.Bank, "-o", outputScriptPath}, Specs...), Script)...)
			} else {
				cmd = exec.Command("sbatch", "-A", j.Bank, "-o", outputScriptPath, Script)
			}

		}
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
	if !strings.Contains(fileText, "clone") {
		err, commandOut = GetOutputSlurm(fmt.Sprint(j.OutputScriptPth, "/slurm_out.log"))
		if err != nil {
			return err, ""
		}
	}
	//Return output
	return nil, commandOut
}

//Gets contents of a slurm output file and returns it when availible
func GetOutputSlurm(outputFile string) (err error, output string) {
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
			os.Remove(outputFile)
			return nil, string(file)
		}
	}
	return nil, ""
}
