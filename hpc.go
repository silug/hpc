package hpc

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Job struct {
	ScriptContents  string
	NativeSpecs     []string
	Bank            string
	UID             int
	GID             int
	OutputScriptPth string
	BatchExecution  bool
}

//This function exists to help the native spec system
func Contains(illegalArgs []string, elementToTest string) bool {
	for _, illegalArg := range illegalArgs {
		if elementToTest == illegalArg {
			return true
		}
		if strings.Contains(elementToTest, illegalArg) {
			return true
		}
	}
	return false
}

//Removes specified arguments
func RemoveIllegalParams(input []string, illegalParams []string) []string {
	skip := false
	var output []string
	for i, parameter := range input {
		if skip {
			skip = false
			continue
		}

		if Contains(illegalParams, parameter) {
			if !(i+1 > len(input)-1) {
				if strings.HasPrefix(input[i+1], "-") {
					skip = false
					continue
				}
			}

			skip = true
			continue
		}
		output = append(output, parameter)

	}
	return output
}

//Creates a script returns the absolute path
func BuildScript(cmd, filenameSuffix string, myUid, myGid int, pth string) (err error, scriptPath string) {
	uniqueID := strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10)
	time.Sleep(50 * time.Millisecond)

	os.MkdirAll(fmt.Sprint(pth, "/scripts"), 0740)
	os.Chown(fmt.Sprint(pth, "/scripts"), myUid, myGid)

	batchScriptFull := pth + "/scripts/" + filenameSuffix + uniqueID + ".bash"
	batchScript, scriptErr := os.Create(batchScriptFull)
	if scriptErr != nil {
		return fmt.Errorf("Unable to create Script file: %v", scriptErr), ""
	}
	fmt.Fprintf(batchScript, "#!/bin/bash\n")
	fmt.Fprintf(batchScript, cmd)

	chmodErr := os.Chmod(batchScriptFull, 0750)
	chownErr := os.Chown(batchScriptFull, myUid, myGid)

	if chmodErr != nil || chownErr != nil {
		return fmt.Errorf("Could not set the script to be executable: %v", chmodErr), ""
	}
	batchScript.Close()

	return nil, batchScriptFull

}

//Finds which batch system is installed on the system returns a specific number
func DetectBatchSystem() (num int) {
	osEnv := os.Environ()
	counter := 0
	for _, entry := range osEnv {
		if strings.Contains(entry, "LSF_BINDIR") {
			counter = counter + 1
		}
	}

	_, err := exec.LookPath("sbatch")
	if err == nil {
		counter = counter + 3
	}

	//if COBALT
	//	counter = counter + 6

	return counter
	//1 = LSF
	//3 = Slurm
	//6 = Cobalt
	//4 = LSF + Slurm
	//10 = LSF + Slurm + Cobalt
	//9 = Slurm + Cobalt

}

//Initial func run. Gets batch system and calls the corresponding run
func (j *Job) Run() (err error, out string) {
	switch DetectBatchSystem() {
	case 1:
		return RunLSF(j)
	case 3:
		return RunSlurm(j)
	default:
		return errors.New("Batch System Detection Error"), ""
	}
	return nil, ""
}

func RunLSF(j *Job) (err error, out string) {
	//Create Script Check for Errors
	err, Script := BuildScript(j.ScriptContents, "batch_script", j.UID, j.GID, j.OutputScriptPth)
	if err != nil {
		return err, ""
	}

	//Create empty command var
	var cmd *exec.Cmd

	//Determin if script to be run should be done locally or through the batch system
	if j.BatchExecution == false {
		cmd = exec.Command("/bin/bash", Script)
	} else {
		//Get output script paths
		outputScriptPath := fmt.Sprint(j.OutputScriptPth, "/lsf_out.log")
		errorScriptPath := fmt.Sprint(j.OutputScriptPth, "/lsf_err.log")
		//Hancle Native Specs
		var Specs []string
		if len(j.NativeSpecs) != 0 {
			//Defines an array of illegal arguments which will not be passed in as native specifications
			illegalArguments := []string{"-e", "-o", "-eo"}
			Specs = RemoveIllegalParams(j.NativeSpecs, illegalArguments)
		}

		//Assemle bash command
		if j.Bank == "" {
			cmd = exec.Command("bsub", append(append([]string{"-o", outputScriptPath, "-e", errorScriptPath}, Specs...), Script)...)
		} else {
			cmd = exec.Command("bsub", append(append([]string{"-G", j.Bank, "-o", outputScriptPath, "-e", errorScriptPath}, Specs...), Script)...)
		}
	}
	//Assign setUID information and env. vars
	cmd.SysProcAttr = &syscall.SysProcAttr{}
	cmd.SysProcAttr.Credential = &syscall.Credential{Uid: uint32(j.UID), Gid: uint32(j.GID)}
	cmd.Env = append(os.Environ())

	//Handle Std out and Std err
	var stdBuffer bytes.Buffer
	mw := io.MultiWriter(os.Stdout, &stdBuffer)
	cmd.Stdout = mw
	cmd.Stderr = mw
	//Run the command, check for errors
	if err := cmd.Run(); err != nil {
		return err, ""
	}

	//Create empty output var
	var commandOut string

	//If command was run with Batch system get output
	if j.BatchExecution == true {
		err, commandOut = GetOutput(Script, fmt.Sprint(j.OutputScriptPth, "/lsf_out.log"))
		if err != nil {
			return err, ""
		}
	}
	//Return output
	return nil, commandOut
}

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
	if strings.Contains(fileText, "clone") {
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
	var stdBuffer bytes.Buffer
	mw := io.MultiWriter(os.Stdout, &stdBuffer)
	cmd.Stdout = mw
	cmd.Stderr = mw
	//Run the command, check for errors
	if err := cmd.Run(); err != nil {
		return err, ""
	}

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

//Parses lsf output file, finds the script that was just run, returns output if any when availible
func GetOutput(scriptName string, outputFile string) (err error, output string) {
	retry := true

	for retry {
		file, err := os.Open(outputFile)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.Contains(line, scriptName) {
				retry = false
			}
		}
		file.Close()
		file = nil
	}

	file, err := os.Open(outputFile)
	if err != nil {
		return err, ""
	}

	var lineArray []string
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
		if strings.Contains(line, scriptName) {
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
