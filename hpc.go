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
	ScriptContents string
	NativeSpecs    []string
	UID            int
	GID            int
}

func BuildScript(cmd, filenameSuffix string, myUid, myGid int) (err error, scriptPath string) {

	uniqueID := strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10)
	time.Sleep(50 * time.Millisecond)

	batchScriptFull := "/tmp/" + filenameSuffix + uniqueID + ".bash"
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
	err, Script := BuildScript(j.ScriptContents, "batch_script", j.UID, j.GID)
	if err != nil {
		return err, ""
	}

	file, err := ioutil.ReadFile(Script)
	if err != nil {
		return err, ""
	}

	fileText := string(file)

	var cmd *exec.Cmd

	if strings.Contains(fileText, "clone") {
		cmd = exec.Command("/bin/bash", Script)
	} else {
		cmd = exec.Command("bsub", "-o", "/tmp/lsf.out","-e","/tmp/lsf.err", strings.Join(j.NativeSpecs, " "), Script)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{}
	cmd.SysProcAttr.Credential = &syscall.Credential{Uid: uint32(j.UID), Gid: uint32(j.GID)}
	cmd.Env = append(os.Environ())

	var stdBuffer bytes.Buffer
	mw := io.MultiWriter(os.Stdout, &stdBuffer)
	cmd.Stdout = mw
	cmd.Stderr = mw
	if err := cmd.Run(); err != nil {
		return err, ""
	}

	var commandOut string

	if !strings.Contains(fileText, "clone") {
		err, commandOut = GetOutput(Script, "/tmp/lsf.out")
		if err != nil {
			return err, ""
		}
	}

	return nil, commandOut
}

func RunSlurm(j *Job) (err error, out string) {
	err, Script := BuildScript(j.ScriptContents, "batch_script", j.UID, j.GID)
	if err != nil {
		return err, ""
	}

	file, err := ioutil.ReadFile(Script)
	if err != nil {
		return err, ""
	}

	fileText := string(file)

	var cmd *exec.Cmd

	if strings.Contains(fileText, "clone") {
		cmd = exec.Command("/bin/bash", Script)
	} else {
		cmd = exec.Command("sbatch", "-o", "/tmp/slurm.out", strings.Join(j.NativeSpecs, " "), Script)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{}
	cmd.SysProcAttr.Credential = &syscall.Credential{Uid: uint32(j.UID), Gid: uint32(j.GID)}
	cmd.Env = append(os.Environ())

	var stdBuffer bytes.Buffer
	mw := io.MultiWriter(os.Stdout, &stdBuffer)
	cmd.Stdout = mw
	cmd.Stderr = mw
	if err := cmd.Run(); err != nil {
		return err, ""
	}

	var commandOut string

	if !strings.Contains(fileText, "clone") {
		file, err := ioutil.ReadFile("/tmp/slurm.out")
		if err != nil {
			return err, ""
		}

		commandOut = string(file)
	}

	return nil, commandOut
}

func GetOutput(scriptName string, outputFile string) (err error, output string) {
	retry := true

	for retry {

		file, err := os.Open(outputFile)
		if err != nil {
			return err, ""
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
