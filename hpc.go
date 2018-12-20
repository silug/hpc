package hpc

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

type Job struct {
	ScriptContents  string
	NativeSpecs     []string
	Bank            string
	UID             int
	GID             int
	OutputScriptPth string
	BatchExecution  bool
	PrintToParent   func(string)
}

type BatchJob interface {
	New() (error, interface{})
	RunJob() (error, string)
	WaitForJob()
	GetOutput() (error, string)
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
func (j *Job) DetectBatchSystem() error {
	//if j.BatchExecution == false {
	//	return nil, j
	//}

	osEnv := os.Environ()
	//counter := 0
	for _, entry := range osEnv {
		if strings.Contains(entry, "LSF_BINDIR") {
			l := new(LSFJob)
			_, batch := l.New(j)
			batch.RunJob()
			batch.WaitForJob()
			return nil
		}
	}

	_, err := exec.LookPath("sbatch")
	if err == nil {
		l := new(SlurmJob)
		_, batch := l.New(j)
		batch.RunJob()
		batch.WaitForJob()
		return nil
	}

	_, err = exec.LookPath("cqsub")
	if err == nil {
		_, err = exec.LookPath("cqstat")
		if err == nil {
			l := new(CobaltJob)
			_, batch := l.New(j)
			batch.RunJob()
			batch.WaitForJob()
			return nil
		} else {
			return fmt.Errorf("Cobalt detected but can't monitor (found cqsub but no cqstat)")
		}
	}

	return fmt.Errorf("No batch system found")
}

//Initial func run. Gets batch system and calls the corresponding run
func (j *Job) Run() (err error, out string) {
	err = j.DetectBatchSystem()
	if err != nil {
		return err, ""
	}

	//batch.RunJob()
	//batch.WaitForJob()
	//batch.GetOutput()

	return nil, ""
}

func (j *Job) tailFile(fileName string, done chan bool) {
	watcher, werr := fsnotify.NewWatcher()
	if werr != nil {
		log.Fatal(werr)
	}
	defer watcher.Close()

	for {
		if _, err := os.Stat(fileName); os.IsNotExist(err) {
			time.Sleep(10 * time.Millisecond)
			j.PrintToParent(fmt.Sprintf("Waiting for file %s...", fileName))
			continue
		}
		break
	}

	werr = watcher.Add(fileName)
	if werr != nil {
		log.Fatal(werr)
	}

	file, ferr := os.Open(fileName)
	if ferr != nil {
		log.Fatal(ferr)
	}
	defer file.Close()

	for {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			j.PrintToParent(scanner.Text())
		}
		file.Seek(0, os.SEEK_CUR)

		select {
		case _, ok := <-watcher.Events:
			if !ok {
				return
			}

		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Printf("Error: %#v", err)

		case <-done:
			return
		}
	}
}

func (j *Job) mkTempFile(template string) (out string, err error) {
	file, err := ioutil.TempFile(j.OutputScriptPth, template)
	if err != nil {
		return "", err
	}

	fileName := file.Name()

	if err = os.Chown(fileName, j.UID, j.GID); err != nil {
		log.Printf("os.Chown(%s, %d, %d) failed: %#v", fileName, j.UID, j.GID, err)
		return "", err
	}

	return fileName, nil
}
