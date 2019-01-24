package hpc

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
)

type Job struct {
	ScriptContents  string
	NativeSpecs     []string
	UID             int
	GID             int
	OutputScriptPth string
	BatchExecution  bool
	PrintToParent   func(string)
}

type BatchJob interface {
	New() (error, interface{})
	RunJob() (error, string)
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
		if parameter != ""{
		  output = append(output, parameter)
		}
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
	fmt.Fprintf(batchScript, "set -eo pipefail\n")
	fmt.Fprintf(batchScript, "set +o noclobber\n")
	fmt.Fprintf(batchScript, cmd)

	chmodErr := os.Chmod(batchScriptFull, 0750)
	chownErr := os.Chown(batchScriptFull, myUid, myGid)

	if chmodErr != nil || chownErr != nil {
		return fmt.Errorf("Could not set the script to be executable: %v", chmodErr), ""
	}
	batchScript.Close()

	return nil, batchScriptFull

}

//Initial func run. Gets batch system and calls the corresponding run
func (j *Job) Run() (err error, out string) {
	if j.BatchExecution == false {
		return j.RunJob()
	}

	_, err = exec.LookPath("bsub")
        if err == nil {
                l := new(LSFJob)
                _, batch := l.New(j)
                return batch.RunJob()
        }
	_, err = exec.LookPath("salloc")
	if err == nil {
		l := new(SlurmJob)
		_, batch := l.New(j)
		return batch.RunJob()
	}

	_, err = exec.LookPath("cqsub")
	if err == nil {
		_, err = exec.LookPath("cqstat")
		if err == nil {
			l := new(CobaltJob)
			_, batch := l.New(j)
			return batch.RunJob()
		} else {
			return fmt.Errorf("Cobalt detected but can't monitor (found cqsub but no cqstat)"), ""
		}
	}

	return fmt.Errorf("No batch system found"), ""
}

func (j *Job) tailPipe(pipe io.ReadCloser) {
	scanner := bufio.NewScanner(pipe)
	for scanner.Scan() {
		j.PrintToParent(scanner.Text())
	}

	return
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
			//j.PrintToParent(fmt.Sprintf("Waiting for file %s...", fileName))
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

func (j *Job) mkTempFile(job *Job, template string) (out string, err error) {
	file, err := ioutil.TempFile(job.OutputScriptPth, template)
	if err != nil {
		return "", err
	}

	fileName := file.Name()

	if err = os.Chown(fileName, job.UID, job.GID); err != nil {
		log.Printf("os.Chown(%s, %d, %d) failed: %#v", fileName, job.UID, job.GID, err)
		return "", err
	}

	return fileName, nil
}

func (j *Job) setUid(args []string) (cmd *exec.Cmd) {
	cmd = exec.Command(args[0], args[1:]...)

	//Assign setUID information and env. vars
	cmd.SysProcAttr = &syscall.SysProcAttr{}
	cmd.SysProcAttr.Credential = &syscall.Credential{Uid: uint32(j.UID), Gid: uint32(j.GID)}

	// Sanitize the environment
	user, err := user.LookupId(fmt.Sprintf("%d", j.UID))
	if err != nil {
		log.Printf("Lookup failed for user %d", j.UID)
	}

	fmt.Println("Before ", os.Environ())

	var safeEnv []string
	for _, entry := range os.Environ() {
		env := strings.SplitN(entry, "=", 2)
		switch env[0] {
		case "LOGNAME", "USER":
			// Return the setuid username
			if user != nil {
				safeEnv = append(safeEnv, fmt.Sprintf("%s=%s", env[0], user.Username))
			}
		case "HOME":
			// Return the setuid user home directory
			if user != nil {
				safeEnv = append(safeEnv, fmt.Sprintf("%s=%s", env[0], user.HomeDir))
			}
		case "PATH":
			safeEnv = append(safeEnv, entry)
		default:
			//If its LSF related
			if strings.Contains(env[0], "LSF"){
				safeEnv = append(safeEnv, fmt.Sprint(entry))
			}
		}
	}
	cmd.Env = safeEnv

	fmt.Println("After ", cmd.Env)

	return
}
