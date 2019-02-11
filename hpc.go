package hpc

import (
	"bufio"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"os/user"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/radovskyb/watcher"
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
	KillJob() error
}

var CurrentLSF LSFJob
var CurrentSLURM SlurmJob
var CurrentCOBALT CobaltJob

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
		if parameter != "" {
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
	script := `#!/bin/bash

[ -f /etc/profile ] && { . /etc/profile || :; }
for f in ~/.bash_profile ~/.bash_login ~/.profile ; do
    if [ -f "$f" ] ; then
        . "$f" || :
        break
    fi
done
unset f

set -eo pipefail
set +o noclobber
`
	fmt.Fprintf(batchScript, script)
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
		log.Debug("Detected nobatch")
		return j.RunJob()
	}

	_, err = exec.LookPath("sbatch")
	if err == nil {
		log.Debug("Detected SLURM")
		l := new(SlurmJob)
		_, CurrentSLURM = l.New(j)
		return CurrentSLURM.RunJob()
	}
	_, err = exec.LookPath("bsub")
	if err == nil {
		log.Debug("Detected LSF")
		l := new(LSFJob)
		_, CurrentLSF = l.New(j)
		return CurrentLSF.RunJob()
	}
	_, err = exec.LookPath("qsub")
	if err == nil {
		log.Debug("Detected Cobalt")
		_, err = exec.LookPath("qstat")
		if err == nil {
			l := new(CobaltJob)
			_, CurrentCOBALT = l.New(j)
			return CurrentCOBALT.RunJob()
		} else {
			return fmt.Errorf("Cobalt detected but can't monitor (found qsub but no qstat)"), ""
		}
	}

	return fmt.Errorf("No batch system found"), ""
}

func (j *Job) Kill() {
	if j.BatchExecution == false {
		return
	}

	_, err := exec.LookPath("sbatch")
	if err == nil {
		CurrentSLURM.KillJob()
		return
	}
	_, err = exec.LookPath("bsub")
	if err == nil {
		CurrentLSF.KillJob()
		return
	}
	_, err = exec.LookPath("qsub")
	if err == nil {
		_, err = exec.LookPath("qstat")
		if err == nil {
			CurrentCOBALT.KillJob()
			return
		} else {
			fmt.Println("Cobalt error")
		}
	}
}

func (j *Job) tailPipe(pipe io.ReadCloser) {
	scanner := bufio.NewScanner(pipe)
	for scanner.Scan() {
		j.PrintToParent(scanner.Text())
	}

	return
}

func (j *Job) tailFile(fileName string, done chan bool) {
	w := watcher.New()
	defer w.Close()

	w.SetMaxEvents(1)
	w.FilterOps(watcher.Write)
	r := regexp.MustCompile("^abc$")
	w.AddFilterHook(watcher.RegexFilterHook(r, false))

	file, ferr := os.Open(fileName)
	if ferr != nil {
		log.Fatal(ferr)
	}
	defer file.Close()

	go func() {

		file.Seek(0, os.SEEK_CUR)

		for {
			select {
			case <-w.Event:
				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					j.PrintToParent(scanner.Text())
				}
			case <-w.Closed:
				fmt.Println("Closed")
			}
		}
	}()

	// Watch this folder for changes.
	if err := w.Add(fileName); err != nil {
		log.Fatalln(err)
	}

	// Start the watching process - it'll check for changes every 100ms.
	if err := w.Start(time.Millisecond * 100); err != nil {
		log.Fatalln(err)
	}

}

func (j *Job) mkTempFile(job *Job, template string) (out string, err error) {
	file, err := ioutil.TempFile(job.OutputScriptPth, template)
	if err != nil {
		return "", err
	}

	fileName := file.Name()

	if err = os.Chown(fileName, job.UID, job.GID); err != nil {
		log.WithFields(log.Fields{
			"filename": fileName,
			"uid":      job.UID,
			"gid":      job.GID,
			"error":    err,
		}).Error("os.Chown() failed")
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
		log.WithFields(log.Fields{
			"uid": j.UID,
		}).Warn("User lookup failed")
	}

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
		default:
			safeEnv = append(safeEnv, entry)
		}
	}
	cmd.Env = safeEnv

	log.WithFields(log.Fields{
		"args": args,
		"uid":  j.UID,
		"gid":  j.GID,
		"env":  safeEnv,
	}).Debug("Built SetUID command")

	return
}
