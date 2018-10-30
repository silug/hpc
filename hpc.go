package hpc

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Job struct {
	ScriptPath  string
	NativeSpecs string
	UID         int
}

func BuildScript(cmd, pathToSave, filenameSuffix string) (err error, scriptPath string) {

	uniqueID := strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10)

	batchScriptFull := pathToSave + "/" + filenameSuffix + uniqueID + ".bash"
	batchScript, scriptErr := os.Create(batchScriptFull)
	if scriptErr != nil {
		return fmt.Errorf("Unable to create Script file: %v", scriptErr), ""
	}
	fmt.Fprintf(batchScript, "#!/bin/bash\n")
	fmt.Fprintf(batchScript, cmd)

	chmodErr := os.Chmod(batchScriptFull, 0750)
	if chmodErr != nil {
		return fmt.Errorf("Could not set the script to be executable: %v", chmodErr), ""
	}
	batchScript.Close()

	return nil, batchScriptFull

}

//Detect batch system func should be added

func (j *Job) Run() {
	fmt.Println("Run Function")
}
