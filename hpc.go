package hpc

import(
	"syscall"
	"bytes"
	"strings"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"time"
)

type Job struct {
	ScriptContents  string
	NativeSpecs string
	UID         int
	GID         int
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

//Detect batch system func should be added

func (j *Job) Run() (err error) {
	
	err, Script := BuildScript(j.ScriptContents,"batch_script",j.UID,j.GID)
	if err != nil{
	  return err
	}

	file, err := ioutil.ReadFile(Script)
	if err != nil{
	  return err
	}

	fileText := string(file)

	var cmd *exec.Cmd
	
	if strings.Contains(fileText,"clone"){
	  cmd = exec.Command("/bin/bash",Script)
	}else {
	  cmd = exec.Command("bsub","-o","/tmp/lsf.out","-e","lsf.err",Script)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{}
        cmd.SysProcAttr.Credential = &syscall.Credential{Uid: uint32(j.UID), Gid: uint32(j.GID)}
	fmt.Println("Setting UID to ",j.UID," ",j.GID)
	cmd.Env = append(os.Environ(),
		"MANPATH=/nfs/lsf/10.1/man:",
		"LSF_SERVERDIR=/nfs/lsf/10.1/linux2.6-glibc2.3-x86_64/etc",
		"LSF_LIBDIR=/nfs/lsf/10.1/linux2.6-glibc2.3-x86_64/lib",
		"LD_LIBRARY_PATH=/nfs/lsf/10.1/linux2.6-glibc2.3-x86_64/lib",
		"PATH=/nfs/lsf/10.1/linux2.6-glibc2.3-x86_64/etc:/nfs/lsf/10.1/linux2.6-glibc2.3-x86_64/bin:/sbin:/bin:/usr/sbin:/usr/bin",
		"LSF_BINDIR=/nfs/lsf/10.1/linux2.6-glibc2.3-x86_64/bin",
		"LSF_ENVDIR=/nfs/lsf/conf",
		"_=/bin/env", 
	)
	
	var stdBuffer bytes.Buffer
	mw := io.MultiWriter(os.Stdout, &stdBuffer)
	cmd.Stdout = mw
	cmd.Stderr = mw
	if err := cmd.Run(); err != nil {
 	   return err
	}

	fmt.Println(stdBuffer.String())	
	return nil

}
