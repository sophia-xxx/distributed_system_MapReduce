package maple_juice

import (
	"google.golang.org/protobuf/types/known/timestamppb"
	"mp3/net_node"
)

/*
split the whole sdfs file and generate file clips for every maple task
*/
func splitFile(filePath string, mapleNum int) []string {
	fileClips := make([]string, mapleNum)
	// read lines of sdfs_src_file
	return fileClips
}

/*
Server run maple task on file clip
*/
func MapleTask(fileName string, fileStart int, fileEnd int) {
	// read file clip, same as "get" command

	// execute maple_exe

	// reply to master

	// how to deal with maple_local_file??
}

/*
Server run maple task on file clip
*/
func JuiceTask(fileList []string) {
	// loop fileList
	// read intermediate sdfs file

	// execute juice_exe

	// append results in localFile

	// reply to master

	// append results in sdfs_result_file, same as "put" command

}

/********Master Function*********/

// master keep record of all maple/reduce tasks
var taskMap map[string]Task

// features to describe maple/juice task
type Task struct {
	TaskNum   int
	FileName  string
	FileStart int
	FileEnd   int
	Status    string
	TaskType  string // "maple"/"juice"
	ServerIp  string
	LastTime  *timestamppb.Timestamp
}

/*
Master init all variables
*/
func init() {

}

/*
Master schedule maple/juice tasks
*/
func schedule(fileClips []string) {
	// allocate tasks to servers and update TaskMap

}

/*
Master shuffle keys to generate N juice tasks
*/
func shuffle() {

}

/*
Master tracking progress/completion of tasks
*/
func updateTaskMap() {

}

/*
Master re-allocate tasks in failed servers
*/
func HandleFailure(n *net_node.Node, failed_index int) {
	// find failed server

	// get the task of failed server

	// add task into taskChannel

}

/*
Master clean all intermediate file in sdfs, same as "delete" command
*/
func cleanAllFiles() {

}
