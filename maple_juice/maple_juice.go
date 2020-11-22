package maple_juice

import (
	"fmt"
	"google.golang.org/protobuf/types/known/timestamppb"
	"mp3/file_system"
	"mp3/net_node"
	"net"
	"net/rpc"
	"os"
	"time"
)

/*
split the whole sdfs file and generate file clips for every maple task
*/
func splitFile(filePath string, mapleNum int) []string {
	fileClips := make([]string, mapleNum)
	// TODO:get sdfs_src_file
	// TODO:read lines of
	return fileClips
}

// define server interface
type Server struct {
	NodeInfo *net_node.Node
}

// features to describe maple/juice task
type Task struct {
	TaskNum   int
	FileName  string
	FilePath  string
	FileStart int
	FileEnd   int
	Status    string
	TaskType  string // "maple"/"juice"
	ServerIp  string // server in charge of this task
	SourceIp  string // server has that file
	LastTime  *timestamppb.Timestamp
}

/*
init sever
*/
func (mapleServer *Server) newMapleServer(n *net_node.Node) *Server {
	server := &Server{
		NodeInfo: n,
	}
	return server
}

/*
Server run maple task on file clip
*/
//fileName string, fileStart int, fileEnd int
func (mapleServer *Server) MapleTask(args Task, replyKeyList *[]string) error {
	// read file clip, same as "get" command
	// var fileReq = make(chan bool)
	node := mapleServer.NodeInfo
	// check if we have the file
	if _, ok := mapleServer.NodeInfo.Files[args.FileName]; !ok {
		fmt.Println(args.FileName, "not exist!")
		return nil
	}
	go file_system.GetFile(node, args.FileName, args.FilePath)
	time.Sleep(4 * time.Second)
	// check if we get the file
	if !WhetherFileExist(args.FileName) {
		fmt.Println("Can't get the file:  " + args.FileName + ". Check the Internet!")
		return nil
	}

	sdfsFile, err := os.Open(args.FileName) // TODO：is this filename same as local_filePath??
	net_node.CheckError(err)
	defer sdfsFile.Close()
	// execute maple_exe

	// reply to master

	// how to deal with maple_local_file??
	return nil
}

/*
Server start listening RPC call
*/
func StartRPC(mapleServer *Server) {
	rpc.Register(mapleServer)
	listener, _ := net.Listen("tcp", "1234")
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("error...")
		}
		go rpc.ServeConn(conn)
	}
}

/*
Server run juice task on file clip
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
	client, _ := rpc.Dial("tcp", address)
	args := &Task{}
	mapleResults := make([]string, 9)
	callServer := client.Go("MJServer.MapleTask", args, mapleResults, nil)
	replyCall := <-callServer.Done
	if replyCall.Error != nil {

	}

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

/*****Utils*****/
func WhetherFileExist(filepath string) bool {
	info, err := os.Stat(filepath)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}
