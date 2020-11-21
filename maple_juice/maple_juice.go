package maple_juice

import (
	"fmt"
	"google.golang.org/protobuf/types/known/timestamppb"
	"mp3/file_system"
	"mp3/net_node"
	"net"
	"net/rpc"
)

/*
split the whole sdfs file and generate file clips for every maple task
*/
func splitFile(filePath string, mapleNum int) []string {
	fileClips := make([]string, mapleNum)
	// TODO:read lines of sdfs_src_file
	return fileClips
}

// define server interface
type Server struct {
	IP   string
	Port string
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
	ServerIp  string
	LastTime  *timestamppb.Timestamp
}

/*
init sever
*/
func (mapleServer *Server) newMapleServer() *Server {
	server := &Server{
		//TODO:get ip and port of this node
	}
	return server
}

/*
Server run maple task on file clip
*/
//fileName string, fileStart int, fileEnd int
func (mapleServer *Server) MapleTask(args Task, replyKeyList *[]string) error {
	// read file clip, same as "get" command
	node := &net_node.Node{
		//TODO: how to use node??
	}
	go file_system.GetFile(node, args.FileName, args.FilePath)
	//TODO: notify that the file has been stored successfully

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
