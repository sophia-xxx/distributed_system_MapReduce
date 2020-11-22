package maple_juice

import (
	"bufio"
	"fmt"
	"log"
	"mp3/file_system"
	"mp3/net_node"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	CLIPPREFIX  = "sdfs_src_file_clip_"
	FILEPREFIX  = "sdfs_intermediate_file_prefix_"
	RPCPORT     = "1234"
	GETFILEWAIT = 4 * time.Second
)

var TimeFormat = "2006-01-02 15:04:05"

/*
client split the whole sdfs_src_file and generate file clips
*/
func splitFile(n *net_node.Node, mapleNum int, sdfsFileName string, localFileName string) map[int]string {
	fmt.Println(">>Start clipping files")
	fileClips := make(map[int]string, mapleNum)
	// get sdfs_src_file
	go file_system.GetFile(n, sdfsFileName, localFileName)
	time.Sleep(GETFILEWAIT)
	// check if we get the file
	if !WhetherFileExist(localFileName) {
		fmt.Println("Can't get the file:  " + sdfsFileName + ". Check the Internet!")
		return nil
	}
	// read lines of file
	file, _ := os.Open(localFileName)
	defer file.Close()
	fileScanner := bufio.NewScanner(file)
	lineCount := 0
	for fileScanner.Scan() {
		lineCount++
	}
	fmt.Println("File has " + strconv.Itoa(lineCount) + " lines.")
	// split file into file clips, then generate list of fileNames
	splitLines := lineCount/mapleNum + 1
	fileScanner = bufio.NewScanner(file)
	// determine whether the file is end
	endScan := false
	for fileScanner.Scan() {
		count := 0
		var fileSplit *os.File
		// create new files for different file clips
		fileSplit, _ = os.Create(CLIPPREFIX + strconv.Itoa(count))
		defer fileSplit.Close()
		for i := 0; i < splitLines-1; i++ {
			line := fileScanner.Text()
			fileSplit.WriteString(line)
			if !fileScanner.Scan() {
				endScan = true
				break
			}
		}
		if endScan {
			break
		}
		// last line
		line := fileScanner.Text()
		fileSplit.WriteString(line)
		// check whether this write successfully
		fileInfo, _ := fileSplit.Stat()
		fileClips[count] = CLIPPREFIX + strconv.Itoa(count)
		fmt.Println("File clip: ", fileInfo.Size())
		count++
	}
	fmt.Println(">>Finish clipping files")
	return fileClips
}

/*
client call master to start schedule tasks
*/
func callMapleJuice() {

}

// define server interface
type Server struct {
	NodeInfo *net_node.Node
}

// features to describe maple/juice task
type Task struct {
	TaskNum  int
	FileName string
	FilePath string
	Status   string
	TaskType string // "maple"/"juice"
	ServerIp string // server in charge of this task
	SourceIp string // server has that file
	LastTime *timestamppb.Timestamp
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
	time.Sleep(GETFILEWAIT)
	// check if we get the file
	if !WhetherFileExist(args.FileName) {
		fmt.Println("Can't get the file:  " + args.FileName + ". Check the Internet!")
		return nil
	}

	fileClip, err := os.Open(args.FileName) // TODO：is this filename same as local_filePath??
	input_FileName := arg.FileName          //need update
	net_node.CheckError(err)
	defer fileClip.Close()
	// execute maple_exe

	// how to deal with maple_local_file??
	// get a "result" file after the maple_exe finished
	// scan the "result" file by line to map and using this map to output file

	sdfs_prefix = args.prefix //need add a prefix parameter in args

	//read in stdin now, need to use some sort of ifstream
	var of_map map[string]*os.File
	of_map = make(map[string]*os.File)
	input := bufio.NewScanner(os.Stdin) // need update to input file stream
	for {
		if !input.Scan() {
			break
		}
		line := input.Text()
		line = strings.TrimSpace(line)
		str := strings.Split(line, " ")
		key := str[0]
		f, ok := of_map[key]
		if !ok {
			append_file_name := sdfs_prefix + "_" + key
			//f, err := os.OpenFile(append_file_name, os.O_RDONLY|os.O_CREATE|os.O_APPEND, 0666)
			f, err := os.Create(append_file_name)
			if err != nil {
				log.Println("open file error :", err)
				return
			}
			of_map[key] = f
		}
		f = of_map[key]
		_, err := f.WriteString(line + "\n")
		if err != nil {
			log.Println(err)
			return
		}
	}
	for key := range of_map {
		of_map[key].Close()
	}

	return nil
}

/*
Server start listening RPC call
*/
func StartServerRPC(mapleServer *Server) {
	rpc.Register(mapleServer)
	listener, _ := net.Listen("tcp", RPCPORT)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Can't start tcp connection")
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
// define master interface
type Master struct {
	NodeInfo *net_node.Node
}

// master keep record of all maple/reduce tasks
var taskMap map[string]Task

/*
Master init all variables
*/
func init() {

}

/*
master rpc method to start MapleJuice
*/
func (master *Master) startMapleJuice() {

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
	/*client, _ := rpc.Dial("tcp", address)
	args := &Task{}
	mapleResults := make([]string, 9)
	callServer := client.Go("MJServer.MapleTask", args, mapleResults, nil)
	replyCall := <-callServer.Done
	if replyCall.Error != nil {

	}*/

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

func getTimeString() string {
	return "(" + strings.Split(time.Now().Format(TimeFormat), " ")[1] + ")"
}
