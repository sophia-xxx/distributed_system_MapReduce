package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
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
	MASTERIP    = "172.22.94.48"
)

var TimeFormat = "2006-01-02 15:04:05"

/*************************For client start maple and juice****************************/

// filepath is "./filename"
func split(fileName string, clipNum int) map[int]string {
	fileClips := make(map[int]string, clipNum)
	// read lines of file
	file, _ := os.Open("./" + fileName)
	defer file.Close()
	fileScanner := bufio.NewScanner(file)
	lineCount := 0
	for fileScanner.Scan() {
		lineCount++
	}
	fmt.Println("File has " + strconv.Itoa(lineCount) + " lines!!!")

	// split file into file clips, then generate list of fileNames
	splitLines := lineCount/clipNum + 1
	// re-open the file
	file, _ = os.Open("./" + fileName)
	fileScanner = bufio.NewScanner(file)
	// determine whether the file is end
	endScan := false
	count := 0
	for fileScanner.Scan() {
		var fileSplit *os.File
		// create new files for different file clips
		fileSplit, _ = os.Create("./" + CLIPPREFIX + strconv.Itoa(count))
		defer fileSplit.Close()
		for i := 0; i < splitLines-1; i++ {
			line := fileScanner.Text()
			fileSplit.WriteString(line + "\n")
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
		fileSplit.WriteString(line + "\n")
		// check whether this write successfully
		fileInfo, _ := fileSplit.Stat()
		//fileClips[count] = CLIPPREFIX + strconv.Itoa(count)
		fmt.Println("File clip: ", fileInfo.Size())
		count++
	}
	//return fileClips
	return fileClips
}

/*
client split the whole sdfs_src_file and generate file clips
*/
func splitFile(n *net_node.Node, mapleNum int, sdfsFileName string, localFileName string) map[int]string {
	fileClips := make(map[int]string, mapleNum)
	// get sdfs_src_file
	go file_system.GetFile(n, sdfsFileName, localFileName)
	time.Sleep(GETFILEWAIT)
	// check if we get the file
	if !WhetherFileExist(localFileName) {
		fmt.Println("Can't get the file:  " + sdfsFileName + ". Check the Internet!")
		return nil
	}
	fmt.Println(">>Start clipping files")
	fileClips = split(localFileName, mapleNum)
	fmt.Println(">>Finish clipping files")
	return fileClips
}

/*
client start MapleJuice
*/
// client deal with maple phase command:
// maple maple_exe num_maple sdfs_intermediate sdfs_src
// and call master to start schedule tasks

func CallMaple(n *net_node.Node, workType string, mapleExe string, mapleNum int, sdfsSrcFile string) {
	// set connection with master RPC
	var reply bool
	files := splitFile(n, mapleNum, sdfsSrcFile, sdfsSrcFile)
	client, err := rpc.Dial("tcp", MASTERIP+":"+RPCPORT)
	if err != nil {
		fmt.Println("Can't set connection with remote process!")
		return
	}
	// call master RPC
	args := &MJReq{
		WorkType: workType,
		MapleExe: mapleExe,
		MapleNum: mapleNum,
		FileClip: files,
		SenderIp: n.Address.IP,
		NodeInfo: n,
	}
	if err := client.Call("Master.StartMapleJuice", args, &reply); err != nil {
		fmt.Println("Can't start MapleJuice!")
	}

}

/*****************************For server RPC********************************/
// define server interface
type Server struct {
	NodeInfo *net_node.Node
}

// features to describe maple/juice task
type Task struct {
	TaskNum       int
	SdfsFileName  string
	LocalFileName string
	Status        string
	TaskType      string // "maple"/"juice"
	ServerIp      string // server in charge of this task
	SourceIp      string // server has that file
	LastTime      *timestamppb.Timestamp
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
func (mapleServer *Server) MapleTask(n *net_node.Node, args Task, replyKeyList *[]string) error {
	// read file clip, same as "get" command
	// var fileReq = make(chan bool)
	node := mapleServer.NodeInfo
	// check if we have the file
	if _, ok := mapleServer.NodeInfo.Files[args.SdfsFileName]; !ok {
		fmt.Println(args.SdfsFileName, "not exist!")
		return nil
	}
	go file_system.GetFile(node, args.SdfsFileName, args.LocalFileName)
	time.Sleep(GETFILEWAIT)
	// check if we get the file
	if !WhetherFileExist(args.LocalFileName) {
		fmt.Println("Can't get the file:  " + args.SdfsFileName + ". Check the Internet!")
		return nil
	}

	fileClip, err := os.Open(args.LocalFileName)
	//input_FileName := args.SdfsFileName         //need update
	net_node.CheckError(err)
	defer fileClip.Close()
	// execute maple_exe

	// how to deal with maple_local_file??
	// get a "result" file after the maple_exe finished
	// scan the "result" file by line to map and using this map to output file

	//sdfs_prefix = args.prefix //need add a prefix parameter in args

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
			append_file_name := FILEPREFIX + key
			//f, err := os.OpenFile(append_file_name, os.O_RDONLY|os.O_CREATE|os.O_APPEND, 0666)
			f, err := os.Create(append_file_name)
			if err != nil {
				log.Println("open file error :", err)
				return nil
			}
			of_map[key] = f
		}
		f = of_map[key]
		_, err := f.WriteString(line + "\n")
		if err != nil {
			log.Println(err)
			return nil
		}
	}
	for key := range of_map {
		of_map[key].Close()
	}

	for key := range of_map {
		local_file_path := FILEPREFIX + key
		f, err := os.Stat(local_file_path)
		inter_target_index := hash_string_to_int(key)
		send_file_tcp(n, inter_target_index, local_file_path, local_file_path, f.Size())
		// Is filepath = filename here? Is it sending multiple files here? will they wait others?
	}
	return nil
}

/*
Server start listening RPC call
*/
func StartServerRPC(mapleServer *Server) {
	rpc.Register(mapleServer)
	listener, _ := net.Listen("tcp", ":"+RPCPORT)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Can't start tcp connection at rpc port in server")
			return
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
	// append results in sdfs_result_file, same as "put" command
}

/**************************Master Function****************************/
// define master interface
type Master struct {
	NodeInfo    *net_node.Node
	FileTaskMap map[string]string // file->Task
	TaskMap     map[string]Task   // file->serverIp
}

// define master rpc para
type MJReq struct {
	WorkType string
	MapleExe string
	MapleNum int
	FileClip map[int]string
	SenderIp net.IP
	NodeInfo *net_node.Node
}

// master keep record of all maple/reduce tasks

/*
Master init all variables
*/
func (master *Master) NewMaster(n *net_node.Node) *Master {
	newMaster := &Master{
		NodeInfo:    n,
		FileTaskMap: make(map[string]string),
		TaskMap:     make(map[string]Task),
	}
	return newMaster
}

/*
master rpc method to start MapleJuice
*/
func (master *Master) StartMapleJuice(mjreq MJReq, reply *bool) error {
	// get all potential servers
	members := mjreq.NodeInfo.Table
	servers := make([]string, 10)
	for _, member := range members {
		IPString := changeIPtoString(member.Address.Ip)
		if strings.Compare(IPString, MASTERIP) != 0 {
			servers = append(servers, IPString)
		}
	}
	if len(servers) == 0 {
		fmt.Println("There is no available servers!")
		return nil
	}
	fileClips := mjreq.FileClip
	// schedule the maple tasks
	for i, server := range servers {
		// hash server Ip and get the index of fileClips
		index := int(Hash(server)) % len(servers)
		// whether the file is already allocated
		if _, ok := master.FileTaskMap[fileClips[index]]; ok {

		}
		master.FileTaskMap[fileClips[index]] = server

		task := &Task{
			TaskNum: i,
		}
	}
	// asynchronize call server RPC function

	//
	*reply = true
	return nil
}

/*
master start listening RPC call
*/
func StartMasterRpc(master *Master) {
	rpc.Register(master)
	listener, _ := net.Listen("tcp", ":"+RPCPORT)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Can't start tcp connection at rpc port in master")
			return
		}
		go rpc.ServeConn(conn)
	}
}

/*
Master schedule maple/juice tasks
*/
func schedule(fileClips map[int]string) {
	// allocate tasks to servers

	// call their RPC methods
	/*client, _ := rpc.Dial("tcp", address)
	args := &Task{}
	mapleResults := make([]string, 9)
	callServer := client.Go("MJServer.MapleTask", args, mapleResults, nil)
	replyCall := <-callServer.Done
	if replyCall.Error != nil {
	}*/

	//update TaskMap

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

func changeIPtoString(ip []byte) string {
	var IPString []string
	for _, i := range ip {
		IPString = append(IPString, strconv.Itoa(int(i)))
	}
	res := strings.Join(IPString, ".")
	return res
}
func Hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

/*
Hash a key string into a int
*/
func hash_string_to_int(n *net_node.Node, key string) int{
	h := fnv.New32a()
	h.Write([]byte(key))
	val := h.Sum32()
	alive_server_size := len(n.table)
	return val % alive_server_size  //Is uint32 % int get a int validly?
}