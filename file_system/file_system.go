package file_system

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"mp3/config"
	"mp3/net_node"
	pings "mp3/ping_protobuff"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var of_map map[string]*os.File
var mutex sync.Mutex

/*
 * Implementation of the ls command
 * Simply queries the file map for the input file and prints the corresponding list of servers
 */
func ListServersWithFile(n *net_node.Node, filename string) {
	if _, ok := n.Files[filename]; ok {
		fmt.Println(n.Files[filename].Servers)
	} else {
		fmt.Println(filename, "does not exist in system")
	}
}

/*
 * Implementation of the store command
 * Iterates through the file map, gets the files on the server, and prints them
 */
func ListFilesOnServer(n *net_node.Node) {
	var s []string
	for file, servs := range n.Files {
		if n.Files[file].Writing == false {
			if servs.Servers[0] == int32(n.Index) || servs.Servers[1] == int32(n.Index) || servs.Servers[2] == int32(n.Index) || servs.Servers[3] == int32(n.Index) {
				s = append(s, file)
			}
		}
	}
	fmt.Println(s)
}

/*
 * Return true if the file is in the filesystem and false otherwise
 */
func file_in_filesystem(n *net_node.Node, filename string) bool {
	_, ok := n.Files[filename]
	return ok
}

/*
 *  Given two file lists, merge them
 */
func MergeFileList(n *net_node.Node, msg *pings.TableMessasgeProto) {
	for filename, other_metadata := range msg.Files {
		// If the file does not exist on this machine,
		// wait to receive the put update
		// Otherwise, update the metadata as appropriate
		if _, ok := n.Files[filename]; ok {
			if n.Files[filename].LastTime.AsTime().Before(other_metadata.LastTime.AsTime()) {
				n.Files[filename].LastTime = other_metadata.LastTime
				n.Files[filename].FileSize = other_metadata.FileSize
				n.Files[filename].Servers = other_metadata.Servers
			}
		} else {
			n.Files[filename] = other_metadata
			n.Files[filename].Writing = false
			//fmt.Println(filename + " Set Writing to " + strconv.FormatBool(n.Files[filename].Writing) + " | MergeFileList")
		}
	}
}

/*
 * File send Protocol:
 * P_[file_size][filename][file_data]
 */
func Send_file_tcp(n *net_node.Node, server_index int32, local_filepath string, filename string, file_size int64, sdfs_prefix string, need_append bool) {
	// Open a TCP connection
	remote_addr := net_node.ConvertToAddr(n.Table[server_index].Address)
	remote_tcp_addr := net_node.ConvertUDPToTCP(*remote_addr)
	conn, err := net.DialTCP("tcp", nil, remote_tcp_addr)
	net_node.CheckError(err)
	if err != nil {
		fmt.Println("Can't dial server.")
		return
	}
	defer conn.Close()

	var first_line []byte
	// Now, send over the file metadata
	file_size_str := fmt.Sprintf("%10d", file_size)
	filename_str := fmt.Sprintf("%100s", filename)
	sdfsPrefix_str := fmt.Sprintf("%100s", sdfs_prefix) //might have a problem
	if need_append {
		first_line = []byte("PA" + file_size_str + filename_str + sdfsPrefix_str)
	} else {
		//debug
		//fmt.Println(filename)
		first_line = []byte("P_" + file_size_str + filename_str + sdfsPrefix_str)
	}
	conn.Write(first_line)

	// Send the file over 16,384 bytes at a time
	f, _ := os.Open(local_filepath)
	defer f.Close()
	io.Copy(conn, f)
}

/*
 * Copies one file on the local machine to another file on the local machine
 */
func write_file_locally(local_filepath string, filename string) {
	f_i, _ := os.Open(local_filepath)
	defer f_i.Close()

	f_o, _ := os.Create(filename)
	defer f_o.Close()

	_, err := io.Copy(f_o, f_i)
	net_node.CheckError(err)
}

/*
 * Gets the server with the lowest memory usage that is not already marked as seen
 */
func get_most_free_server(n *net_node.Node, seen *[]bool) int {
	var min_val int64 = 9223372036854775807 // This  is the max int64 value
	var min_index int = -1
	for i := 0; i < len(n.Table); i++ {
		if n.Table[i].Status == net_node.ACTIVE && !(*seen)[i] && n.Table[i].FileMemoryUsed < min_val {
			min_val = n.Table[i].FileMemoryUsed
			min_index = i
		}
	}

	return min_index
}

/*
 * Gets the four active servers with the lowest memory usage
 */
func get_four_most_free_servers(n *net_node.Node) []int32 {
	file_server_indices := [4]int32{-1, -1, -1, -1}
	seen := make([]bool, len(n.Table))
	for i := 0; i < len(n.Table); i++ {
		seen[i] = false
	}

	// Get the index of the four servers using the least amount of memory
	for i := 0; i < 4; i++ {
		min_index := get_most_free_server(n, &seen)

		// If the min_index  is -1, there are fewer than 4 other active servers
		if min_index == -1 {
			break
		} else {
			seen[min_index] = true
			file_server_indices[i] = int32(min_index)
		}
	}

	return file_server_indices[:]
}

/*
 * Given a list of servers and a filename, send the file to those servers
 */
func send_file_to_servers(n *net_node.Node, file_server_indices []int32, local_filepath string, filename string, file_size int64) {
	for _, server_index := range file_server_indices {

		// If the index is negative, skip the iteration
		// If the server index is the index of the current server, just write  the file locally
		// Otherwise, send the file to the appropriate server
		if server_index < 0 {
			continue
		} else if uint32(server_index) == n.Index {
			write_file_locally(local_filepath, filename)
		} else {
			// debug
			//fmt.Println(server_index, " filename: "+local_filepath+"  "+filename)
			Send_file_tcp(n, server_index, local_filepath, filename, file_size, "", false)
		}
	}
}

/*
 * Notify the other machines that the current machine is attempting to write a given file
 * Format: "WS[server_index][filename]"
 */
func notify_other_servers_of_file_write_start(n *net_node.Node, filename string) {
	// Construct the message
	// Format: WS[index][filename]
	index_str := fmt.Sprintf("%32d", n.Index)
	filename_str := fmt.Sprintf("%100s", filename)
	msg := []byte("WS" + index_str + filename_str)

	for i := 0; i < len(n.Table); i++ {
		if i == int(n.Index) || n.Table[i].Status != net_node.ACTIVE {
			continue
		}
		net_node.SendMsgTCP(net_node.ConvertToAddr(n.Table[i].Address), msg)
	}
}

/*
 * After receiving a message that another machine is attempting to write a file
 * wait for any preexisting reads and writes to complete. Then, lock the file
 * and send a response.
 */
func RespondToWriteStartMsg(n *net_node.Node, connection net.Conn) {
	// Get the server index
	server_index_buff := make([]byte, 32)
	connection.Read(server_index_buff)
	server_index_str := strings.Trim(string(server_index_buff), " ")
	i, _ := strconv.ParseInt(server_index_str, 10, 32)
	server_index := int32(i)

	// Get the filename
	file_name_buff := make([]byte, 100)
	connection.Read(file_name_buff)
	filename := strings.Trim(string(file_name_buff), " ")

	// Wait for any preexisting reads and writes to complete
	if file_in_filesystem(n, filename) {
		//fmt.Println(filename + " is in system with writing = " + strconv.FormatBool(n.Files[filename].Writing))
		//for n.Files[filename].Writing || n.Files[filename].NumReading > 0 {
		for n.Files[filename].Writing {
			fmt.Println(filename + " is writing")
			time.Sleep(300 * time.Millisecond)
		}
		//n.Files[filename].Writing = true
	} else {
		//n.Files[filename] = &pings.FileMetaDataProto{Writing: true, FileSize: 0}
		n.Files[filename] = &pings.FileMetaDataProto{Writing: false, FileSize: 0}
		//fmt.Println(filename + " Set Writing to false | RespondToWriteStartMsg")
	}

	// Now that all reads are complete, acknowledge that we have finished
	// writing the file
	// Format WA[filename]
	filename_str := fmt.Sprintf("%100s", filename)
	msg := []byte("WA" + filename_str)
	net_node.SendMsgTCP(net_node.ConvertToAddr(n.Table[server_index].Address), msg)
}

/*
 * After sending a message to another node that the current machine is attempting
 * to write a file, the other machine will respond. This function, handles the response.
 * It increments the number of machines that have acknowledged the write request.
 */
func ReceiveWriteStrMsgResponse(n *net_node.Node, connection net.Conn) {
	// Get the filename
	file_name_buff := make([]byte, 100)
	connection.Read(file_name_buff)
	filename := strings.Trim(string(file_name_buff), " ")
	mutex.Lock()
	n.Files[filename].NumAckWriting += 1
	mutex.Unlock()
}

/*
 * This is the control function for acquiring a write lock. The steps are as follows;
 * 1) Wait for all writes and reads on the  file to complete.
 * 2) Notify the other servers that we are attempting to write the file.
 * 3) Receive responses from all of the other active servers
 */
func acquire_distributed_write_lock(n *net_node.Node, filename string) {
	// If we are currently writing the file,
	// block until the write is finished
	if file_in_filesystem(n, filename) {
		for n.Files[filename].Writing || n.Files[filename].NumReading > 0 {
			time.Sleep(10 * time.Millisecond)
		}
		n.Files[filename].Writing = true
		//fmt.Println(filename + " Set Writing to true | acquire_distributed_write_lock fileinsystem")
	} else {
		//fmt.Println(filename + " not in system, add to local list")
		n.Files[filename] = &pings.FileMetaDataProto{Writing: true, FileSize: 0}
		//fmt.Println(filename + " Set Writing to true | acquire_distributed_write_lock filenotinsystem")
	}

	// Notify the servers that we are writing a file
	// so that other writes/reads will not be able to proceed
	notify_other_servers_of_file_write_start(n, filename)

	// Wait for the other servers to respond
	// for int(n.Files[filename].NumAckWriting) < net_node.NumActiveServ(n)-1 {
	// 	fmt.Println(filename + " got " + strconv.Itoa(int(n.Files[filename].NumAckWriting)) + " acks")
	// 	fmt.Println("numberactive serv is " + strconv.Itoa(net_node.NumActiveServ(n)))
	// 	fmt.Println("waiting for all ackwriting")
	// 	time.Sleep(100 * time.Millisecond)
	// }

	n.Files[filename].NumAckWriting = 0
}

/*
 * After we have finished writing a file, send the updated metadata to the other servers
 */
func notify_servers_of_file_put_complete(n *net_node.Node, servers []int32, filename string, file_size int64) {
	// Update the current node's metadata
	prev_size := n.Files[filename].FileSize
	file_meta_data := &pings.FileMetaDataProto{
		FileSize:      uint64(file_size),
		Servers:       servers,
		LastTime:      timestamppb.Now(),
		Writing:       false,
		NumAckWriting: 0,
		NumReading:    0,
	}
	n.Files[filename] = file_meta_data
	//fmt.Println(filename + " Set Writing to false | notify_servers_of_file_put_complete")

	// Update the current node's file size numbers
	for i := 0; i < len(n.Files[filename].Servers); i++ {
		server_index := n.Files[filename].Servers[i]
		if server_index < 0 {
			continue
		}
		n.Table[server_index].FileMemoryUsed += int64(file_size - int64(prev_size))
	}

	// Construct the message
	// Format: WE[filename][meta_data]
	filename_str := fmt.Sprintf("%100s", filename)
	data, _ := proto.Marshal(file_meta_data)
	buf := []byte(data)
	msg := append([]byte("WE"+filename_str), buf...)

	// Notify  the remaining servers
	for i := 0; i < len(n.Table); i++ {
		if i == int(n.Index) {
			continue
		}
		addr := net_node.ConvertToAddr(n.Table[i].Address)
		net_node.SendMsgTCP(addr, msg)
	}
}

/*
 * Update a file's metadata after a write completes
 */
func ReceiveFileWriteCompleteMsg(n *net_node.Node, connection net.Conn) {
	// Get the filename
	file_name_buff := make([]byte, 100)
	connection.Read(file_name_buff)
	filename := strings.Trim(string(file_name_buff), " ")

	// Get the previous size
	prev_size := n.Files[filename].FileSize

	// Unmarshall the metadata protobuffer
	buf := make([]byte, 1024)
	size, err := connection.Read(buf)
	net_node.CheckError(err)
	meta_data := &pings.FileMetaDataProto{}
	err = proto.Unmarshal(buf[0:size], meta_data)
	net_node.CheckError(err)

	// Update the file's metadata
	n.Files[filename] = meta_data

	// if n.Files[filename].Writing == true {
	// 	fmt.Println(filename + " Set Writing to true | ReceiveFileWriteCompleteMsg")
	// } else {
	// 	fmt.Println(filename + " Set Writing to false | ReceiveFileWriteCompleteMsg")
	// }

	// Update the file size for all appropriate servers
	for i := 0; i < len(n.Files[filename].Servers); i++ {
		server_index := n.Files[filename].Servers[i]
		if server_index < 0 {
			continue
		}
		n.Table[server_index].FileMemoryUsed += int64(n.Files[filename].FileSize - prev_size)
	}
}

/*
 * Notify the other machines that the current machine is attempting to read a given file
 * Format: "RS[server_index][filename]"
 */
func notify_other_servers_of_file_read_start(n *net_node.Node, filename string) {
	// Construct the message
	// Format: RS[index][filename]
	index_str := fmt.Sprintf("%32d", n.Index)
	filename_str := fmt.Sprintf("%100s", filename)
	msg := []byte("RS" + index_str + filename_str)

	for i := 0; i < len(n.Table); i++ {
		if i == int(n.Index) {
			continue
		}
		net_node.SendMsgTCP(net_node.ConvertToAddr(n.Table[i].Address), msg)
	}
}

/*
 * After receiving a message that another machine is attempting to read a file
 * wait for any preexisting writes to complete. Then, increment NumReading
 * and send a response.
 */
func RespondToReadStartMsg(n *net_node.Node, connection net.Conn) {
	// Get the server index
	server_index_buff := make([]byte, 32)
	connection.Read(server_index_buff)
	server_index_str := strings.Trim(string(server_index_buff), " ")
	i, _ := strconv.ParseInt(server_index_str, 10, 32)
	server_index := int32(i)

	// Get the filename
	file_name_buff := make([]byte, 100)
	connection.Read(file_name_buff)
	filename := strings.Trim(string(file_name_buff), " ")

	// Wait for any preexisting  writes to complete
	for n.Files[filename].Writing {
		time.Sleep(10 * time.Millisecond)
	}
	n.Files[filename].NumReading += 1

	// Now that all reads are complete, acknowledge that we have finished
	// writing the file
	// Format WA[filename]
	filename_str := fmt.Sprintf("%100s", filename)
	msg := []byte("RA" + filename_str)
	net_node.SendMsgTCP(net_node.ConvertToAddr(n.Table[server_index].Address), msg)
}

/*
 * After sending a message to another node that the current machine is attempting
 * to read a file, the other machine will respond. This function, handles the response.
 * It increments the number of machines that have acknowledged the read request.
 */
func ReceiveReadStrMsgResponse(n *net_node.Node, connection net.Conn) {
	// Get the filename
	file_name_buff := make([]byte, 100)
	connection.Read(file_name_buff)
	filename := strings.Trim(string(file_name_buff), " ")
	n.Files[filename].NumAckReading += 1
}

/*
 * This is the control function for acquiring a read lock. The steps are as follows;
 * 1) Wait for all writes on the file to complete.
 * 2) Notify the other servers that we are attempting to read the file.
 * 3) Receive responses from all of the other active servers
 */
func acquire_distributed_read_lock(n *net_node.Node, filename string) {
	// If we are currently writing the file,
	// block until the write is finished
	for n.Files[filename].Writing {
		time.Sleep(10 * time.Millisecond)
	}
	n.Files[filename].NumReading += 1

	// Notify the servers that we are writing a file
	// so that other writes/reads will not be able to proceed
	notify_other_servers_of_file_read_start(n, filename)

	// Wait for the other servers to respond
	for int(n.Files[filename].NumAckReading) < net_node.NumActiveServ(n)-1 {
		time.Sleep(10 * time.Millisecond)
	}

	n.Files[filename].NumAckReading = 0
}

/*
 * After we have finished reading a file, notify the other servers
 */
func notify_servers_of_file_get_complete(n *net_node.Node, servers []int32, filename string, file_size int64) {
	// Construct the message
	// Format: RE[filename]
	filename_str := fmt.Sprintf("%100s", filename)
	msg := []byte("RE" + filename_str)

	// Notify  the remaining servers
	for i := 0; i < len(n.Table); i++ {
		if i == int(n.Index) {
			continue
		}
		addr := net_node.ConvertToAddr(n.Table[i].Address)
		net_node.SendMsgTCP(addr, msg)
	}
}

/*
 * Decrement NumReading after a read completes
 */
func ReceiveFileReadCompleteMsg(n *net_node.Node, connection net.Conn) {
	// Get the filename
	file_name_buff := make([]byte, 100)
	connection.Read(file_name_buff)
	filename := strings.Trim(string(file_name_buff), " ")

	n.Files[filename].NumReading -= 1
}

/*
 * Receives a file from another TCP server and writes it to a local file
 */
func ReceiveFile(connection net.Conn, need_append bool) {
	// Get the file size
	file_size_buff := make([]byte, 10)
	connection.Read(file_size_buff)
	file_size_str := strings.Trim(string(file_size_buff), " ")
	file_size, _ := strconv.ParseInt(file_size_str, 10, 64)

	// Now, get the file name
	file_name_buff := make([]byte, 100)
	connection.Read(file_name_buff)
	filename := strings.Trim(string(file_name_buff), " ")
	fmt.Println(filename)

	//Now, get the sdfs prefix
	sdfsPrefix_buff := make([]byte, 100)
	connection.Read(sdfsPrefix_buff)
	sdfsPrefix_str := strings.Trim(string(sdfsPrefix_buff), " ")
	if len(sdfsPrefix_str) != 0 {
		fmt.Println("With Prefix: " + sdfsPrefix_str)
	}

	// debug
	//fmt.Println("With Prefix: " + sdfsPrefix_str)

	// Create the file
	new_file, err := os.Create(filename)
	net_node.CheckError(err)
	defer new_file.Close()

	// Write the initial buffer
	buff := make([]byte, 16384)
	_, err = connection.Read(buff)
	net_node.CheckError(err)

	num_bytes_read := int64(16384)
	if num_bytes_read > file_size {
		num_bytes_read = file_size
	}
	data := buff[:num_bytes_read]
	new_file.Write(data)
	if need_append {
		nameTemp := strings.Split(filename, "_")
		if strings.Compare(nameTemp[len(nameTemp)-1], "reduce") == 0 {
			CreatAppendSdfsReduceFile(filename)
		} else {
			if len(filename) >= len(config.MAPLEFILEPREFIX) {
				file_name_prefix := filename[0:len(config.MAPLEFILEPREFIX)]
				if strings.Compare(file_name_prefix, config.MAPLEFILEPREFIX) == 0 {
					CreatAppendSdfsKeyFile(filename)
				}
			}
		}
	}
	// Now, write the rest of the file
	io.Copy(new_file, connection)
}

/*
 * Sends a local file over TCP
 */
func SendFile(n *net_node.Node, connection net.Conn) {
	// Get the server_index
	server_index_buff := make([]byte, 32)
	connection.Read(server_index_buff)
	server_index_str := strings.Trim(string(server_index_buff), " ")
	i, _ := strconv.ParseInt(server_index_str, 10, 32)
	server_index := int32(i)

	// Get the local file path
	file_path_buff := make([]byte, 100)
	connection.Read(file_path_buff)
	local_filepath := strings.Trim(string(file_path_buff), " ")

	// Now, get the file name
	file_name_buff := make([]byte, 100)
	connection.Read(file_name_buff)
	filename := strings.Trim(string(file_name_buff), " ")

	// Determine if the file we are putting actually exists
	f, err := os.Stat(local_filepath)
	if os.IsNotExist(err) {
		fmt.Println(local_filepath, "does not exist, cant send this file")
		return
	}
	file_size := f.Size()

	Send_file_tcp(n, server_index, local_filepath, filename, file_size, "", false)
}

func in_server_list(n *net_node.Node, servers []int32) bool {
	for _, s := range servers {
		if s == int32(n.Index) {
			return true
		}
	}

	return false
}

/*
 * Delete a file from the machine
 */
func CheckAndDelete(n *net_node.Node, connection net.Conn) {
	file_name_buff := make([]byte, 100)
	connection.Read(file_name_buff)
	filename := strings.Trim(string(file_name_buff), " ")
	servers := n.Files[filename].Servers
	on_current_server := in_server_list(n, servers)

	// Update the file storage on the servers with the file
	file_size := n.Files[filename].FileSize
	for _, s := range servers {
		n.Table[s].FileMemoryUsed -= int64(file_size)
	}

	delete(n.Files, filename)

	_, err := os.Stat(filename)
	if err == nil && on_current_server {
		os.Remove(filename)
	}
	if os.IsNotExist(err) {
	}
}

/*
 * Send a file to another machine
 */
func DuplicateFile(n *net_node.Node, filename string, send_to_idx int32) {
	// First, determine if the file we are putting actually exists
	f, err := os.Stat(filename)
	if os.IsNotExist(err) {
		fmt.Println(filename, "does not exist ,cant duplicate this file")
		return
	}
	file_size := f.Size()

	// Do not begin writing until we have waited for all
	// other writes and reads on the file to finish  and notified
	// other servers that we are writing

	acquire_distributed_write_lock(n, filename)

	Send_file_tcp(n, send_to_idx, filename, filename, file_size, "", false)

	// Send a message to the remaining servers that the file has been put
	servers := n.Files[filename].Servers
	for _, idx := range servers {
		if idx == -1 {
			continue
		}
		if n.Table[idx].Status != net_node.ACTIVE {
			n.Files[filename].Servers[idx] = send_to_idx
		}
	}
	notify_servers_of_file_put_complete(n, servers, filename, file_size)
}

/*
 * Send a request to a server with a file to send one if its files to another server
 */
func send_file_dup_request(n *net_node.Node, from_server int32, to_server int32, filename string) {
	// Open a TCP connection
	remote_addr := net_node.ConvertToAddr(n.Table[from_server].Address)
	remote_tcp_addr := net_node.ConvertUDPToTCP(*remote_addr)
	conn, err := net.DialTCP("tcp", nil, remote_tcp_addr)
	net_node.CheckError(err)
	defer conn.Close()

	// Now, send over the file metadata
	to_server_str := fmt.Sprintf("%32d", to_server)
	filename_str := fmt.Sprintf("%100s", filename)
	first_line := []byte("DR" + to_server_str + filename_str)
	conn.Write(first_line)
}

/*
 *  When thereif a failure, reallocate the files on the failed server
 */
func HandleFailure(n *net_node.Node, failed_idx int) {
	// The smallest active index node handles the failure
	failure_handling_node_idx := 0
	for i, _ := range n.Table {
		if n.Table[i].Status == net_node.ACTIVE {
			failure_handling_node_idx = i
			break
		}
	}
	if uint32(failure_handling_node_idx) != n.Index {
		return
	}
	RellocateFiles(n, failed_idx)

	// Mark the unhandled_failed status as failed (handled)
	n.Table[failed_idx].Status = net_node.FAILED
}

func GetFilesOnServer(n *net_node.Node, i int32) []string {
	var s []string
	for file, servs := range n.Files {
		if n.Files[file].Writing {
			continue
		}
		if servs.Servers[0] == i || servs.Servers[1] == i || servs.Servers[2] == i || servs.Servers[3] == i {
			s = append(s, file)
		}
	}
	return s
}

/*
 * For each file on the inactive server, find the server with the lowest
 * amount of memory usage that does not have the file, and request that a server
 * with the file sends it over
 */
func RellocateFiles(n *net_node.Node, failed_idx int) {
	files := GetFilesOnServer(n, int32(failed_idx))
	// Propagate files formerly on failed machine to the nodes with lowest space used
	for _, file := range files {
		seen := make([]bool, len(n.Table))
		for i := range seen {
			seen[i] = false
		}
		servers := n.Files[file].Servers
		from_server := -1

		for _, serv := range servers {
			if serv != -1 && int(serv) != failed_idx {
				seen[serv] = true
				from_server = int(serv)
			}
		}
		// If no server has the file, return
		if from_server == -1 {
			return
		}
		seen[failed_idx] = true
		to_server := get_most_free_server(n, &seen)
		if to_server == -1 {
			fmt.Println("Could not find server to duplicate to")
			continue
		}
		if from_server == -1 {
			fmt.Println("Could not find server containing duplicate")
		}
		send_file_dup_request(n, int32(from_server), int32(to_server), file)
		// TODO remove failed node from file list

		if n.Files[file].Servers[0] == int32(failed_idx) {
			n.Files[file].Servers[0] = int32(to_server)
		}
		if n.Files[file].Servers[1] == int32(failed_idx) {
			n.Files[file].Servers[1] = int32(to_server)
		}
		if n.Files[file].Servers[2] == int32(failed_idx) {
			n.Files[file].Servers[2] = int32(to_server)
		}
		if n.Files[file].Servers[3] == int32(failed_idx) {
			n.Files[file].Servers[3] = int32(to_server)
		}
		n.Files[file].LastTime = timestamppb.Now()
	}
}

/*
 * Control function for the delete command
 */
func DeleteFile(n *net_node.Node, filename string) {
	if _, ok := n.Files[filename]; ok {
	} else {
		fmt.Println(filename, "does not exist in system")
	}

	// A delete is a write operation
	acquire_distributed_write_lock(n, filename)

	// Delete the file from the current server
	servers := n.Files[filename].Servers
	on_current_server := in_server_list(n, servers)

	// Update the file storage on the servers with the file
	file_size := n.Files[filename].FileSize
	for _, s := range servers {
		n.Table[s].FileMemoryUsed -= int64(file_size)
	}

	delete(n.Files, filename)

	_, err := os.Stat(filename)
	if err == nil && on_current_server {
		os.Remove(filename)
	}
	if os.IsNotExist(err) {
	}

	// Notify  the remaining servers
	filename_str := fmt.Sprintf("%100s", filename)
	msg := []byte("D_" + filename_str)
	for i := 0; i < len(n.Table); i++ {
		if i == int(n.Index) {
			continue
		}
		addr := net_node.ConvertToAddr(n.Table[i].Address)
		net_node.SendMsgTCP(addr, msg)
	}
}

/*
 * Control function for the put command
 */
func PutFile(n *net_node.Node, local_filepath string, filename string) {
	// First, determine if the file we are putting actually exists

	// debug
	//fmt.Println("Enter with: " + local_filepath)

	f, err := os.Stat(local_filepath)
	// debug
	if err != nil {
		fmt.Println(err)
	}

	if os.IsNotExist(err) {
		fmt.Println(local_filepath, "does not exist, cant put this file")
		return
	}
	file_size := f.Size()
	// debug
	//fmt.Println("File size: ", file_size)

	// Determine if we are putting a new file or updating an existing one
	in_fs := file_in_filesystem(n, filename)
	// debug
	if !in_fs {
		fmt.Println("file not in sytem")
	}

	// Do not begin writing until we have waited for all
	// other writes and reads on the file to finish  and notified
	// other servers that we are writing

	acquire_distributed_write_lock(n, filename)

	// debug
	//fmt.Println("acquire lock")

	// Determine if the file is already in the file system, we simply need to update
	// it. Otherwise, we need to create a new file
	var servers []int32
	if in_fs {
		servers = n.Files[filename].Servers
	} else {
		servers = get_four_most_free_servers(n)
	}
	// debug
	fmt.Println("Send file to  ", servers)
	send_file_to_servers(n, servers, local_filepath, filename, file_size)

	// Send a message to the remaining servers that the file has been put
	notify_servers_of_file_put_complete(n, servers, filename, file_size)
}

/*
 * Control function for the get command
 */
func GetFile(n *net_node.Node, filename string, local_filepath string) {
	// Check if the file exists
	if _, ok := n.Files[filename]; !ok {
		fmt.Println(filename, "does not exist, cant get this file")
		return
	}

	// If we are currently writing, wait until the write is done
	acquire_distributed_read_lock(n, filename)

	servers_with_file := n.Files[filename].Servers

	for _, server := range servers_with_file {
		// If the current server has it, use that
		if n.Index == uint32(server) {
			sourceFile, err := os.Open(filename)
			net_node.CheckError(err)
			defer sourceFile.Close()

			newFile, err := os.Create(local_filepath)
			net_node.CheckError(err)
			defer newFile.Close()

			_, err = io.Copy(newFile, sourceFile)
			net_node.CheckError(err)
			return
		}
	}

	server := servers_with_file[0]

	// Open a TCP connection
	remote_addr := net_node.ConvertToAddr(n.Table[server].Address)
	remote_tcp_addr := net_node.ConvertUDPToTCP(*remote_addr)
	conn, err := net.DialTCP("tcp", nil, remote_tcp_addr)
	net_node.CheckError(err)
	defer conn.Close()

	// Now, send over the file metadata
	index_str := fmt.Sprintf("%32d", n.Index)
	file_path_str := fmt.Sprintf("%100s", filename)
	filename_str := fmt.Sprintf("%100s", local_filepath)
	first_line := []byte("G_" + index_str + file_path_str + filename_str)
	conn.Write(first_line)
}

/*
server get file using server index
*/
func GetFileWithIndex(n *net_node.Node, filename string, local_filepath string, serverIndex int) {
	// Open a TCP connection
	remote_addr := net_node.ConvertToAddr(n.Table[serverIndex].Address)
	remote_tcp_addr := net_node.ConvertUDPToTCP(*remote_addr)
	conn, err := net.DialTCP("tcp", nil, remote_tcp_addr)
	net_node.CheckError(err)
	defer conn.Close()

	// Now, send over the file metadata
	index_str := fmt.Sprintf("%32d", n.Index)
	file_path_str := fmt.Sprintf("%100s", filename)
	filename_str := fmt.Sprintf("%100s", local_filepath)
	first_line := []byte("G_" + index_str + file_path_str + filename_str)
	conn.Write(first_line)
}

// define mutex writer
type SyncWriter struct {
	m      sync.Mutex
	Writer io.Writer
}

func (w *SyncWriter) Write(b []byte) (n int, err error) {
	w.m.Lock()
	defer w.m.Unlock()
	return w.Writer.Write(b)
}

var WRITERMAP = make(map[string]*SyncWriter)

/*
	get local mutex writer for sync-writing
*/
func GetLocalSyncWriter(filename string) *SyncWriter {
	if x, found := WRITERMAP[filename]; found {
		return x
	}
	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		f, err := os.Create(filename)
		if err != nil {
			fmt.Println("create error")
		}
		fmt.Println("create", filename)
		WRITERMAP[filename] = &SyncWriter{sync.Mutex{}, f}
	} else {
		f, err := os.Open(filename)
		if err != nil {
			fmt.Println("open error")
		}
		WRITERMAP[filename] = &SyncWriter{sync.Mutex{}, f}
	}
	return WRITERMAP[filename]
}

// After Receiving a sdfs_intermediate_file, do the append
func CreatAppendSdfsKeyFile(filename string) {
	var append_target_filename string
	nameTemp := strings.Split(filename, "_")
	append_target_filename = strings.Join(nameTemp[:len(nameTemp)-1], "_")
	//for i := len(filename) - 1; i >= 0; i-- {
	//	if filename[i] == '_' { //extra for reduce append
	//		append_target_filename = filename[0:i]
	//		break
	//		//}
	//	}
	//}
	// _, err := os.Stat(append_target_filename)
	// if os.IsNotExist(err) {
	// 	f, err := os.Create(append_target_filename)
	// 	of_map[append_target_filename] = f
	// } else {
	// 	f, err := os.Open(append_target_filename)
	// 	of_map[append_target_filename] = f
	// }
	// f := of_map[append_target_filename]
	writer := GetLocalSyncWriter(append_target_filename)
	ifstream, err := os.Open(filename)
	if err != nil {
		fmt.Println("Can not open the MapleTask input file!")
	}
	input := bufio.NewScanner(ifstream)
	wg := sync.WaitGroup{}
	for {
		if !input.Scan() {
			break
		}
		line := input.Text()
		wg.Add(1)
		go func(message string) {
			fmt.Fprintln(writer, message)
			wg.Done()
		}(line)

		if err != nil {
			log.Println(err)
			return
		}
	}
	wg.Wait()
}

// // After Receiving a sdfsPrefix_key_reduce file, do the append
// func AppendReduceFile(filename string, DestFileName string){
// 	var target_reduce_result string
// }
func CreatAppendSdfsReduceFile(filename string) {
	var DestFileName string
	nameTemp := strings.Split(filename, "_")
	DestFileName = nameTemp[0]
	//for i := 0; i < len(filename) 0; i++ {
	//	if filename[i] == '_' { //extra for reduce append
	//		DestFileName = filename[0:i]
	//	}
	//}
	// _, err := os.Stat(DestFileName)
	// if os.IsNotExist(err) {
	// 	f, err := os.Create(DestFileName)
	// 	of_map[DestFileName] = f
	// } else {
	// 	f, err := os.Open(DestFileName)
	// 	of_map[DestFileName] = f
	// }
	// f := of_map[DestFileName]
	writer := GetLocalSyncWriter(DestFileName)
	ifstream, err := os.Open(filename)
	if err != nil {
		fmt.Println("Can not open the MapleTask input file!")
	}
	input := bufio.NewScanner(ifstream)
	wg := sync.WaitGroup{}
	for {
		if !input.Scan() {
			break
		}
		line := input.Text()
		wg.Add(1)
		go func(message string) {
			fmt.Fprintln(writer, message)
			wg.Done()
		}(line)

		if err != nil {
			log.Println(err)
			return
		}
	}
	wg.Wait()
}

// when node receive maple-end command
// it will put the local file into sdfs directory
func PutIntermediateFile(node *net_node.Node, connection net.Conn) {
	sdfs_prefix_buff := make([]byte, 100)
	connection.Read(sdfs_prefix_buff)
	sdfs_prefix := strings.Trim(string(sdfs_prefix_buff), " ")

	if len(sdfs_prefix) == 0 {
		fmt.Println("Can't get the sdfs prefix")
	}
	files, _ := ioutil.ReadDir("./")
	// debug
	//for _, f := range files {
	//	filename := f.Name()
	//	fmt.Println(filename)
	//}

	for _, f := range files {
		fileName := f.Name()
		if len(fileName) == 0 {
			continue
		}
		tempList := strings.Split(fileName, "_")
		prefixString := strings.Join(tempList[:len(tempList)-1], "_")
		if len(tempList) < 5 && strings.Compare(prefixString, config.MAPLEFILEPREFIX) == 0 {
			// put file into sdfs directory
			//fmt.Println("Find match prefix " + prefixString + " filename is " + fileName)
			fmt.Println("Beginning put  " + fileName + "  as " + sdfs_prefix + "_" + tempList[len(tempList)-1])
			fmt.Println("File size:  ", f.Size())
			go PutFile(node, fileName, sdfs_prefix+"_"+tempList[len(tempList)-1])
			//time.Sleep(config.GETFILEWAIT)
		}
	}

}
