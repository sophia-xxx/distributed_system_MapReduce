package run_server

import (
	"bufio"
	"fmt"
	"log"
	"mp3/config"
	"mp3/maple_juice"
	"net"
	"os"
	"strconv"
	"strings"

	"mp3/detect_failures"
	"mp3/file_system"
	"mp3/join_and_leave"
	"mp3/net_node"
	pings "mp3/ping_protobuff"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func run_udp(n *net_node.Node) {
	UDPConn, err := net.ListenUDP("udp", &n.Address)
	defer UDPConn.Close()
	if err != nil {
		log.Println(n.Address.Port, err)
		fmt.Println("Can't run UDP listening")
		return
	}
	log.Println(n.Address, "Running")

	buf := make([]byte, 2048)

	go detect_failures.PingNodes(n)
	go detect_failures.DetectFailures(n)

	for {
		// If the leave command was entered and nodes have already been pinged
		// about this, stop running the server
		if n.Status == net_node.LEFT {
			break
		}

		size, addr, err := UDPConn.ReadFromUDP(buf)
		n.CheckError(err)

		msg := &pings.TableMessasgeProto{}
		err = proto.Unmarshal(buf[0:size], msg)
		n.CheckError(err)

		switch msg.MsgType {
		case net_node.JOINING:
			log.Println(n.Address, "Node Onboarded:", msg.Sender.Address)
			msg.Sender.Address.Ip = addr.IP
			join_and_leave.OnboardNewNode(n, msg)
		case net_node.JOINED:
			log.Println(n.Address, "Node Joined:", msg.Table[0].Address)
			join_and_leave.AddNewNodeToTable(n, msg)
		case net_node.NODE_LEAVING:
			log.Println(n.Address, "Node Left:", msg.Sender.Address)
			join_and_leave.MarkNodeLeft(n, msg)
		case net_node.GOSSIP:
			detect_failures.MergeTables(n, msg)
			//TODO impl:
			file_system.MergeFileList(n, msg)
		case net_node.HEARTBEAT:
			// There is an edge case where the new node has not yet
			// been added to the table
			// Handle this
			if msg.Sender.Index < uint32(len(n.Table)) {
				n.Table[msg.Sender.Index].LastTime = timestamppb.Now()
				n.Table[msg.Sender.Index].Status = net_node.ACTIVE
			}
		case net_node.SWITCH_TO_GOSSIP:
			n.Gossip = true
		case net_node.SWITCH_TO_ALL_TO_ALL:
			n.Gossip = false
		}
	}
}

func run_tcp(n *net_node.Node) {
	tcp_addr := net_node.ConvertUDPToTCP(n.Address)
	TCPConn, err := net.ListenTCP("tcp", tcp_addr)
	defer TCPConn.Close()
	if err != nil {
		log.Println(n.Address.Port, err)
		fmt.Println("Can't start TCP listening")
		return
	}
	log.Println(n.Address, "Running")

	for {
		// If the leave command was entered and nodes have already been pinged
		// about this, stop running the server
		if n.Status == net_node.LEFT {
			break
		}

		connection, err := TCPConn.Accept()
		if err != nil {
			log.Println(n.Address.Port, err)
		}

		verb := make([]byte, 2)
		connection.Read(verb)
		verb_str := string(verb)

		switch verb_str {
		// Notification that write will start
		case "WS":
			file_system.RespondToWriteStartMsg(n, connection)

		// Acknowledgement that write will start
		case "WA":
			file_system.ReceiveWriteStrMsgResponse(n, connection)

		// Putting the file
		case "P_":
			fmt.Printf("RECEIVING FILE: ")
			file_system.ReceiveFile(connection, false)

		// Putting the file that required to be appended by mapleJuice
		case "PA":
			fmt.Printf("RECEIVING FILE(Needed appebd): ")
			file_system.ReceiveFile(connection, true)

		// Acknowledgeing that write has ended
		case "WE":
			file_system.ReceiveFileWriteCompleteMsg(n, connection)

		// Notification that read will start
		case "RS":
			file_system.RespondToReadStartMsg(n, connection)

		// Acknowledgement that read will start
		case "RA":
			file_system.ReceiveReadStrMsgResponse(n, connection)

		// Getting file
		case "G_":
			file_system.SendFile(n, connection)

		// Acknowledgeing that write has ended
		case "RE":
			file_system.ReceiveFileReadCompleteMsg(n, connection)

		// Requesting that a file be duplicated to another server
		case "DR":
			fmt.Println("RECIEVING DR")
			send_to_idx_buff := make([]byte, 32)
			connection.Read(send_to_idx_buff)
			send_to_idx_str := strings.Trim(string(send_to_idx_buff), " ")
			send_to_idx, _ := strconv.ParseInt(send_to_idx_str, 10, 32)

			// Now, get the file name
			file_name_buff := make([]byte, 100)
			connection.Read(file_name_buff)
			filename := strings.Trim(string(file_name_buff), " ")
			go file_system.DuplicateFile(n, filename, int32(send_to_idx))

		// Delete command
		case "D_":
			file_system.CheckAndDelete(n, connection)

		// maple end command
		case "ED":
			fmt.Println("Receive Maple end message, begin put the maple intermediate file to sdfs!")
			file_system.PutIntermediateFile(n, connection)
		}

		connection.Close()
	}
}

func run_server(n *net_node.Node) {
	go run_udp(n)
	go run_tcp(n)
}

/*
 *   Command line interface
 */
func printCLIHelp() {
	fmt.Println("Valid Commands:")
	fmt.Println("(if not joined) join intro")
	fmt.Println("(if not joined) join [port_number]")
	fmt.Println("(if joined) leave")
	fmt.Println("(if joined) members")
	fmt.Println("(if joined) id")
	fmt.Println("(if joined) gossip")
	fmt.Println("(if joined) all-to-all")
	fmt.Println("(if joined) put [filepath]")
	fmt.Println("(if joined) get [filename]")
	fmt.Println("(if joined) delete [filename]")
	fmt.Println("(if joined) ls [filename]")
	fmt.Println("(if joined) store")
	fmt.Println("(all scenarios) exit")
}

func execute_join_command(node *net_node.Node, args []string, node_active bool, server *maple_juice.Server, master *maple_juice.Master) *net_node.Node {
	if len(args) != 2 || node_active {
		printCLIHelp()
		return nil
	} else {
		node = join_and_leave.ExecuteJoin(args[1:])
		node.Initialize()
		initMapleJuice(server, master, node)
		log.Println(node.Address, "Joining")
		go run_server(node)
		return node
	}
}

func execute_leave_command(node *net_node.Node, args []string, node_active bool) {
	log.Println(node.Address, "Leaving")

	if len(args) != 1 || !node_active {
		printCLIHelp()
	} else {
		node.Status = net_node.LEAVING
		file_system.RellocateFiles(node, int(node.Index))
	}
}

func execute_id_command(node *net_node.Node, args []string, node_active bool) {
	if len(args) != 1 || !node_active {
		printCLIHelp()
	} else {
		fmt.Println(
			node.Address.IP.String() + ":" + strconv.Itoa(node.Address.Port) + "-" +
				node.Join_time.AsTime().Format("15:04:05"),
		)
		fmt.Println(node.Index, ":Index")
	}
}

func execute_members_command(node *net_node.Node, args []string, node_active bool) {
	if len(args) != 1 || node == nil {
		printCLIHelp()
	} else {
		for _, row := range node.Table {
			ip_string := net_node.ConvertToAddr(row.Address).IP.String() + ":" + strconv.Itoa(node.Address.Port)
			join_time_string := row.JoinTime.AsTime().Format("15:04:05")
			status_string := net_node.StatusString(row.Status)
			fmt.Println("Index:", row.Index, "; ID:", ip_string+join_time_string+"; Status:", status_string)
		}
	}
}

func execute_gossip_command(node *net_node.Node, args []string, node_active bool) {
	if len(args) != 1 || node == nil {
		printCLIHelp()
	} else if !node.Gossip {
		detect_failures.SwitchMode(node)
	} else {
		fmt.Println("already in gossip mode")
	}
}

func execute_all_to_all_command(node *net_node.Node, args []string, node_active bool) {
	if len(args) != 1 || node == nil {
		printCLIHelp()
	} else if node.Gossip {
		detect_failures.SwitchMode(node)
	} else {
		fmt.Println("already in all-to-all mode")
	}
}

func execute_put_command(node *net_node.Node, args []string, node_active bool) {
	if len(args) != 3 || node == nil {
		printCLIHelp()
	} else {
		go file_system.PutFile(node, args[1], args[2])
	}
}

func execute_get_command(node *net_node.Node, args []string, node_active bool) {
	if len(args) != 3 || node == nil {
		printCLIHelp()
	} else {
		go file_system.GetFile(node, args[1], args[2])
	}
}

func execute_delete_command(node *net_node.Node, args []string, node_active bool) {
	if len(args) != 2 || node == nil {
		printCLIHelp()
	} else {
		go file_system.DeleteFile(node, args[1])
	}
}

func execute_ls_command(node *net_node.Node, args []string, node_active bool) {
	if len(args) != 2 || node == nil {
		printCLIHelp()
	} else {
		file_system.ListServersWithFile(node, args[1])
		// TODO: Implement the ls command
	}
}

func execute_store_command(node *net_node.Node, args []string, node_active bool) {
	if len(args) != 1 || node == nil {
		printCLIHelp()
	} else {
		file_system.ListFilesOnServer(node)
		// TODO: Implement the store command
	}
}

// maple <maple_exe> <mapleNum> <sdfs_prefix> <sdfs_src_file>
func excute_maple_command(node *net_node.Node, args []string) {
	//initMapleJuice(server, master, node)
	mapleNum, _ := strconv.Atoi(args[2])
	maple_juice.CallMaple(node, args[0], args[1], mapleNum, args[3], args[4])
}

func excute_juice_command(node *net_node.Node, args []string) {
	//juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1(delete)} partition="hash"/"range"
	juiceNum, _ := strconv.Atoi(args[2])
	maple_juice.CallJuice(node, args[0], args[1], juiceNum, args[3], args[4], args[5], args[6])
}

func CLI() {
	reader := bufio.NewReader(os.Stdin)

	var node *net_node.Node
	var server *maple_juice.Server
	var master *maple_juice.Master

	for {
		fmt.Print("-> ")
		text, _ := reader.ReadString('\n')

		// convert CRLF to LF
		text = strings.Replace(text, "\n", "", -1)

		// Split the string into nodes
		args := strings.Fields(text)

		node_active := node != nil && node.Status == net_node.ACTIVE

		// Execute commands
		switch {
		// Whitespace entered
		case len(args) == 0:
			printCLIHelp()

		// Join command
		case strings.Compare(args[0], "join") == 0:
			node = execute_join_command(node, args, node_active, server, master)

		// Leave command
		case strings.Compare(args[0], "leave") == 0:
			execute_leave_command(node, args, node_active)

		// Show ID
		case strings.Compare(args[0], "id") == 0:
			execute_id_command(node, args, node_active)

		// List members
		case strings.Compare(args[0], "members") == 0:
			execute_members_command(node, args, node_active)

		// Switch to Gossip for failure detection
		case strings.Compare(args[0], "gossip") == 0:
			execute_gossip_command(node, args, node_active)

		// Switch to all-to-all for failure detection
		case strings.Compare(args[0], "all-to-all") == 0:
			execute_all_to_all_command(node, args, node_active)

		// Put file
		case strings.Compare(args[0], "put") == 0:
			execute_put_command(node, args, node_active)

		// Get file
		case strings.Compare(args[0], "get") == 0:
			execute_get_command(node, args, node_active)

		// Delete file
		case strings.Compare(args[0], "delete") == 0:
			execute_delete_command(node, args, node_active)

		// List the servers that contain a given file
		case strings.Compare(args[0], "ls") == 0:
			execute_ls_command(node, args, node_active)

		// List all the files currently stored on a machine
		case strings.Compare(args[0], "store") == 0:
			execute_store_command(node, args, node_active)

		// Exit program
		case strings.Compare(args[0], "exit") == 0:
			os.Exit(0)

		// MapleJuice
		case strings.Compare(args[0], "maple") == 0:
			excute_maple_command(node, args)

		case strings.Compare(args[0], "juice") == 0:
			excute_juice_command(node, args)

		// Invalid command
		default:
			printCLIHelp()
		}
	}
}

func initMapleJuice(server *maple_juice.Server, master *maple_juice.Master, node *net_node.Node) {
	server = maple_juice.NewMapleServer(node)
	master = maple_juice.NewMaster(node)
	if strings.Compare(maple_juice.ChangeIPtoString(node.Address.IP), config.MASTERIP) == 0 {
		go maple_juice.StartMasterRpc(master)
	} else {
		go maple_juice.StartServerRPC(server)
	}
}
