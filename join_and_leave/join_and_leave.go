package join_and_leave

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"mp3/net_node"
	pings "mp3/ping_protobuff"
)

func AddNewNodeToTable(n *net_node.Node, msg *pings.TableMessasgeProto) {
	index := msg.Table[0].Index
	new_table_length := int(index) + 1
	if len(n.Table) > int(index) {
		new_table_length = len(n.Table)
	}
	table := make([]*pings.TableEntryProto, new_table_length)

	// Copy the existing entries
	for i, _ := range n.Table {
		table[i] = n.Table[i]
	}

	// Add the new entry
	table[index] = msg.Table[0]

	n.Table = table
}

func MarkNodeLeft(n *net_node.Node, msg *pings.TableMessasgeProto) {
	index := msg.Sender.Index
	n.Table[index].Status = net_node.LEFT
}

func create_intro_node() *net_node.Node {
	// By default, gossip is set to true
	address := net.UDPAddr{IP: net_node.INTRO_IP, Port: net_node.INTRO_PORT, Zone: ""}
	creation_time := timestamppb.Now()
	node := &net_node.Node{
		Table:     make([]*pings.TableEntryProto, 1),
		Address:   address,
		Index:     0,
		Join_time: creation_time,
		Gossip:    true,
		Status:    net_node.ACTIVE,
		Files:     map[string]*pings.FileMetaDataProto{},
	}

	// Initialize the node's table
	node.Table[0] = &pings.TableEntryProto{
		Address: &pings.UDPAddressProto{
			Ip:   address.IP,
			Port: int32(net_node.INTRO_PORT),
			Zone: "",
		},
		JoinTime:       creation_time,
		LastTime:       creation_time,
		Status:         net_node.ACTIVE,
		Index:          0,
		FileMemoryUsed: 0,
	}

	fmt.Println("Starting intro node on IP", node.Address.IP, "and port", node.Address.Port)

	return node
}

func send_new_node_msg_to_introducer(local_address *net.UDPAddr, intro_address *net.UDPAddr) {
	creation_time := timestamppb.Now()
	msg := &pings.TableMessasgeProto{
		MsgType: net_node.JOINING,
		Sender: &pings.TableEntryProto{
			Address: &pings.UDPAddressProto{
				Ip:   local_address.IP,
				Port: int32(local_address.Port),
				Zone: "",
			},
			JoinTime:       creation_time,
			LastTime:       creation_time,
			Status:         net_node.ACTIVE,
			FileMemoryUsed: 0,
		},
	}
	net_node.SendMsgUDP(intro_address, msg)
}

func receive_onboarding_msg(address *net.UDPAddr) *pings.TableMessasgeProto {
	ServerConn, err := net.ListenUDP("udp", address)
	defer ServerConn.Close()
	net_node.CheckError(err)

	buf := make([]byte, 1024)

	size, _, err := ServerConn.ReadFromUDP(buf)
	net_node.CheckError(err)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	msg := &pings.TableMessasgeProto{}
	err = proto.Unmarshal(buf[0:size], msg)
	net_node.CheckError(err)

	return msg
}

func create_normal_node(new_port int) *net_node.Node {
	intro_address := &net.UDPAddr{IP: net_node.INTRO_IP, Port: net_node.INTRO_PORT, Zone: ""}
	local_address := &net.UDPAddr{IP: []byte{0, 0, 0, 0}, Port: new_port, Zone: ""}

	// Communicate with introducer node
	send_new_node_msg_to_introducer(local_address, intro_address)
	onboarding_msg := receive_onboarding_msg(local_address)
	if onboarding_msg == nil {
		return nil
	}

	index := 0
	for i, _ := range onboarding_msg.Table {
		onboarding_msg.Table[i].LastTime = timestamppb.Now()
		if onboarding_msg.Table[i].Status == net_node.ONBOARDING {
			index = i
			onboarding_msg.Table[i].Status = net_node.ACTIVE
		}
	}

	// Finally, create the node
	node := &net_node.Node{
		Table:     onboarding_msg.Table,
		Address:   *net_node.ConvertToAddr(onboarding_msg.Table[index].Address),
		Index:     uint32(index),
		Join_time: onboarding_msg.Sender.LastTime,
		Gossip:    onboarding_msg.Gossip,
		Status:    net_node.ACTIVE,
		Files:     onboarding_msg.Files,
	}

	fmt.Println("Starting normal node on IP", node.Address.IP, "and port", node.Address.Port)

	return node
}

func OnboardNewNode(n *net_node.Node, msg *pings.TableMessasgeProto) {
	// Find the first index in the table without an active node
	index := 0
	for i, _ := range n.Table {
		if i == 0 {
			continue
		}

		if n.Table[i].Status == net_node.FAILED || n.Table[i].Status == net_node.LEFT {
			index = i
			break
		}
	}

	if index == 0 {
		index = len(n.Table)
	}

	// Add the new entry to the table
	if index == len(n.Table) {
		n.Table = append(n.Table, &pings.TableEntryProto{
			Address:        msg.Sender.Address,
			JoinTime:       msg.Sender.JoinTime,
			LastTime:       msg.Sender.LastTime,
			Status:         net_node.ONBOARDING,
			Index:          uint32(index),
			FileMemoryUsed: msg.Sender.FileMemoryUsed,
		})
	} else {
		n.Table[index] = &pings.TableEntryProto{
			Address:        msg.Sender.Address,
			JoinTime:       msg.Sender.JoinTime,
			LastTime:       msg.Sender.LastTime,
			Status:         net_node.ONBOARDING,
			Index:          uint32(index),
			FileMemoryUsed: msg.Sender.FileMemoryUsed,
		}
	}

	// Send the table over to the new node
	n.SendTableUDP(net_node.ConvertToAddr(msg.Sender.Address))

	// Update status now to active
	n.Table[index].Status = net_node.ACTIVE

	// Send the information about the new node over to all of the other nodes
	joined_msg := &pings.TableMessasgeProto{MsgType: net_node.JOINED}
	joined_msg.Table = []*pings.TableEntryProto{n.Table[index]}
	for i := 1; i < len(n.Table); i++ {
		if i != index && n.Table[i].Status == net_node.ACTIVE {
			addr := net_node.ConvertToAddr(n.Table[i].Address)
			net_node.SendMsgUDP(addr, joined_msg)
		}
	}
}

/*
*   CONTROL FUNCTION FOR JOINING A NEW NODE
 */
func ExecuteJoin(args []string) *net_node.Node {
	if strings.Compare("intro", args[0]) == 0 {
		return create_intro_node()
	} else {
		i64, _ := strconv.ParseInt(args[0], 10, 0)
		new_port := int(i64)
		return create_normal_node(new_port)
	}
}
