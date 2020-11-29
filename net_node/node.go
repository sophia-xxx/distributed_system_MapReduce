package net_node

import (
	"log"
	"net"

	pings "mp3/ping_protobuff"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Node Statuses
var ACTIVE int32 = 1
var LEAVING int32 = 2
var LEFT int32 = 3
var FAILED int32 = 4
var FAILED_UNHANDLED int32 = 5
var ONBOARDING int32 = 6

// Types of messages
var JOINING int32 = 5
var JOINED int32 = 6
var NODE_LEAVING int32 = 7
var FAIL int32 = 8
var GOSSIP int32 = 9
var HEARTBEAT int32 = 10
var SEND_TABLE int32 = 11
var SWITCH_TO_GOSSIP int32 = 12
var SWITCH_TO_ALL_TO_ALL int32 = 13
var FILE_PUT int32 = 14
var FILE_DELETE int32 = 15
var BLOCKING int32 = 16
var UNBLOCKING int32 = 16

var INTRO_PORT = 9017
var INTRO_IP = []byte{172, 22, 156, 22} // 172.22.156.22   g07-01

type Node struct {
	Table     []*pings.TableEntryProto
	Address   net.UDPAddr // Note that the UDP and TCP address types are the same
	Index     uint32
	Join_time *timestamppb.Timestamp
	Gossip    bool
	Status    int32
	Files     map[string]*pings.FileMetaDataProto
	Blocked   bool
}

/*
 *   INITIALIZATION
 */
func (n *Node) Initialize() {
	n.Files = make(map[string]*pings.FileMetaDataProto)
}

/*
 *   BASIC NODE UTILS
 */
func StatusString(Status int32) string {
	switch {
	case Status == ACTIVE:
		return "ACTIVE"
	case Status == LEFT:
		return "LEFT"
	case Status == LEAVING:
		return "LEAVING"
	case Status == FAILED:
		return "FAILED"
	default:
		return ""
	}
}

func GetEntry(n *Node) *pings.TableEntryProto {
	tableEntry := n.Table[n.Index]

	protoTableEntry := &pings.TableEntryProto{
		Address:        convert_to_proto_addr(n.Address),
		JoinTime:       tableEntry.JoinTime,
		LastTime:       tableEntry.LastTime,
		Status:         tableEntry.Status,
		Index:          tableEntry.Index,
		FileMemoryUsed: tableEntry.FileMemoryUsed,
	}

	return protoTableEntry
}

func NumActiveServ(n *Node) int {
	ans := 0
	for i := range n.Table {
		if n.Table[i].Status == ACTIVE {
			ans = ans + 1
		}
	}
	return ans
}

func convert_to_proto_addr(addr net.UDPAddr) *pings.UDPAddressProto {
	addrProto := &pings.UDPAddressProto{
		Ip:   addr.IP,
		Port: int32(addr.Port),
		Zone: addr.Zone,
	}

	return addrProto
}

func ConvertToAddr(addrProto *pings.UDPAddressProto) *net.UDPAddr {
	addr := net.UDPAddr{IP: addrProto.Ip, Port: int(addrProto.Port), Zone: ""}
	return &addr
}

func ConvertUDPToTCP(addr net.UDPAddr) *net.TCPAddr {
	tcp_addr := net.TCPAddr{
		IP:   addr.IP,
		Port: addr.Port,
		Zone: addr.Zone,
	}
	return &tcp_addr
}

/*
 * Message sending utils
 */
func SendMsgTCP(remote_addr *net.UDPAddr, msg []byte) {
	remote_tcp_addr := ConvertUDPToTCP(*remote_addr)
	conn, err := net.DialTCP("tcp", nil, remote_tcp_addr)
	CheckError(err)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.Write(msg)
}

func SendMsgUDP(remoteAddr *net.UDPAddr, msg *pings.TableMessasgeProto) {
	conn, err := net.DialUDP("udp", nil, remoteAddr)
	CheckError(err)
	defer conn.Close()

	data, err := proto.Marshal(msg)
	CheckError(err)

	buf := []byte(data)
	_, err = conn.Write(buf)
	CheckError(err)
}

func (n *Node) SendTableUDP(addr *net.UDPAddr) {
	// Create message object
	msg := &pings.TableMessasgeProto{
		MsgType: SEND_TABLE,
		Sender:  GetEntry(n),
		Table:   n.Table,
		Files:   n.Files,
		Gossip:  n.Gossip,
	}

	// Call sendmsg
	SendMsgUDP(addr, msg)
}

/*
 *   ERROR LOGGING UTILS
 */
func (n *Node) CheckError(err error) {
	if err != nil {
		log.Println(n.Address, err)
	}
}
func CheckError(err error) {
	if err != nil {
		log.Println(err)
	}
}
