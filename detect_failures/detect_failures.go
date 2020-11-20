package detect_failures

import (
	"log"
	"math/rand"
	"net"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"mp3/file_system"
	"mp3/net_node"
	pings "mp3/ping_protobuff"
)

/*
 *   SENDING MESSAGES AND PINGS
 */
func send_new_node_entry(n *net_node.Node, addr *net.UDPAddr, index int) {
	msg := &pings.TableMessasgeProto{MsgType: net_node.JOINED}
	msg.Table = []*pings.TableEntryProto{n.Table[index]}
	net_node.SendMsgUDP(addr, msg)
}

func send_gossip_ping(n *net_node.Node, entry *pings.TableEntryProto, leaving bool) {
	if leaving {
		n.Table[n.Index].Status = net_node.LEFT
		n.Table[n.Index].LastTime = timestamppb.Now()
	}

	msg := &pings.TableMessasgeProto{
		MsgType: net_node.GOSSIP,
		Sender:  net_node.GetEntry(n),
		Table:   n.Table,
		Files:   n.Files,
	}
	addr := net_node.ConvertToAddr(entry.Address)
	net_node.SendMsgUDP(addr, msg)
}

func ping_nodes_gossip(n *net_node.Node, leaving bool) {
	indices := make([]uint32, len(n.Table))
	for i := 0; i < len(n.Table); i++ {
		indices[i] = uint32(i)
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(indices), func(i, j int) { indices[i], indices[j] = indices[j], indices[i] })

	i := 0
	num_pinged := 0
	for num_pinged < 5 {
		if i >= len(indices) {
			break
		}

		index := indices[i]
		i += 1

		if index == n.Index || n.Table[index].Status != net_node.ACTIVE {
			continue
		} else {
			send_gossip_ping(n, n.Table[index], leaving)
			num_pinged += 1
		}

	}
}

func send_all_to_all_ping(n *net_node.Node, entry *pings.TableEntryProto, leaving bool) {
	// One source of false positives is that the leaving pings might fail in all-to-all heartbeating,
	// resulting in other nodes not knowing the node has left
	var msg *pings.TableMessasgeProto
	if leaving {
		msg = &pings.TableMessasgeProto{MsgType: net_node.NODE_LEAVING, Sender: net_node.GetEntry(n)}
	} else {
		msg = &pings.TableMessasgeProto{MsgType: net_node.HEARTBEAT, Sender: net_node.GetEntry(n)}
	}

	addr := net_node.ConvertToAddr(entry.Address)
	net_node.SendMsgUDP(addr, msg)
}

func ping_nodes_all_to_all(n *net_node.Node, leaving bool) {
	for i, _ := range n.Table {
		if uint32(i) == n.Index || (n.Table[i].Status != net_node.ACTIVE) {
			continue
		}

		send_all_to_all_ping(n, n.Table[i], leaving)
	}
}

func ping_nodes_leaving(n *net_node.Node) {
	if n.Gossip {
		ping_nodes_gossip(n, true)
	} else {
		ping_nodes_all_to_all(n, true)
	}
}

/*
*   PROCESSING GOSSIP PINGS
 */
func MergeTables(n *net_node.Node, msg *pings.TableMessasgeProto) {
	if msg.Sender.Index < uint32(len(n.Table)) {
		if msg.Sender.Status == net_node.LEFT {
			n.Table[msg.Sender.Index].Status = net_node.LEFT
			log.Println(n.Address, "Node Left:", msg.Sender.Address)
		} else {
			n.Table[msg.Sender.Index].Status = net_node.ACTIVE
		}

		n.Table[msg.Sender.Index].LastTime = timestamppb.Now()
	}

	for i, x := range msg.Table {
		if uint32(i) == n.Index {
			continue
		}

		if i >= len(n.Table) {
			break
		}

		if n.Table[i].LastTime.AsTime().Before(x.LastTime.AsTime()) {
			n.Table[i] = x
			n.Table[i].LastTime = x.LastTime
		}

		if n.Table[i].Status == net_node.FAILED_UNHANDLED {
			file_system.HandleFailure(n, i)
		}
	}
}

/*
*   CONTROL FUNCTION SWITCHING BETWEEN GOSSIP AND ALL-TO-ALL
 */
func SwitchMode(n *net_node.Node) {
	n.Gossip = !n.Gossip

	msg := &pings.TableMessasgeProto{}
	if n.Gossip {
		msg.MsgType = net_node.SWITCH_TO_GOSSIP
	} else {
		msg.MsgType = net_node.SWITCH_TO_ALL_TO_ALL
	}

	for i, _ := range n.Table {
		if uint32(i) == n.Index {
			continue
		}
		addr := net_node.ConvertToAddr(n.Table[i].Address)
		net_node.SendMsgUDP(addr, msg)
	}
}

/*
*   CONTROL FUNCTION FOR PINGING
 */
func PingNodes(n *net_node.Node) {
	keep_iterating := true
	for keep_iterating {
		time.Sleep(1 * time.Second)

		if n.Status == net_node.LEAVING {
			keep_iterating = false
			ping_nodes_leaving(n)
			n.Status = net_node.LEFT
		} else if n.Gossip {
			ping_nodes_gossip(n, false)
		} else {
			ping_nodes_all_to_all(n, false)
		}
	}
}

/*
*   CONTROL FUNCTION FOR FAILURE DETECTION
 */
func DetectFailures(n *net_node.Node) {
	for {
		for i, _ := range n.Table {
			if n.Status == net_node.LEAVING || n.Status == net_node.LEFT {
				return
			}

			if uint32(i) == n.Index {
				continue
			}

			active := n.Table[i].Status == net_node.ACTIVE
			time_invalid := time.Now().Sub(n.Table[i].LastTime.AsTime()).Seconds() > 3.5
			if active && time_invalid {
				log.Println(n.Address, "Detected failure:", n.Table[i].Address)
				n.Table[i].Status = net_node.FAILED_UNHANDLED
				file_system.HandleFailure(n, i)
			}
		}
	}
}
