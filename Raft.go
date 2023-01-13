package main

import (
	"context"
	"cuhk/asgn/raft"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
)

func main() {
	ports := os.Args[2]
	myport, _ := strconv.Atoi(os.Args[1])
	nodeID, _ := strconv.Atoi(os.Args[3])
	heartBeatInterval, _ := strconv.Atoi(os.Args[4])
	electionTimeout, _ := strconv.Atoi(os.Args[5])

	portStrings := strings.Split(ports, ",")

	// A map where
	// 		the key is the node id
	//		the value is the {hostname:port}
	nodeidPortMap := make(map[int]int)
	for i, portStr := range portStrings {
		port, _ := strconv.Atoi(portStr)
		nodeidPortMap[i] = port
	}

	// Create and start the Raft Node.
	_, err := NewRaftNode(myport, nodeidPortMap,
		nodeID, heartBeatInterval, electionTimeout)

	if err != nil {
		log.Fatalln("Failed to create raft node:", err)
	}

	// Run the raft node forever.
	select {}
}

type raftNode struct {
	mu                sync.Mutex
	currentTerm       int32
	votedFor          int32
	log               []*raft.LogEntry
	commitIndex       int32
	kvstore           map[string]int32
	serverState       raft.Role
	electionTimeout   int32
	heartBeatInterval int32
	finishChan        chan bool
	resetChan         chan bool
	commitChan        chan int32
	majoritySize      int
	// volatile for leader
	nextIndex  []int32
	matchIndex []int32
}

// Desc:
// NewRaftNode creates a new RaftNode. This function should return only when
// all nodes have joined the ring, and should return a non-nil error if this node
// could not be started in spite of dialing any other nodes.
//
// Params:
// myport: the port of this new node. We use tcp in this project.
//
//	Note: Please listen to this port rather than nodeidPortMap[nodeId]
//
// nodeidPortMap: a map from all node IDs to their ports.
// nodeId: the id of this node
// heartBeatInterval: the Heart Beat Interval when this node becomes leader. In millisecond.
// electionTimeout: The election timeout for this node. In millisecond.
func NewRaftNode(myport int, nodeidPortMap map[int]int, nodeId, heartBeatInterval,
	electionTimeout int) (raft.RaftNodeServer, error) {

	//remove myself in the hostmap
	delete(nodeidPortMap, nodeId)

	//a map for {node id, gRPCClient}
	hostConnectionMap := make(map[int32]raft.RaftNodeClient)

	rn := raftNode{
		currentTerm:       0,
		votedFor:          -1,
		log:               nil,
		commitIndex:       0,
		finishChan:        make(chan bool, 1),
		kvstore:           make(map[string]int32),
		serverState:       raft.Role_Follower,
		electionTimeout:   int32(electionTimeout),
		heartBeatInterval: int32(heartBeatInterval),
		commitChan:        make(chan int32, 0),
		resetChan:         make(chan bool, 1),
		majoritySize:      (len(nodeidPortMap)+1)/2 + 1,
		nextIndex:         make([]int32, len(nodeidPortMap)+1),
		matchIndex:        make([]int32, len(nodeidPortMap)+1),
	}

	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", myport))

	if err != nil {
		log.Println("Fail to listen port", err)
		os.Exit(1)
	}

	s := grpc.NewServer()
	raft.RegisterRaftNodeServer(s, &rn)

	log.Printf("Start listening to port: %d", myport)
	go s.Serve(l)

	//Try to connect nodes
	for tmpHostId, hostPorts := range nodeidPortMap {
		hostId := int32(tmpHostId)
		numTry := 0
		for {
			numTry++

			conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", hostPorts), grpc.WithInsecure(), grpc.WithBlock())
			//defer conn.Close()
			client := raft.NewRaftNodeClient(conn)
			if err != nil {
				log.Println("Fail to connect other nodes. ", err)
				time.Sleep(1 * time.Second)
			} else {
				hostConnectionMap[hostId] = client
				break
			}
		}
	}
	log.Printf("Successfully connect all nodes")

	ctx := context.Background()
	rn.log = append(rn.log, &raft.LogEntry{})
	// log.Printf("loglong: %d", int32(len(rn.log)))

	go func() {
		for {
			switch rn.serverState {
			case raft.Role_Follower:
				flag := true
				for flag {
					select {
					case <-time.After(time.Duration(rn.electionTimeout) * time.Millisecond):
						// If timer times out, the raft node becomes the candidate
						rn.serverState = raft.Role_Candidate
						rn.finishChan <- true
					case <-rn.resetChan:
						// Do nothing
					case <-rn.finishChan:
						flag = false
					}
				}
			case raft.Role_Candidate:
				flag := true
				for flag {
					rn.currentTerm++
					rn.votedFor = int32(nodeId)
					voteNum := 1
					for hostId, client := range hostConnectionMap {
						lastlogTerm := int32(0)
						lastlogIndex := int32(len(rn.log) - 1)
						//! jjudge 0
						if lastlogIndex != 0 {
							lastlogTerm = rn.log[lastlogIndex].Term
						} else {
							lastlogTerm = 0
						}
						go func(hostId int32, client raft.RaftNodeClient) {
							r, err := client.RequestVote(
								ctx,
								&raft.RequestVoteArgs{
									From:         int32(nodeId),
									To:           int32(hostId),
									Term:         rn.currentTerm,
									CandidateId:  int32(nodeId),
									LastLogIndex: int32(lastlogIndex),
									LastLogTerm:  int32(lastlogTerm),
								},
							)
							if err == nil && r.VoteGranted == true && r.Term == rn.currentTerm {
								// TODO: Lock
								voteNum++
								rn.mu.Lock()
								if voteNum == rn.majoritySize && rn.serverState == raft.Role_Candidate {
									rn.serverState = raft.Role_Leader
									rn.finishChan <- true
								}
								rn.mu.Unlock()
								// TODO: Unlock
							} else if r.Term > rn.currentTerm {
								rn.mu.Lock()
								rn.currentTerm = r.Term
								rn.serverState = raft.Role_Follower
								rn.votedFor = r.From
								rn.finishChan <- true
								rn.mu.Unlock()
								// What if the other node has larger term?
							}
						}(hostId, client)
					}

					select {
					case <-time.After(time.Duration(rn.electionTimeout) * time.Millisecond):
						// If the election times out, start a new election
						// rn.finishChan <- true

					case <-rn.resetChan:
						// Do nothing
					case <-rn.finishChan:
						flag = false
					}
				}
			case raft.Role_Leader:
				// Initialize the nextIndex and matchIndex
				flag := true
				//! 初始化nextIndex matchIndex

				//! Traversing the hostConnectionMap , initialize the nextIndex and matchIndex
				for i := 0; i <= 4; i++ {
					rn.matchIndex[i] = 0
					rn.nextIndex[i] = int32(len(rn.log)-1) + 1
				}
				initial := true
				interval := int32(0)
				// compare := make([]int32, len(nodeidPortMap)+1)

				for flag {
					select {
					case <-time.After(time.Duration(interval) * time.Millisecond):
						// instead := int32(0)

						// count := int32(0)
						for hostId, client := range hostConnectionMap {
							// !Get prevLogIndex and prevLogTerm

							prevLogIndex := rn.nextIndex[hostId] - 1
							prevLogTerm := int32(0)
							// log.Printf("prelogindex: %d", int32(len(rn.log)))
							// log.Printf(" rn.log1 %d", int32(len(rn.log)))
							// log.Printf("prelogindex: %d", int32(rn.log[int32(len(rn.log))].Term))
							if prevLogIndex != 0 {
								prevLogTerm = rn.log[prevLogIndex].Term
							}

							sendLog := []*raft.LogEntry{}
							if !initial && int32(len(rn.log)-1) >= prevLogIndex+1 {
								sendLog = rn.log[prevLogIndex+1:]
							}
							rn.mu.Lock()
							leaderCommit := rn.commitIndex
							rn.mu.Unlock()
							// log.Printf("sendlog: %d", int32(len(sendLog)))
							go func(hostId int32, client raft.RaftNodeClient) {
								r, err := client.AppendEntries(
									ctx,
									&raft.AppendEntriesArgs{
										From:         int32(nodeId),
										To:           int32(hostId),
										Term:         rn.currentTerm,
										LeaderId:     int32(nodeId),
										PrevLogIndex: prevLogIndex,
										PrevLogTerm:  prevLogTerm,
										Entries:      sendLog,
										LeaderCommit: leaderCommit,
									},
								)
								if err == nil && r.Success == true {
									rn.mu.Lock()
									rn.nextIndex[hostId] = int32(r.MatchIndex + 1)
									rn.matchIndex[hostId] = int32(r.MatchIndex)
									instead := int32(0)
									// if count == 0 {
									//! 实现 N majority matchindex > N
									compare := make([]int32, len(rn.matchIndex))
									copy(compare, rn.matchIndex)
									for i := 0; i < len(compare); i++ {
										for j := i; j < len(compare); j++ {
											if compare[i] < compare[j] {
												instead = compare[i]
												compare[i] = compare[j]
												compare[j] = int32(instead)
											}
										}
									}

									N := compare[len(compare)/2-1]
									if rn.currentTerm == rn.log[N].Term && N > rn.commitIndex {
										rn.commitIndex = N
										rn.commitChan <- N
										log.Printf("N: %d", int32(N))

									}

									rn.mu.Unlock()

								} else {

									rn.nextIndex[hostId]--
									if r.Term > rn.currentTerm {
										rn.serverState = raft.Role_Follower
										rn.votedFor = -1
										rn.finishChan <- true
									}
									// What if there is an error OR Success == false?
								}

							}(hostId, client)

						}

					// No need to reset timer for the leader
					case <-rn.finishChan:
						flag = false
					}

					if initial {
						initial = false
						interval = rn.heartBeatInterval
					}
				}
			}
		}
	}()

	return &rn, nil
}

// Desc:
// Propose initializes proposing a new operation, and replies with the
// result of committing this operation. Propose should not return until
// this operation has been committed, or this node is not leader now.
//
// If the we put a new <k, v> pair or deleted an existing <k, v> pair
// successfully, it should return OK; If it tries to delete an non-existing
// key, a KeyNotFound should be returned; If this node is not leader now,
// it should return WrongNode as well as the currentLeader id.
//
// Params:
// args: the operation to propose
// reply: as specified in Desc
func (rn *raftNode) Propose(ctx context.Context, args *raft.ProposeArgs) (*raft.ProposeReply, error) {
	log.Printf("Receive propose from client")
	var ret raft.ProposeReply
	if rn.serverState == raft.Role_Leader {
		ret.CurrentLeader = rn.votedFor
		if args.Op == raft.Operation_Delete {
			// Check whether the key exist
			// If existed, set ok to true, otherwise false

			if _, ok := rn.kvstore[args.Key]; ok {

				ret.Status = raft.Status_OK
			} else {
				ret.Status = raft.Status_KeyNotFound
			}
		} else {
			ret.Status = raft.Status_OK
		}
	} else {
		ret.CurrentLeader = rn.votedFor
		ret.Status = raft.Status_WrongNode
	}

	if ret.Status == raft.Status_OK || ret.Status == raft.Status_KeyNotFound {
		rn.log = append(rn.log, &raft.LogEntry{Term: rn.currentTerm, Op: args.Op, Key: args.Key, Value: args.V})
		lenth := int32(len(rn.log) - 1)
		value := <-rn.commitChan
		for value != int32(lenth) {

		}
		if _, ok := rn.kvstore[args.Key]; ok {

			ret.Status = raft.Status_OK
		}

		// Lock
		rn.mu.Lock()
		// rn.commitIndex++

		// if ret.Status == raft.Status_OK {

		if args.Op == raft.Operation_Put {

			rn.kvstore[args.Key] = args.V
			log.Printf("leader put++")

		} else if args.Op == raft.Operation_Delete {
			if ret.Status == raft.Status_OK {
				log.Printf("leader delete++")
				delete(rn.kvstore, args.Key)

			}

		}
		// }
		ret.Status = raft.Status_OK
		log.Printf("propose reyure %d", rn.commitIndex)
		// Unlock
		rn.mu.Unlock()

	}
	return &ret, nil
}

// Desc:GetValue
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key
func (rn *raftNode) GetValue(ctx context.Context, args *raft.GetValueArgs) (*raft.GetValueReply, error) {
	var ret raft.GetValueReply
	if val, ok := rn.kvstore[args.Key]; ok {
		ret.V = val
		ret.Status = raft.Status_KeyFound
	} else {
		ret.V = 0
		ret.Status = raft.Status_KeyNotFound
	}
	// Unock
	return &ret, nil
}

// Desc:
// Receive a RecvRequestVote message from another Raft Node. Check the paper for more details.
//
// Params:
// args: the RequestVote Message, you must include From(src node id) and To(dst node id) when
// you call this API
// reply: the RequestVote Reply Message
func (rn *raftNode) RequestVote(ctx context.Context, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	var reply raft.RequestVoteReply
	reply.From = args.To
	reply.To = args.From

	// TODO: Consider what if args.Term > rn.currenTerm?
	if args.Term > rn.currentTerm {
		rn.mu.Lock()
		rn.votedFor = args.CandidateId
		rn.currentTerm = args.Term
		rn.serverState = raft.Role_Follower

		rn.finishChan <- true
		rn.mu.Unlock()
	}

	reply.Term = rn.currentTerm

	comTerm := int32(0)
	if int32(len(rn.log)-1) == 0 {
		comTerm = int32(0)
	} else {
		comTerm = rn.log[args.LastLogIndex].Term
	}
	// Add more conditions here
	if args.Term >= rn.currentTerm && (rn.votedFor == -1 || rn.votedFor == args.CandidateId) && (comTerm <= args.LastLogTerm || int32(len(rn.log)) <= args.LastLogIndex) {

		// rn.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}

	if reply.VoteGranted == true {
		// rn.currentTerm = args.Term
		rn.resetChan <- true
		// reset the timer to avoid timeout
	}

	return &reply, nil
}

// Desc:
// Receive a RecvAppendEntries message from another Raft Node. Check the paper for more details.
//
// Params:
// args: the AppendEntries Message, you must include From(src node id) and To(dst node id) when
// you call this API
// reply: the AppendEntries Reply Message
func (rn *raftNode) AppendEntries(ctx context.Context, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	var reply raft.AppendEntriesReply
	// log.Printf("append: %d->%d", reply.To, reply.From)
	// log.Printf("log: %d", int32(len(rn.log)))
	reply.From = args.To
	reply.To = args.From
	reply.Success = true
	reply.Term = rn.currentTerm
	// lenth := int32(len(rn.log))

	// Receive heartbeat from new leader
	if args.Term >= rn.currentTerm {
		rn.mu.Lock()
		reply.Success = true
		reply.Term = args.Term
		rn.currentTerm = args.Term
		rn.mu.Unlock()
		if rn.serverState != raft.Role_Follower {
			rn.mu.Lock()
			rn.serverState = raft.Role_Follower //
			rn.finishChan <- true
			rn.votedFor = reply.To
			rn.mu.Unlock()
			// TODO
		} else {
			// reply.Success = false
			// rn.resetChan <- true
			rn.mu.Lock()
			rn.votedFor = reply.To
			// rn.resetChan <- true
			rn.mu.Unlock()

			// TODO
		}
	}
	reply.Term = rn.currentTerm
	term := int32(0)
	if args.PrevLogIndex != 0 {
		term = rn.log[args.PrevLogIndex].Term
	}
	// Consider in which case the reply is unsuccessful
	if args.Term < rn.currentTerm {
		reply.Success = false
		reply.Term = rn.currentTerm
		rn.mu.Lock()
		rn.serverState = raft.Role_Candidate
		rn.finishChan <- true
		rn.mu.Unlock()
		// log.Printf(" 1")

	} else if int32(len(rn.log)) < args.PrevLogIndex {
		// log.Printf(" 2")
		reply.Success = false
	} else if term != args.PrevLogTerm {
		// log.Printf(" 3")
		reply.Success = false
	}

	if reply.Success {
		// if int32(len(args.Entries)) != 0 {
		rn.mu.Lock()
		// log.Printf(" rn.log1 %d", int32(len(rn.log)))

		// if int32(len(rn.log)) != 0 {
		rn.log = rn.log[:args.PrevLogIndex+1]
		// }
		rn.log = append(rn.log, args.Entries...)
		// log.Printf(" rn.log2 %d", int32(len(rn.log)))

		rn.mu.Unlock()
		// Delete the conflict entries
		// Append new entring not in the log
		// }
		reply.MatchIndex = int32(len(rn.log) - 1)

	}

	if args.LeaderCommit > rn.commitIndex {
		minIndex := int32(len(rn.log) - 1)
		if args.LeaderCommit < int32(len(rn.log)-1) {
			minIndex = args.LeaderCommit
		}
		for i := rn.commitIndex + 1; i <= minIndex; i++ {

			rn.mu.Lock()

			if rn.log[i].Op == raft.Operation_Put {
				// log.Printf(" put 1")
				rn.kvstore[rn.log[i].Key] = rn.log[i].Value
				// log.Printf(" put 2")
			} else if rn.log[i].Op == raft.Operation_Delete {
				// log.Printf(" delete 1")
				if _, ok := rn.kvstore[rn.log[i].Key]; ok {
					delete(rn.kvstore, rn.log[i].Key)
					log.Printf(" %d delete", reply.From)
				}

			}

			// Unlock
			rn.mu.Unlock()
			// }
			// Apply the opertion
		}
		rn.commitIndex = minIndex
	}

	rn.resetChan <- true

	return &reply, nil
}

// Desc:
// Set electionTimeOut as args.Timeout milliseconds.
// You also need to stop current ticker and reset it to fire every args.Timeout milliseconds.
//
// Params:
// args: the heartbeat duration
// reply: no use
func (rn *raftNode) SetElectionTimeout(ctx context.Context, args *raft.SetElectionTimeoutArgs) (*raft.SetElectionTimeoutReply, error) {
	var reply raft.SetElectionTimeoutReply
	rn.electionTimeout = args.Timeout
	rn.resetChan <- true
	return &reply, nil
}

// Desc:
// Set heartBeatInterval as args.Interval milliseconds.
// You also need to stop current ticker and reset it to fire every args.Interval milliseconds.
//
// Params:
// args: the heartbeat duration
// reply: no use
func (rn *raftNode) SetHeartBeatInterval(ctx context.Context, args *raft.SetHeartBeatIntervalArgs) (*raft.SetHeartBeatIntervalReply, error) {
	var reply raft.SetHeartBeatIntervalReply
	rn.heartBeatInterval = args.Interval
	rn.resetChan <- true
	return &reply, nil
}

// NO NEED TO TOUCH THIS FUNCTION
func (rn *raftNode) CheckEvents(context.Context, *raft.CheckEventsArgs) (*raft.CheckEventsReply, error) {
	return nil, nil
}
