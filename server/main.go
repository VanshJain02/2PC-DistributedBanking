package main

// Code From Vansh Jain
// DS Lab 3
// SBU ID - 116713519

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	pb "project1/proto"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

const (
	initialBalance = 10
)

type server struct {
	pb.UnimplementedBankingServiceServer
	name              string
	id                int
	cluster_id        int
	ShardDB           *sql.DB
	numClients        int
	numClusters       int
	serversPerCluster int
	isActive          bool
	total_txns        float64
	total_latency     time.Duration
	servers           []*grpc.ClientConn
	mu                sync.Mutex
	ballot_num        int64
	acceptNum         int64
	acceptVal         *pb.Transaction
	commitedTxns      map[int32]*pb.Transaction
	WAL               map[int32]*pb.Transaction
	start             time.Time
}

type Cluster struct {
	id      int
	servers []*server
}

type execTxn struct {
	TxID   int             `json:"tx_id"`
	Txn    *pb.Transaction `json:"txn"`
	Status string          `json:"status"`
}

func CreateSQLiteDB(shardName string) *sql.DB {
	db, err := sql.Open("sqlite3", fmt.Sprintf("%s.db", shardName))
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS accounts (
		id TEXT PRIMARY KEY,
		balance REAL,
		locked BOOLEAN DEFAULT 0,
		executed_txns TEXT DEFAULT NULL
	)`)
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS WALTABLE (
		id REAL PRIMARY KEY,
		WAL TEXT DEFAULT NULL
	)`)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	return db
}

func InitializeShardData(db *sql.DB, startID, endID int) {
	for id := startID; id <= endID; id++ {
		_, err := db.Exec("INSERT OR REPLACE INTO accounts (id, balance, locked, executed_txns) VALUES (?, ?, ?, ?)", fmt.Sprintf("C%d", id), initialBalance, false, nil)
		if err != nil {
			log.Printf("Failed to initialize data for ID %d: %v", id, err)
		}
		// _, err = db.Exec("INSERT OR REPLACE INTO accounts (id, locked) VALUES (?, ?)", fmt.Sprintf("C%d", id), 0)
		// if err != nil {
		// 	log.Printf("Failed to initialize locked for ID %d: %v", id, err)
		// }
	}
	_, _ = db.Exec(`DELETE FROM WALTABLE;`)

}

func (s *server) SyncServerStatus(ctx context.Context, in *pb.ServerStatus) (*pb.Empty, error) {
	fmt.Printf("\n******** [%s] Server Status Updated *******\n", s.name)
	s.isActive = in.ServerStatus[s.id-1] == 1
	return &pb.Empty{}, nil
}

func (s *server) PrintPerformance(ctx context.Context, in *pb.Empty) (*pb.PrintPerformanceResponse, error) {
	if s.total_txns > 0 {
		avgLatency := s.total_latency / time.Duration(s.total_txns)
		fmt.Printf("Server %s Performance:\n", s.name)
		fmt.Printf("  Total Transactions: %d\n", s.total_latency)
		fmt.Printf("  Total Latency: %s\n", s.total_latency)
		fmt.Printf("  Average Latency per Transaction: %s\n", avgLatency)
		fmt.Printf("  Throughput (Txns/sec): %.2f\n", s.total_txns/s.total_latency.Seconds())
	} else {
		fmt.Printf("The server has 0 transactions, hence cannot show performance metrics\n")
	}
	return &pb.PrintPerformanceResponse{TotalTransactions: int64(s.total_txns), TotalLatency: float32(s.total_latency)}, nil
}

func (s *server) PrintBalance(ctx context.Context, in *pb.PrintBalanceRequest) (*pb.PrintBalanceResponse, error) {
	// var balance float64
	var balance float64
	err := s.ShardDB.QueryRow("SELECT balance FROM accounts WHERE id = ?", fmt.Sprintf("C%d", in.ClientId)).Scan(&balance)
	if err != nil {
		log.Printf("[%s] failed to check balance: %v", s.name, err)
	}
	log.Printf("[%s]: %v", s.name, balance)
	// fmt.Printf("Server %s balance: %f\n", s.name, s.Clientbalance)
	return &pb.PrintBalanceResponse{Balance: int64(balance)}, nil
}
func (s *server) PrintLog(ctx context.Context, in *pb.Empty) (*pb.PrintLogResponse, error) {
	var currentTxnData sql.NullString
	err := s.ShardDB.QueryRow(`SELECT executed_txns FROM accounts WHERE id = (?)`, fmt.Sprintf("C%d", 1000*s.cluster_id+1)).Scan(&currentTxnData)
	if err != nil {
		log.Print("Error found")

		if err == sql.ErrNoRows {
			log.Print("Account with id not found")
		}
	}

	var transactions []execTxn

	// If there are existing transactions, deserialize them
	if currentTxnData.Valid {
		err = json.Unmarshal([]byte(currentTxnData.String), &transactions)
		if err != nil {
		}
	}
	convertedTxns := []*pb.ExecTxn{}
	for _, i := range transactions {
		convertedTxns = append(convertedTxns, &pb.ExecTxn{TxID: int32(i.TxID), Txn: i.Txn, Status: i.Status})
	}
	// fmt.Printf("Client %s log: %v\n", s.name, s.commitedTxns)
	return &pb.PrintLogResponse{Logs: convertedTxns}, nil
}

func (s *server) PCCommit(ctx context.Context, in *pb.PCCommitMessage) (*pb.PCResponse, error) {
	if !s.isActive {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	start := time.Now()
	// name := fmt.Sprintf("C%d", in.TxnCorresponding)
	// log.Printf("----- [%s] COMMITTED %v -----", s.name, in.TxnCorresponding)

	WAL := s.fetchWAL(int64(in.TxnCorresponding))
	log.Printf("----- [%s] COMMITTED (txn: %d->%d : %v)-----", s.name, WAL.Sender, WAL.Receiver, WAL.Amount)

	x := fmt.Sprintf("C%d", int(WAL.Sender))
	y := fmt.Sprintf("C%d", int(WAL.Receiver))
	sender_id := int(WAL.Sender - 1)
	receiver_id := int(WAL.Receiver - 1)
	cluster_id_sender := (sender_id * s.numClusters) / s.numClients
	cluster_id_receiver := (receiver_id * s.numClusters) / s.numClients
	if cluster_id_sender == s.cluster_id {
		s.AppendExecutedTxn(x, execTxn{TxID: int(WAL.Timestamp), Txn: WAL, Status: "C"})

	}
	if cluster_id_receiver == s.cluster_id {
		s.AppendExecutedTxn(y, execTxn{TxID: int(WAL.Timestamp), Txn: WAL, Status: "C"})

	}

	s.commitedTxns[int32(WAL.Timestamp)] = WAL
	s.deleteFromWAL(int64(in.TxnCorresponding))
	delete(s.WAL, in.TxnCorresponding)
	_, err := s.ShardDB.Exec("UPDATE accounts SET locked = 0 WHERE id IN (?, ?)", x, y)
	if err != nil {
		log.Printf("failed to release locks: %v", err)
	}
	s.total_latency += time.Since(start)
	return &pb.PCResponse{Ack: true}, nil
}
func (s *server) PCAbort(ctx context.Context, in *pb.PCAbortMessage) (*pb.PCResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	start := time.Now()
	// name := fmt.Sprintf("C%d", in.TxnCorresponding)
	log.Printf("----- [%s] TIME TO ABORT %v-----", s.name, in.TxnCorresponding)

	if _, exist := s.WAL[in.TxnCorresponding]; exist {
		WAL := s.fetchWAL(int64(in.TxnCorresponding))

		// log.Printf("[%s] (txn %v: %d->%d : %v)", s.name, in.TxnCorresponding, WAL.Sender, WAL.Receiver, WAL.Amount)

		x := fmt.Sprintf("C%d", int(WAL.Sender))
		y := fmt.Sprintf("C%d", int(WAL.Receiver))
		amt := WAL.Amount

		_, err := s.ShardDB.Exec(`
        UPDATE accounts
        SET balance = CASE
            WHEN id = ? THEN balance + ?
            WHEN id = ? THEN balance - ?
        END
        WHERE id IN (?, ?)`,
			x, amt, y, amt, x, y)
		if err != nil {
			s.ShardDB.Exec("UPDATE accounts SET locked = 0 WHERE id IN (?, ?)", x, y) // Release locks
			log.Printf("failed to perform transfer: %v", err)
		}
		_, err = s.ShardDB.Exec("UPDATE accounts SET locked = 0 WHERE id IN (?, ?)", x, y)
		if err != nil {
			log.Printf("failed to release locks: %v", err)
		} else {
			// log.Printf("Released Locks for %d  %v", err)

		}
		s.deleteFromWAL(int64(in.TxnCorresponding))
		delete(s.WAL, in.TxnCorresponding)
	}
	s.total_latency += time.Since(start)
	return &pb.PCResponse{Ack: false}, nil
}

func (s *server) TransferMoney(ctx context.Context, in *pb.TransferRequest) (*pb.TransactionResponse, error) {
	if !s.isActive {
		for {
		}
	}

	sender_id := int(in.Txn.Sender - 1)
	receiver_id := int(in.Txn.Receiver - 1)
	cluster_id_sender := (sender_id * s.numClusters) / s.numClients
	cluster_id_receiver := (receiver_id * s.numClusters) / s.numClients
	if cluster_id_sender == cluster_id_receiver {
		s.mu.Lock()
		// fmt.Print("\nIntra Shard Transaction\n")

		s.start = time.Now()
		result := s.startPaxos(in, 2)
		s.total_latency += time.Since(s.start)
		s.mu.Unlock()
		log.Printf("[%s] Replying Back To Client", s.name)
		return &pb.TransactionResponse{Message: fmt.Sprintf("Result of %d: Send Back Reply", in.Txn.Timestamp), Success: result}, nil

	} else {
		// fmt.Print("Cross Shard Transaction\n")
		if s.cluster_id == cluster_id_sender {
			s.mu.Lock()

			s.start = time.Now()
			result := s.startPaxos(in, 1)
			s.total_latency += time.Since(s.start)
			s.mu.Unlock()
			log.Printf("[%s] Replying Back To Client", s.name)
			return &pb.TransactionResponse{Message: fmt.Sprintf("Result of %d: Send Back Reply", in.Txn.Timestamp), Success: result}, nil
		}
		if s.cluster_id == cluster_id_receiver {
			s.mu.Lock()

			s.start = time.Now()
			result := s.startPaxos(in, 1)
			s.total_latency += time.Since(s.start)
			s.mu.Unlock()
			log.Printf("[%s] Replying Back To Client", s.name)
			return &pb.TransactionResponse{Message: fmt.Sprintf("Result of %d: Send Back Reply", in.Txn.Timestamp), Success: result}, nil

		}

		return &pb.TransactionResponse{Message: "Transfer Unsuccessful", Success: false}, nil

	}

}

func (s *server) startPaxos(in *pb.TransferRequest, min_count int) bool {
	s.ballot_num++
	ch := make(chan bool) // Create a channel
	go func(ballot_num int64, in *pb.TransferRequest, min_count int) {
		log.Printf("[%s] STARTING PAXOS %d->%d : %v", s.name, in.Txn.Sender, in.Txn.Receiver, in.Txn.Amount)
		promiseMsgs := []*pb.PromiseMessage{}
		acceptValFlag := false
		ballotNumFlag := false
		acceptVal := &pb.Transaction{}

		prepareMessage := pb.PrepareMessage{
			BallotNum:          ballot_num,
			LenCommitedSeqNums: int32(len(s.commitedTxns)),
			SenderId:           int32(s.id),
		}

		promise := pb.PromiseMessage{
			Ack:       true,
			N:         ballot_num,
			AcceptNum: s.acceptNum,
			AcceptVal: s.acceptVal,
		}
		promiseMsgs = append(promiseMsgs, &promise)
		max_commited_txns := int32(len(s.commitedTxns))
		max_commited_txns_id := -1
		var wg sync.WaitGroup
		total_promises := 0
		wg.Add((s.serversPerCluster - 1))

		for _, srv := range s.servers {
			if srv == nil {
				total_promises++
				continue
			}
			go func(srv *grpc.ClientConn) {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second) // Timeout for gRPC call
				defer cancel()
				promiseMsg, _ := pb.NewBankingServiceClient(srv).Prepare(ctx, &prepareMessage)
				defer wg.Done()
				if promiseMsg != nil {

					if promiseMsg.LenCommitedSeqNums > max_commited_txns {
						log.Printf("[%s] LEADER SYNC IS REQUIRED", s.name)
						max_commited_txns = promiseMsg.LenCommitedSeqNums
						max_commited_txns_id = int(promiseMsg.SenderId)
					}
					if promiseMsg.Ack {
						if promiseMsg.AcceptNum != 0 {
							acceptValFlag = true
							acceptVal = promiseMsg.AcceptVal
						}
						promiseMsgs = append(promiseMsgs, promiseMsg)
						if len(promiseMsgs) <= (s.serversPerCluster/2)+1 {

						}
					} else if promiseMsg.N != 0 {
						ballotNumFlag = true
					}

				}
				// total_promises++
				// if total_promises == len(s.servers) && len(promiseMsgs) < (s.serversPerCluster/2)+1 {
				// 	wg.Done()
				// }

			}(srv)
		}
		wg.Wait()

		if max_commited_txns_id != -1 {
			log.Printf("LEADER UNDERGOING SYNCHRONISATION: %d", in.Txn.Timestamp)
			s.StartSyncing(int(max_commited_txns), max_commited_txns_id)
			log.Print("******* Retrying PAXOS *******")
			ch <- s.startPaxos(in, min_count)
			return
		} else if ballotNumFlag {
			ch <- s.startPaxos(in, min_count)
			return
		}
		s.total_txns++
		if len(promiseMsgs) > (s.serversPerCluster / 2) {
			log.Printf("[%s] Majority Prepare Achieved - Proposing Txn %d", s.name, in.Txn.Timestamp)
		} else {
			log.Printf("[%s] Failure: Prepare Quorum Not Formed: %d", s.name, in.Txn.Timestamp)
			ch <- false
			return
		}

		proposedTxn := &pb.Transaction{}
		if acceptValFlag {
			log.Printf("[%s] PROPOSING ACCEPT VAL: %d", s.name, in.Txn.Timestamp)
			proposedTxn = acceptVal
		} else {
			log.Printf("[%s] PROPOSING TXN(%d) %d->%d : %v", s.name, in.Txn.Timestamp, in.Txn.Sender, in.Txn.Receiver, in.Txn.Amount)
			proposedTxn = in.Txn
		}

		acceptMessage := pb.AcceptMessage{
			N:           s.ballot_num,
			ProposedTxn: proposedTxn,
		}
		sender_id := int(in.Txn.Sender - 1)
		receiver_id := int(in.Txn.Receiver - 1)
		cluster_id_sender := (sender_id * s.numClusters) / s.numClients

		x := fmt.Sprintf("C%d", sender_id+1)
		y := fmt.Sprintf("C%d", receiver_id+1)
		res, _ := s.ShardDB.Exec("UPDATE accounts SET locked = 1 WHERE id IN (?, ?) AND locked = 0", x, y)
		rowsAffected, _ := res.RowsAffected()
		// log.Printf("[%s] Locks Made: %d", s.name, rowsAffected)
		if rowsAffected < int64(min_count) {
			s.ShardDB.Exec("UPDATE accounts SET locked = 0 WHERE id IN (?, ?)", x, y)
			log.Printf("[%s] Could not acquire sufficient locks for txn: %d", s.name, in.Txn.Timestamp)
			ch <- false
			return
		}
		if cluster_id_sender == s.cluster_id {
			var balance float64
			err := s.ShardDB.QueryRow("SELECT balance FROM accounts WHERE id = ?", fmt.Sprintf("C%d", sender_id+1)).Scan(&balance)
			if err != nil {
				s.ShardDB.Exec("UPDATE accounts SET locked = 0 WHERE id IN (?, ?)", x, y)

				// s.mu.Unlock()
				log.Printf("[%s] failed to check balance: %v", s.name, err)
			}
			if balance < float64(in.Txn.Amount) {
				s.ShardDB.Exec("UPDATE accounts SET locked = 0 WHERE id IN (?, ?)", x, y)
				log.Printf("[%s] Insufficient balance for txn: %d", s.name, in.Txn.Timestamp)
				ch <- false
				return
			}
		}
		s.acceptNum = acceptMessage.N
		s.acceptVal = acceptMessage.ProposedTxn
		acceptedMsgs := []*pb.AcceptedMessage{}
		acceptedMsgs = append(acceptedMsgs, &pb.AcceptedMessage{Ack: true, N: acceptMessage.N, ProposedTxn: acceptMessage.ProposedTxn})
		var wg1 sync.WaitGroup

		wg1.Add(s.serversPerCluster - 1)
		for _, srv := range s.servers {
			if srv == nil {
				continue
			}
			go func(srv *grpc.ClientConn) {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()
				acceptedMsg, _ := pb.NewBankingServiceClient(srv).Accept(ctx, &acceptMessage)
				defer wg1.Done()
				if acceptedMsg != nil {
					if acceptedMsg.Ack {
						acceptedMsgs = append(acceptedMsgs, acceptedMsg)
						if len(acceptedMsgs) <= (s.serversPerCluster/2)+1 {
						}
					}
				}
			}(srv)
		}
		wg1.Wait()
		if len(acceptedMsgs) > (s.serversPerCluster / 2) {
			log.Printf("[%s] Majority Accept Achieved - Commiting Txn: %d", s.name, in.Txn.Timestamp)
			time.Sleep(5 * time.Millisecond)
		} else {
			log.Printf("[%s] Failure: Accept Quorum Not Formed: %d", s.name, in.Txn.Timestamp)
			ch <- false
			return
		}
		commit := pb.CommitMessage{N: s.ballot_num}
		for _, srv := range s.servers {
			if srv == nil {
				continue
			}
			go func(srv *grpc.ClientConn) {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second) // Timeout for gRPC call
				defer cancel()
				pb.NewBankingServiceClient(srv).Decide(ctx, &commit)
			}(srv)
		}
		log.Printf(" [%s] Commiting Transactions......   %d->%d : %v", s.name, s.acceptVal.Sender, s.acceptVal.Receiver, s.acceptVal.Amount)
		x = fmt.Sprintf("C%d", s.acceptVal.Sender)
		y = fmt.Sprintf("C%d", s.acceptVal.Receiver)
		amt := s.acceptVal.Amount
		_, err := s.ShardDB.Exec(`
        UPDATE accounts
        SET balance = CASE
            WHEN id = ? THEN balance - ?
            WHEN id = ? THEN balance + ?
        END
        WHERE id IN (?, ?)`,
			x, amt, y, amt, x, y)

		sender_id = int(s.acceptVal.Sender - 1)
		receiver_id = int(s.acceptVal.Receiver - 1)
		cluster_id_sender = (sender_id * s.numClusters) / s.numClients
		cluster_id_receiver := (receiver_id * s.numClusters) / s.numClients
		if cluster_id_receiver != cluster_id_sender {
			if cluster_id_sender == s.cluster_id {
				s.WAL[int32(s.acceptVal.Timestamp)] = s.acceptVal
				s.AppendExecutedTxn(x, execTxn{TxID: int(s.acceptVal.Timestamp), Txn: s.acceptVal, Status: "P"})
				s.insertIntoWAL(s.acceptVal.Timestamp, s.acceptVal)

			}
			if cluster_id_receiver == s.cluster_id {
				s.WAL[int32(s.acceptVal.Timestamp)] = s.acceptVal
				s.AppendExecutedTxn(y, execTxn{TxID: int(s.acceptVal.Timestamp), Txn: s.acceptVal, Status: "P"})
				s.insertIntoWAL(s.acceptVal.Timestamp, s.acceptVal)
			}
		} else {
			s.AppendExecutedTxn(x, execTxn{TxID: int(s.acceptVal.Timestamp), Txn: s.acceptVal, Status: ""})
			s.commitedTxns[int32(s.acceptVal.Timestamp)] = s.acceptVal

			if err != nil {
				s.ShardDB.Exec("UPDATE accounts SET locked = 0 WHERE id IN (?, ?)", x, y) // Release locks
				log.Printf("failed to perform transfer: %v", err)
			}
			_, err = s.ShardDB.Exec("UPDATE accounts SET locked = 0 WHERE id IN (?, ?)", x, y)
			if err != nil {
				log.Printf("[%s] failed to release locks: %v", s.name, err)
			}
		}
		s.acceptVal = nil
		s.acceptNum = 0
		ch <- true
	}(s.ballot_num, in, min_count)

	result := <-ch
	return result

}

func (s *server) Catchup(ctx context.Context, in *pb.CatchupMessage) (*pb.SyncedResponse, error) {
	log.Printf("[%s] Contacted by Server to Catchup", s.name)
	return &pb.SyncedResponse{IsSynced: "yes", CommitedTxn: s.commitedTxns, BallotNum: s.ballot_num}, nil
}
func (s *server) StartSyncing(highest_commit int, highest_commit_id int) int64 {
	if highest_commit != 0 {
		start := time.Now()
		ctx1, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		log.Printf("[%s] HIGHEST COMMIT ID: %d : %d", s.name, highest_commit_id, (highest_commit_id-1)%s.serversPerCluster)
		data, _ := pb.NewBankingServiceClient(s.servers[(highest_commit_id-1)%s.serversPerCluster]).Catchup(ctx1, &pb.CatchupMessage{})
		s.ballot_num = data.BallotNum
		for i, block := range data.CommitedTxn {
			if _, exist := s.commitedTxns[i]; !exist {
				log.Printf("[%s] Executing Transaction %d->%d : %v", s.name, block.Sender, block.Receiver, block.Amount)
				s.commitedTxns[i] = block
				x := fmt.Sprintf("C%d", block.Sender)
				y := fmt.Sprintf("C%d", block.Receiver)
				amt := block.Amount
				s.ShardDB.Exec(`
					UPDATE accounts
					SET balance = CASE
						WHEN id = ? THEN balance - ?
						WHEN id = ? THEN balance + ?
					END
					WHERE id IN (?, ?)`,
					x, amt, y, amt, x, y)
				s.total_txns++

				sender_id := int(block.Sender - 1)
				receiver_id := int(block.Receiver - 1)
				cluster_id_sender := (sender_id * s.numClusters) / s.numClients
				cluster_id_receiver := (receiver_id * s.numClusters) / s.numClients
				if cluster_id_sender == cluster_id_receiver {
					s.AppendExecutedTxn(x, execTxn{TxID: int(block.Timestamp), Txn: block, Status: ""})
				} else {
					if cluster_id_sender == s.cluster_id {
						s.AppendExecutedTxn(x, execTxn{TxID: int(block.Timestamp), Txn: block, Status: "P"})
						s.AppendExecutedTxn(x, execTxn{TxID: int(block.Timestamp), Txn: block, Status: "C"})

					}
					if cluster_id_receiver == s.cluster_id {
						s.AppendExecutedTxn(y, execTxn{TxID: int(block.Timestamp), Txn: block, Status: "P"})
						s.AppendExecutedTxn(y, execTxn{TxID: int(block.Timestamp), Txn: block, Status: "C"})

					}

				}

			}
		}
		s.total_latency += time.Since(start)
		return 1
	}
	return 0
}

func (s *server) Prepare(ctx context.Context, in *pb.PrepareMessage) (*pb.PromiseMessage, error) {
	if !s.isActive {
		return nil, nil
	}
	// s.mu.Lock()

	// log.Printf("[%s] Prepare Received from %d ", s.name, in.SenderId)

	promise := pb.PromiseMessage{
		Ack:                true,
		N:                  s.ballot_num,
		AcceptNum:          s.acceptNum,
		AcceptVal:          s.acceptVal,
		LenCommitedSeqNums: int32(len(s.commitedTxns)),
		SenderId:           int32(s.id),
	}
	if in.BallotNum > int64(s.ballot_num) {
		s.start = time.Now()
		s.ballot_num = in.BallotNum
		promise.N = s.ballot_num
		if in.LenCommitedSeqNums > int32(len(s.commitedTxns)) {
			log.Printf("[%s] Inconsistency Found in Commit", s.name)
			s.StartSyncing(int(in.LenCommitedSeqNums), int(in.SenderId))
		} else if in.LenCommitedSeqNums < int32(len(s.commitedTxns)) {
			log.Printf("[%s] Leader Inconsistency %v : %v", s.name, len(s.commitedTxns), in.LenCommitedSeqNums)

		}
		if in.LenCommitedSeqNums == int32(len(s.commitedTxns)) {
			s.total_txns++
		}
		// log.Printf("[%s] Promise Sent to S%d", s.name, in.SenderId)
		return &promise, nil
	} else {
		promise.Ack = false
		log.Printf("[%s] Promise Rejected to S%d", s.name, in.SenderId)
		return &promise, nil
	}
}

func (s *server) Accept(ctx context.Context, in *pb.AcceptMessage) (*pb.AcceptedMessage, error) {
	if !s.isActive {
		return nil, nil
	}
	s.mu.Lock()
	sender_id := int(in.ProposedTxn.Sender)
	receiver_id := int(in.ProposedTxn.Receiver)
	cluster_id_sender := ((sender_id - 1) * s.numClusters) / s.numClients
	cluster_id_receiver := ((receiver_id - 1) * s.numClusters) / s.numClients
	if cluster_id_sender == s.cluster_id {
		res, err := s.ShardDB.Exec("UPDATE accounts SET locked = 1 WHERE id IN (?) AND locked = 0", fmt.Sprintf("C%d", sender_id))
		if err != nil {
			log.Printf("[%s] failed to acquire locks: %v", s.name, err)
		} else {
			// log.Printf("[%s] Acquired Locks of %s, %s", s.name, in.ProposedTxn.Sender, in.ProposedTxn.Receiver)
		}
		rowsAffected, _ := res.RowsAffected()
		// log.Printf("[%s] Locks Made by Sender %d : %d", s.name, sender_id, rowsAffected)
		if rowsAffected < 1 {
			s.ShardDB.Exec("UPDATE accounts SET locked = 0 WHERE id IN (?)", fmt.Sprintf("C%d", sender_id)) // Release any acquired locks
			log.Printf("[%s] could not acquire locks for %d ", s.name, sender_id)
			return &pb.AcceptedMessage{Ack: false}, nil
		}

	}
	if cluster_id_receiver == s.cluster_id {
		res, err := s.ShardDB.Exec("UPDATE accounts SET locked = 1 WHERE id IN (?) AND locked = 0", fmt.Sprintf("C%d", receiver_id))
		if err != nil {
			log.Printf("[%s] failed to acquire locks: %v", s.name, err)
		} else {
			// log.Printf("[%s] Acquired Locks of %s, %s", s.name, in.ProposedTxn.Sender, in.ProposedTxn.Receiver)
		}
		rowsAffected, _ := res.RowsAffected()
		// log.Printf("[%s] Locks Made by Receiver %d : %d", s.name, receiver_id, rowsAffected)

		if rowsAffected < 1 {
			s.ShardDB.Exec("UPDATE accounts SET locked = 0 WHERE id IN (?)", fmt.Sprintf("C%d", receiver_id)) // Release any acquired locks
			log.Printf("[%s] could not acquire locks for %d ", s.name, receiver_id)
			return &pb.AcceptedMessage{Ack: false}, nil
		}

	}

	if in.N > s.acceptNum {
		s.acceptNum = in.N
		s.acceptVal = in.ProposedTxn
		// log.Printf("[%s] Accepted Msg Sent", s.name)
		return &pb.AcceptedMessage{Ack: true, N: in.N, ProposedTxn: in.ProposedTxn}, nil
	}
	return &pb.AcceptedMessage{Ack: false}, nil
}

func (s *server) Decide(ctx context.Context, in *pb.CommitMessage) (*pb.Empty, error) {
	if !s.isActive {
		return nil, nil
	}
	if s.acceptNum != 0 {
		temp_acceptVal := s.acceptVal
		log.Printf(" [%s] Commiting Transactions......   %d->%d : %v", s.name, s.acceptVal.Sender, s.acceptVal.Receiver, s.acceptVal.Amount)
		s.acceptNum = 0
		s.acceptVal = nil
		sender_id := int(temp_acceptVal.Sender - 1)
		receiver_id := int(temp_acceptVal.Receiver - 1)
		cluster_id_sender := (sender_id * s.numClusters) / s.numClients
		cluster_id_receiver := (receiver_id * s.numClusters) / s.numClients
		x := fmt.Sprintf("C%d", temp_acceptVal.Sender)
		y := fmt.Sprintf("C%d", temp_acceptVal.Receiver)
		amt := temp_acceptVal.Amount

		if cluster_id_receiver != cluster_id_sender {
			if cluster_id_sender == s.cluster_id {
				s.AppendExecutedTxn(x, execTxn{TxID: int(temp_acceptVal.Timestamp), Txn: temp_acceptVal, Status: "P"})
				s.WAL[int32(temp_acceptVal.Timestamp)] = temp_acceptVal
				s.insertIntoWAL(temp_acceptVal.Timestamp, temp_acceptVal)
			}
			if cluster_id_receiver == s.cluster_id {
				s.AppendExecutedTxn(y, execTxn{TxID: int(temp_acceptVal.Timestamp), Txn: temp_acceptVal, Status: "P"})
				s.WAL[int32(temp_acceptVal.Timestamp)] = temp_acceptVal
				s.insertIntoWAL(temp_acceptVal.Timestamp, temp_acceptVal)

			}

		} else {
			_, err := s.ShardDB.Exec("UPDATE accounts SET locked = 0 WHERE id IN (?, ?)", x, y)
			if err != nil {
				log.Printf("[%s]failed to release locks: %v", s.name, err)
			} else {
				// log.Printf("[%s] Locks Released for %s and %s", s.name, x, y)

			}
			s.commitedTxns[int32(temp_acceptVal.Timestamp)] = temp_acceptVal
			// Serialize transactions to JSON
			s.AppendExecutedTxn(x, execTxn{TxID: int(temp_acceptVal.Timestamp), Txn: temp_acceptVal, Status: ""})

		}

		s.total_latency += time.Since(s.start)

		_, err := s.ShardDB.Exec(`
        UPDATE accounts
        SET balance = CASE
            WHEN id = ? THEN balance - ?
            WHEN id = ? THEN balance + ?
        END
        WHERE id IN (?, ?)`,
			x, amt, y, amt, x, y)
		if err != nil {
			s.ShardDB.Exec("UPDATE accounts SET locked = 0 WHERE id IN (?, ?)", x, y) // Release locks
			log.Printf("failed to perform transfer: %v", err)
		}

	}
	s.mu.Unlock()

	// s.addTransactionToDatastore(s.acceptVal, true)
	// s.last_commited_ballot_num = int64(len(s.datastore))

	// log.Printf("Last Commited Ballot Num for Server %s: %d", s.name, s.last_commited_ballot_num)

	return &pb.Empty{}, nil
}

func (s *server) insertIntoWAL(txn_timestamp int64, txn *pb.Transaction) {
	// log.Print(txn_timestamp)
	serialized, err := proto.Marshal(txn)
	if err != nil {
		log.Fatalf("Failed to serialize transaction: %v", err)
	}
	_, err = s.ShardDB.Exec(`INSERT OR REPLACE INTO WALTABLE (id, WAL) VALUES (?, ?)`, txn_timestamp, string(serialized))
	if err != nil {
		log.Fatalf("Failed to Update WAL transaction: %v", err)
	}

}

func (s *server) deleteFromWAL(txn_timestamp int64) {
	_, err := s.ShardDB.Exec(`DELETE FROM WALTABLE WHERE id = ?`, txn_timestamp)
	// _, err := s.ShardDB.Exec(`UPDATE WALTABLE SET WAL = ? WHERE id = ?`, nil, txn_timestamp)
	if err != nil {
		log.Fatalf("Failed to Delete WAL transaction: %v", err)
	}

}

func (s *server) fetchWAL(txn_timestamp int64) *pb.Transaction {
	var currentTxnData sql.NullString
	err := s.ShardDB.QueryRow(`SELECT WAL FROM WALTABLE WHERE id = (?)`, txn_timestamp).Scan(&currentTxnData)
	if err != nil {
		log.Printf("Error found 1 : %v", err)
		if err == sql.ErrNoRows {
			log.Print("WALTABLE with id not found")
			return nil
		}
	}
	var transactions pb.Transaction
	if currentTxnData.Valid {
		err = proto.Unmarshal([]byte(currentTxnData.String), &transactions)
		if err != nil {
		}
	}
	return &transactions
}

func (s *server) AppendExecutedTxn(sender string, newTransactions execTxn) error {
	var currentTxnData sql.NullString
	err := s.ShardDB.QueryRow(`SELECT executed_txns FROM accounts WHERE id = (?)`, sender).Scan(&currentTxnData)
	// log.Print(sender)
	if err != nil {
		log.Print("Error found")

		if err == sql.ErrNoRows {
			log.Print("Account with id not found")
			return nil
		}
		return err
	}
	var transactions []execTxn
	if currentTxnData.Valid {
		err = json.Unmarshal([]byte(currentTxnData.String), &transactions)
		if err != nil {
			return err
		}
	}
	transactions = append(transactions, newTransactions)
	updatedTxnData, err := json.Marshal(transactions)
	if err != nil {
		return err
	}
	_, err = s.ShardDB.Exec(`UPDATE accounts SET executed_txns = ?`, string(updatedTxnData))
	if err != nil {
		return err
	}
	return nil
}

func main() {
	fmt.Printf("Enter number of clusters:\n")
	var numClusters int
	fmt.Scan(&numClusters)
	fmt.Printf("Enter number of servers per cluster:\n")
	var serversPerCluster int
	fmt.Scan(&serversPerCluster)
	// serversPerCluster := 3
	numClients := 3000
	// totalDataItems := 3000
	shardSize := numClients / numClusters
	flag.Parse()

	clusters := make([]*Cluster, numClusters)
	for i := 0; i < numClusters; i++ {
		clusters[i] = &Cluster{id: i + 1}
		startID := i*shardSize + 1
		endID := (i + 1) * shardSize

		for j := 0; j < serversPerCluster; j++ {
			// db, _ := sql.Open("sqlite3", fmt.Sprintf("shard_C%d_S%d.db", i+1, 3*i+j+1))
			// log.Print(fmt.Sprintf("shard_C%d_S%d", i+1, serversPerCluster*i+j+1))
			db := CreateSQLiteDB(fmt.Sprintf("shard_C%d_S%d", i+1, serversPerCluster*i+j+1))
			InitializeShardData(db, startID, endID)
			server := &server{id: j + 1, ShardDB: db}
			clusters[i].servers = append(clusters[i].servers, server)
		}
	}
	for i := 0; i < numClusters*serversPerCluster; i++ {
		go func(idx int) {
			port := fmt.Sprintf("localhost:500%d", 50+idx+1)
			lis, err := net.Listen("tcp", port)
			if err != nil {
				log.Fatalf("Failed to listen on port %s: %v", port, err)
			}
			grpcServer := grpc.NewServer()
			db, _ := sql.Open("sqlite3", fmt.Sprintf("shard_C%d_S%d.db", (idx/serversPerCluster)+1, idx+1))
			s := &server{
				id:                idx + 1,
				name:              fmt.Sprintf("S%d", idx+1),
				numClusters:       numClusters,
				numClients:        numClients,
				serversPerCluster: serversPerCluster,
				ShardDB:           db,
				cluster_id:        idx / numClusters,
			}

			s.WAL = make(map[int32]*pb.Transaction)
			s.commitedTxns = make(map[int32]*pb.Transaction)
			s.servers = make([]*grpc.ClientConn, serversPerCluster)
			for id := range serversPerCluster {
				if idx != serversPerCluster*(idx/serversPerCluster)+id {
					// log.Printf("[%d] %s",idx+1,(fmt.Sprintf("localhost:5005%d", 3*(idx/numClusters)+id+1)))
					// log.Printf("[%s][%d] %d", s.name,idx,serversPerCluster*(idx/serversPerCluster)+id+1)
					conn, _ := grpc.NewClient(fmt.Sprintf("localhost:500%d", 50+serversPerCluster*(idx/serversPerCluster)+id+1), grpc.WithTransportCredentials(insecure.NewCredentials()))
					s.servers[id] = conn
				} else {
					s.servers[id] = nil
				}
			}
			pb.RegisterBankingServiceServer(grpcServer, s)
			log.Printf("[%s] gRPC server listening at %v", s.name, lis.Addr())
			if err := grpcServer.Serve(lis); err != nil {
				log.Fatalf("Failed to serve: %v", err)
			}
		}(i)
	}

	select {}

}
