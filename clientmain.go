package main

// Code From Vansh Jain
// DS Lab 3
// SBU ID - 116713519
import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	pb "project1/proto"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var numClients = 3000
var numClusters = 3
var serversPerCluster = 3
var shardSize = numClients / numClusters

var clients = make([]ClientInfo, numClients)

type ClientInfo struct {
	name      string
	client_id int
	clusters  []*Cluster
}

type Transaction struct {
	timestamp     int64
	sender        int32
	receiver      int32
	amount        float64
	serverStatus  []int64
	contactServer []string
}

type Block struct {
	blockId  int
	TxnBlock []Transaction
}

type Cluster struct {
	id      int
	servers []pb.BankingServiceClient
}

func parseCSV(filePath string, num_servers int) ([]Block, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// url := filePath

	// Step 1: Make HTTP GET request
	// file, err := http.Get(url)
	// if err != nil {
	// 	log.Fatalf("Failed to get CSV from URL: %v", err)
	// }
	// defer file.Body.Close()

	// r := csv.NewReader(file.Body)
	r := csv.NewReader(file)
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	// log.Print(records)
	var TransactionOrder []Block
	var BlockTransaction []Transaction
	blockId := 1
	serverStatus_txn := make([]int64, num_servers)
	contact_server := []string{}
	for id, row := range records {
		x := strings.Split(strings.Trim(row[1], "()"), ",")
		amount, err := strconv.ParseFloat(strings.TrimSpace(x[2]), 64)
		if err != nil {
			log.Fatal(err)
		}

		if id == 0 {
			blockId = 1
			for _, i := range strings.Split(strings.Trim(row[2], "[]"), ",") {
				i = strings.TrimSpace(i)
				i, _ := strconv.Atoi(strings.TrimPrefix(i, "S"))
				serverStatus_txn[i-1] = 1
			}
			contact_server = strings.Split(strings.Trim(row[3], "[]"), ",")
		}

		if len(row[2]) != 0 && id != 0 {
			blockId += 1
			TransactionOrder = append(TransactionOrder, Block{blockId: blockId - 1, TxnBlock: BlockTransaction})

			BlockTransaction = []Transaction{}
			serverStatus_txn = make([]int64, num_servers)
			for _, i := range strings.Split(strings.Trim(row[2], "[]"), ",") {
				i = strings.TrimSpace(i)
				i, _ := strconv.Atoi(strings.TrimPrefix(i, "S"))
				serverStatus_txn[i-1] = 1
			}
			contact_server = strings.Split(strings.Trim(row[3], "[]"), ",")
		}
		copyServerStat := make([]int64, len(serverStatus_txn))
		copy(copyServerStat, serverStatus_txn[:])
		s, _ := strconv.Atoi(strings.TrimSpace(x[0]))
		r, _ := strconv.Atoi(strings.TrimSpace(x[1]))
		abd := Transaction{sender: int32(s), receiver: int32(r), amount: float64(amount), serverStatus: copyServerStat, contactServer: contact_server}
		BlockTransaction = append(BlockTransaction, abd)
	}
	TransactionOrder = append(TransactionOrder, Block{blockId: blockId, TxnBlock: BlockTransaction})
	return TransactionOrder, nil
}

var mu sync.Mutex

type client struct {
	pb.UnimplementedBankingServiceServer
	clientId      int
	Clientbalance map[string]float32
}

func (c *client) TransferMoneyResponse(ctx context.Context, in *pb.Reply) (*pb.Empty, error) {
	mu.Lock()
	defer mu.Unlock()

	return &pb.Empty{}, nil
}

func executeTransactions(transaction_block []Transaction) {
	for _, block := range transaction_block {
		go func(block Transaction) {
			sender_id := int(block.sender)
			receiver_id := int(block.receiver)
			block.timestamp = int64(time.Now().Nanosecond())
			log.Printf("Initiating transfer from %d to %d on %v of amount %.2f\n", block.sender, block.receiver, block.timestamp, block.amount)

			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
			defer cancel()
			cluster_id_sender := ((sender_id - 1) * numClusters) / numClients
			cluster_id_receiver := ((receiver_id - 1) * numClusters) / numClients

			if cluster_id_sender == cluster_id_receiver {
				// fmt.Print("\nIntra Shard Transaction\n")
				randServer, _ := strconv.Atoi(string(strings.TrimSpace(block.contactServer[cluster_id_sender])[1]))
				result, _ := clients[sender_id].clusters[cluster_id_sender].servers[(randServer-1)%serversPerCluster].TransferMoney(ctx, &pb.TransferRequest{
					Txn: &pb.Transaction{Sender: block.sender,
						Timestamp: block.timestamp,
						Receiver:  block.receiver,
						Amount:    float32(block.amount)},
				})
				log.Print(result)
			} else {
				var wg sync.WaitGroup
				result_sender := false
				result_receiver := false
				// fmt.Print("\nCross Shard Transaction\n")
				wg.Add(1)
				go func(sender_id int, sender *Cluster) {
					defer wg.Done()
					randServer, _ := strconv.Atoi(string(strings.TrimSpace(block.contactServer[cluster_id_sender])[1]))

					// randServer := (int(block.contactServer[sender_id][1]) - 1) % serversPerCluster
					// log.Print(randServer)
					temp_result, _ := sender.servers[(randServer-1)%serversPerCluster].TransferMoney(ctx, &pb.TransferRequest{
						Txn: &pb.Transaction{Sender: block.sender,
							Timestamp: block.timestamp,
							Receiver:  block.receiver,
							Amount:    float32(block.amount)},
					})
					// log.Printf("Cluster:%d Server:%d", sender.id, randServer)
					// log.Printf("Cluster:%d Server:%d ---Result: %v", sender.id, randServer, temp_result)
					if temp_result != nil {
						result_sender = temp_result.Success
						log.Printf("Sender Result: %v", temp_result)

					}
				}(cluster_id_sender, clients[sender_id].clusters[cluster_id_sender])
				wg.Add(1)
				go func(receiver_id int, receiver *Cluster) {
					defer wg.Done()
					randServer, _ := strconv.Atoi(string(strings.TrimSpace(block.contactServer[cluster_id_receiver])[1]))

					// randServer := (int(block.contactServer[cluster_id_receiver][1]) - 1) % serversPerCluster
					// log.Print(randServer)
					temp_result, _ := receiver.servers[(randServer-1)%serversPerCluster].TransferMoney(ctx, &pb.TransferRequest{
						Txn: &pb.Transaction{Sender: block.sender,
							Timestamp: block.timestamp,
							Receiver:  block.receiver,
							Amount:    float32(block.amount)},
					})
					// log.Printf("Cluster:%d Server:%d", receiver.id, randServer)
					// log.Printf("Cluster:%d Server:%d ---Result: %v", receiver.id, randServer, temp_result)
					if temp_result != nil {
						result_receiver = temp_result.Success
						log.Printf("Receiver Result: %v", temp_result)

					}
				}(cluster_id_receiver, clients[sender_id].clusters[cluster_id_receiver])
				wg.Wait()
				time.Sleep(5 * time.Millisecond)
				if result_sender && result_receiver {
					log.Print("SENDING COMMIT TO ALL 6 SERVERS")
					go func(sender *Cluster) {
						for _, srv := range sender.servers {
							go func(server pb.BankingServiceClient) {
								ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
								defer cancel()
								reply, _ := server.PCCommit(ctx, &pb.PCCommitMessage{TxnCorresponding: int32(block.timestamp)})
								for reply == nil {
									log.Print("RETRYING COMMIT TO SERVER")
									ctx, cancel := context.WithTimeout(context.Background(), 2000*time.Millisecond)
									defer cancel()
									reply, _ = server.PCCommit(ctx, &pb.PCCommitMessage{TxnCorresponding: int32(block.timestamp)})
								}
								// log.Print(reply)

							}(srv)
						}
					}(clients[sender_id].clusters[cluster_id_sender])
					go func(sender *Cluster) {
						for _, srv := range sender.servers {
							go func(server pb.BankingServiceClient) {
								ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
								defer cancel()
								reply, _ := server.PCCommit(ctx, &pb.PCCommitMessage{TxnCorresponding: int32(block.timestamp)})
								for reply == nil {
									log.Print("RETRYING COMMIT TO SERVER")
									ctx, cancel = context.WithTimeout(context.Background(), 2000*time.Millisecond)
									defer cancel()
									reply, _ = server.PCCommit(ctx, &pb.PCCommitMessage{TxnCorresponding: int32(block.timestamp)})
									time.Sleep(5 * time.Second)
								}
								// log.Printf("Sender Result: %v", reply)
							}(srv)
						}
					}(clients[sender_id].clusters[cluster_id_receiver])

				} else {
					log.Print("SENDING ABORT TO ALL 6 SERVERS")

					go func(sender *Cluster) {
						for _, srv := range sender.servers {
							go func(server pb.BankingServiceClient) {
								ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
								defer cancel()
								reply, _ := server.PCAbort(ctx, &pb.PCAbortMessage{TxnCorresponding: int32(block.timestamp)})
								for reply == nil {
									log.Print("RETRYING ABORT TO SERVER")
									ctx, _ = context.WithTimeout(context.Background(), 2000*time.Millisecond)
									reply, _ = server.PCAbort(ctx, &pb.PCAbortMessage{TxnCorresponding: int32(block.timestamp)})
								}
								// log.Printf("Receiver Result: %v", reply)
							}(srv)
						}
					}(clients[sender_id].clusters[cluster_id_sender])
					go func(sender *Cluster) {
						for _, srv := range sender.servers {
							go func(server pb.BankingServiceClient) {
								ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
								defer cancel()
								reply, _ := server.PCAbort(ctx, &pb.PCAbortMessage{TxnCorresponding: int32(block.timestamp)})
								for reply == nil {
									log.Print("RETRYING ABORT TO SERVER")
									ctx, _ = context.WithTimeout(context.Background(), 2000*time.Millisecond)
									reply, _ = server.PCAbort(ctx, &pb.PCAbortMessage{TxnCorresponding: int32(block.timestamp)})

								}
								// log.Print(reply)
							}(srv)
						}
					}(clients[sender_id].clusters[cluster_id_receiver])

				}

			}

		}(block)
		sender_id := int(block.sender)
		receiver_id := int(block.receiver)
		cluster_id_sender := ((sender_id - 1) * numClusters) / numClients
		cluster_id_receiver := ((receiver_id - 1) * numClusters) / numClients
		time.Sleep(1 * time.Millisecond)
		if cluster_id_receiver != cluster_id_sender {
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func main() {
	fmt.Printf("Enter number of clusters:\n")
	var numC int
	fmt.Scan(&numC)
	fmt.Printf("Enter number of servers per cluster:\n")
	var serversPCluster int
	fmt.Scan(&serversPCluster)

	numClusters = numC
	serversPerCluster = serversPCluster
	shardSize = numClients / numClusters

	csvFilePath := "/Users/vansh/Desktop/SBU/DS_GIT/2pc-VanshJain02/Test_Cases_-_Lab3.csv"
	// csvFilePath := "test.csv"
	// csvFilePath := "lab1_Test.csv"
	// csvFilePath := "https://drive.google.com/uc?export=download&id=1Cj06sjBfH7BVFPcMA65v7gBmzR42RQkP"
	// csvFilePath := "vansh_test.csv"
	flag.Parse()
	clientTransactions, err := parseCSV(csvFilePath, numClusters*serversPerCluster)
	if err != nil {
		log.Fatalf("Failed to parse CSV: %v", err)
	}

	var wg sync.WaitGroup
	doneChan := make(chan bool, numClients)

	clusters := make([]*Cluster, numClusters)

	for i := 0; i < numClusters; i++ {
		clusters[i] = &Cluster{id: i + 1}
		// startID := i*(shardSize) + 1
		// endID := (i + 1) * shardSize

		for j := 0; j < serversPerCluster; j++ {
			conn, err := grpc.NewClient(fmt.Sprintf("localhost:500%d", 50+serversPerCluster*i+j+1), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Fatalf(" did not connect: %v", err)
			}
			clusters[i].servers = append(clusters[i].servers, pb.NewBankingServiceClient(conn))
		}
	}

	for i := 0; i < numClients; i++ {
		clientInfo := ClientInfo{
			name:      fmt.Sprintf("C%d", i+1),
			client_id: i + 1,
		}
		clientInfo.clusters = clusters
		clients[i] = clientInfo
	}

	i := 0
	// for idx, listen_addr := range client_listen_addr {
	// 	go func(idx int, addr string) {
	// 		lis, err := net.Listen("tcp", "localhost:"+addr)
	// 		if err != nil {
	// 			log.Fatalf("failed to listen on port %v: %v", addr, err)
	// 		}
	// 		grpcServer := grpc.NewServer()
	// 		client_obj := &client{
	// 			clientId: clients[idx].client_id,
	// 		}
	// 		pb.RegisterBankingServiceServer(grpcServer, client_obj)
	// 		log.Printf("gRPC server listening at %v", lis.Addr())
	// 		if err := grpcServer.Serve(lis); err != nil {
	// 			log.Fatalf("failed to serve: %v", err)
	// 		}
	// 	}(idx, listen_addr)
	// }

	for i < len(clientTransactions)+1 {

		fmt.Println("\nChoose an option:")
		if i < len(clientTransactions) {
			fmt.Println("1. Proceed to a New Block of Transactions")
		}
		fmt.Println("2. Print Balance")
		fmt.Println("3. Print Datastore")
		fmt.Println("4. Print Performance")
		fmt.Println("5. Exit")

		var option int
		fmt.Scan(&option)

		switch option {
		case 1:

			if i < len(clientTransactions) {
				var wg sync.WaitGroup
				for j := 0; j < numClusters; j++ {
					for k := 0; k < serversPerCluster; k++ {
						wg.Add(1)
						go func() {
							defer wg.Done()
							ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
							defer cancel()
							clients[0].clusters[j].servers[k].SyncServerStatus(ctx, &pb.ServerStatus{ServerStatus: clientTransactions[i].TxnBlock[0].serverStatus})
						}()
					}
				}
				wg.Wait()
				executeTransactions(clientTransactions[i].TxnBlock)
			}
			i++
		case 2:
			fmt.Printf("Enter the Client ID: \n")
			var ch int
			fmt.Scan(&ch)
			for idx, i := range clients[0].clusters[int(((ch-1)*numClusters)/numClients)].servers {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()
				balance, _ := i.PrintBalance(ctx, &pb.PrintBalanceRequest{ClientId: int32(ch)})
				// performance, _ := i.server[mrand.Intn(len(i.server))].PrintPerformance(ctx, &pb.Empty{})
				fmt.Printf("Server %s Client C%d Balance: %v\n", "S"+strconv.Itoa(3*int(((ch-1)*numClusters)/numClients)+idx+1), ch, balance.Balance)
			}

		case 3:
			for j := 0; j < numClusters; j++ {
				for k := 0; k < serversPerCluster; k++ {
					ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
					defer cancel()
					log, _ := clients[0].clusters[j].servers[k].PrintLog(ctx, &pb.Empty{})
					fmt.Printf("\n***** Log of Server S%d *****\n", 3*j+k+1)
					for _, txn := range log.Logs {
						fmt.Printf("%v\n", txn)
					}
					fmt.Print("\n")

					// performance, _ := i.server[mrand.Intn(len(i.server))].PrintPerformance(ctx, &pb.Empty{})

				}
			}

		case 4:

			for j := 0; j < numClusters; j++ {
				for k := 0; k < serversPerCluster; k++ {
					ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
					defer cancel()
					performance, _ := clients[0].clusters[j].servers[k].PrintPerformance(ctx, &pb.Empty{})
					// performance, _ := i.server[mrand.Intn(len(i.server))].PrintPerformance(ctx, &pb.Empty{})
					if performance.TotalTransactions > 0 {
						avgLatency := time.Duration(performance.TotalLatency) / time.Duration(performance.TotalTransactions)
						fmt.Printf("Server %s Performance:\n", "S"+strconv.Itoa(3*j+k+1))
						fmt.Printf("  Total Transactions: %d\n", performance.TotalTransactions)
						fmt.Printf("  Total Latency: %s\n", time.Duration(performance.TotalLatency))
						fmt.Printf("  Average Latency per Transaction: %s\n", avgLatency)
						fmt.Printf("  Throughput (Txns/sec): %.2f\n\n", float64(performance.TotalTransactions)/time.Duration(performance.TotalLatency).Seconds())
					} else {
						fmt.Printf("The Server %s has 0 transactions, hence cannot show performance metrics\n\n", "S"+strconv.Itoa(3*j+k+1))
					}
				}
			}
		case 5:
			i++
			return
		default:
			fmt.Println("Invalid option. Please try again.")
		}

	}
	go func() {
		lis, err := net.Listen("tcp", "localhost:8001")
		if err != nil {
			log.Fatalf("failed to listen on port %v: %v", "8001", err)
		}
		grpcServer := grpc.NewServer()
		client_obj := &client{
			clientId: clients[0].client_id,
		}
		pb.RegisterBankingServiceServer(grpcServer, client_obj)
		log.Printf("gRPC server listening at %v", lis.Addr())
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	wg.Wait()
	close(doneChan)
}
