Distributed Systems Lab 3

Name: Vansh Jain

SBU ID: 116713519


The project is to handle a PAXOS Consensus protocol which can achieve consensus with atleast 2f+1 nodes.
This is a modified version of PAXOS which includes CatchUp Mechanism, Quorum Construction, Data loss, Proposes AcceptVal, Datastore Consistency and Log Consistency.
It includes server implementations using RPC calls.

To run this project you must open 6 terminals: (5 terminals for 5 Servers) and (1 terminal for 5 Clients).

To run this project, run the following commands:-

1. go run server/main.go --port 50051 --id 0 --name S1
2. go run server/main.go --port 50052 --id 1 --name S2
3. go run server/main.go --port 50053 --id 2 --name S3
4. go run server/main.go --port 50054 --id 3 --name S4
5. go run server/main.go --port 50055 --id 4 --name S5
   
Now all your servers might be active and running.

6. go run clientmain.go

NOTE: To compile the protofile, run this command:-
1. cd proto
2. protoc --go_out=. --go-grpc_out=. proto.proto



   
This line should start your client portal.

Start your transactions by typing 1 and pressing enter. This will lead to the first set of transaction block. Then you can start monitoring the performance metrics, logs,datastore and client balances. You can initiate the new set of blocks by typing 1 and pressing enter button.

All the Design and Implementation Thoughts have been implemented, moreover the bonus 3 has also been implemented which can viewed on the client side menu (case 6)










