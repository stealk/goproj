package main

import (
	"fmt"
	"log"
	"math/big"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"time"

	"github.com/patrickmn/go-cache"
)

/*
Create a number:
{"jsonrpc":"1.0","method":"create","params":["grav_const", "0.000000000066731039356729"],"id":1}
Create another number:
{"jsonrpc":"1.0","method":"create","params":["planet_mass", "6416930923733925522307001.29472615"],"id":2}
Request calculation results:
{"jsonrpc":"1.0","method":"mul","params":["grav_const", "planet_mass"],"id":3}
{"jsonrpc":"1.0","method":"mul","params":["planet_mass", "0.5"],"id":4}
Update named number values:
{"jsonrpc":"1.0","method":"set","params":["grav_param", "428208470021099.94"],"id":5}
A number object may have multiple clients operating on it at any given time.
The server should be able to handle multiple concurrent requests, and provide error information when an operation fails.
{"jsonrpc":"1.0","method":"set","params":["altitude", "12623451902.239174235"],"id":6}
{"jsonrpc":"1.0","method":"set","params":["altitude", "12623451825.837853671"],"id":7}
{"jsonrpc":"1.0","method":"set","params":["altitude", "12623451746.495183227"],"id":8}

*/

type RpcObj struct {
	Jsonrpc float64  `json:"jsonrpc"`
	Method  string   `json:method`
	Params  []string `json:"params"`
	Id      int      `json:"id"`
}

type ReplyObj struct {
	Id     int    `json:"id"`
	Ok     bool   `json:"ok"`
	Msg    string `json:"msg"`
	Result string `json:result`
}

// ksy is (string), value is (*big.Float)
// TODO: using DB!!??
// var database = make(map[string]*big.Float)
var database = cache.New(5*time.Minute, 10*time.Minute)

type Arith int

const (
	ADD Arith = 0
	SUB Arith = 1
	MUL Arith = 2
	DIV Arith = 3
)

type ServerHandler struct{}

func (serverHandler ServerHandler) Serve(rpcObj RpcObj, returnObj *ReplyObj) error {
	log.Println("server\t-", "recive request, RpcObj:", rpcObj)
	if !VerifyRpcObj(&rpcObj) {
		FillReplay(returnObj, rpcObj.Id, false, "JsonrpcVersion not match", "")
		return nil
	}
	switch rpcObj.Method {
	case "create":
		Create(&rpcObj, returnObj)
	case "set":
		Set(&rpcObj, returnObj)
	case "delete":
		Delete(&rpcObj, returnObj)
	case "mul":
		Calc(&rpcObj, returnObj, MUL)
	case "add":
		Calc(&rpcObj, returnObj, ADD)
	case "sub":
		Calc(&rpcObj, returnObj, SUB)
	case "div":
		Calc(&rpcObj, returnObj, DIV)
	default:
		FillReplay(returnObj, rpcObj.Id, false, "method name not match", "")
	}
	return nil
}

func VerifyRpcObj(rpcObj *RpcObj) bool {
	if rpcObj.Jsonrpc != 1.0 {
		return false
	}
	return true
}

func FillReplay(returnObj *ReplyObj, id int, ok bool, msg string, result string) {
	returnObj.Id = id
	returnObj.Ok = ok
	returnObj.Msg = msg
	returnObj.Result = result
}

func Create(rpcObj *RpcObj, returnObj *ReplyObj) {
	key := rpcObj.Params[0]
	value := rpcObj.Params[1]
	// The key should not be a number
	_, keyIsNum := new(big.Float).SetString(key)
	if keyIsNum {
		FillReplay(returnObj, rpcObj.Id, false, "Create: Variable name should not be a number", "")
		return
	}

	_, exist := database.Get(key)
	if !exist {
		n := new(big.Float)
		n, ok := n.SetString(value)
		if !ok {
			FillReplay(returnObj, rpcObj.Id, false, "Create: The input string is not valid", "")
			return
		}
		database.Set(key, n, cache.DefaultExpiration)
		FillReplay(returnObj, rpcObj.Id, true, "Create: "+key+" success", "")
		return
	}
	FillReplay(returnObj, rpcObj.Id, false, key+" existed!! ", "")
}

func Set(rpcObj *RpcObj, returnObj *ReplyObj) {
	key := rpcObj.Params[0]
	value := rpcObj.Params[1]

	_, exist := database.Get(key)
	if exist {
		n := new(big.Float)
		n, ok := n.SetString(value)
		if !ok {
			fmt.Println("SetString: error")
			FillReplay(returnObj, rpcObj.Id, false, "Set: The input string is not valid", "")
			return
		}
		database.Set(key, n, cache.DefaultExpiration)
		FillReplay(returnObj, rpcObj.Id, true, "Set: "+key+" success", "")
		return
	}
	FillReplay(returnObj, rpcObj.Id, false, key+" does not exist!! ", "")
}

func Calc(rpcObj *RpcObj, returnObj *ReplyObj, op Arith) {
	key1 := rpcObj.Params[0]
	key2 := rpcObj.Params[1]

	// Check whether the key1 and key2 are key or number
	n1, exist1 := database.Get(key1)
	n2, exist2 := database.Get(key2)

	if !exist1 { // key1 is not a valid key, try parse key1 as number
		n := new(big.Float)
		n, ok := n.SetString(key1)
		if !ok {
			// not a key, not a number, retrun error
			fmt.Println("Params[0] error, expired or invalid")
			FillReplay(returnObj, rpcObj.Id, false, "Params[0] error, expired or invalid", "")
			return
		}
		n1 = n
	}
	if !exist2 { // key2 is not a valid key, try parse key2 as number
		n := new(big.Float)
		n, ok := n.SetString(key2)
		if !ok {
			// not a key, not a number, retrun error
			fmt.Println("Params[1] error, expired or invalid")
			FillReplay(returnObj, rpcObj.Id, false, "Params[1] error, expired or invalid", "")
			return
		}
		n2 = n
	}

	// Compute n1 op n2
	var result *big.Float
	switch op {
	case MUL:
		result = new(big.Float).Mul(n1.(*big.Float), n2.(*big.Float))
	case ADD:
		result = new(big.Float).Add(n1.(*big.Float), n2.(*big.Float))
	case SUB:
		result = new(big.Float).Sub(n1.(*big.Float), n2.(*big.Float))
	case DIV: // div 0?
		result = new(big.Float).Quo(n1.(*big.Float), n2.(*big.Float))
	}
	FillReplay(returnObj, rpcObj.Id, true, "Calculate success", result.String())
}

func Delete(rpcObj *RpcObj, returnObj *ReplyObj) {
	key := rpcObj.Params[0]

	_, exist := database.Get(key)
	if exist {
		database.Delete(key)
		FillReplay(returnObj, rpcObj.Id, true, "Delete: "+key+" success", "")
		return
	}
	FillReplay(returnObj, rpcObj.Id, false, "Delete: "+key+" not existed!! ", "")
}

func startServer() {
	// create server
	server := rpc.NewServer()

	// listen port 9999
	listener, err := net.Listen("tcp", ":9999")
	if err != nil {
		log.Fatal("server\t-", "listen error:", err.Error())
	}
	defer listener.Close()
	log.Println("server\t-", "start listion on port 9999")

	// serverHandler
	serverHandler := &ServerHandler{}
	server.Register(serverHandler)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err.Error())
		}

		go server.ServeCodec(jsonrpc.NewServerCodec(conn))
	}
}

func main() {
	// start the server
	go startServer()

	// TODO: try some client code here
	callRpcBySynchronous()
	callRpcByAsynchronous()
}

//
func callRpcBySynchronous() {
	log.Println("client\t-", "callRpcBySynchronous")

	client, err := net.DialTimeout("tcp", "localhost:9999", 30*time.Second) // 30 seconds
	if err != nil {
		log.Fatal("client\t-", err.Error())
	}
	defer client.Close()

	clientRpc := jsonrpc.NewClient(client)

	var reply ReplyObj
	log.Println("client\t-", "call Serv method")

	clientRpc.Call("ServerHandler.Serve",
		&RpcObj{Jsonrpc: 1.2, Method: "create", Params: []string{"ggg", "12623451902.239174235"}, Id: 0},
		&reply)
	if reply.Ok {
		log.Fatal("Create success with Jsonrpc: 1.2", reply)
	}
	if reply.Id != 0 {
		log.Fatal("ID error", reply)
	}
	clientRpc.Call("ServerHandler.Serve",
		&RpcObj{Jsonrpc: 1.0, Method: "create", Params: []string{"ggg", "12623451902.239174235"}, Id: 999},
		&reply)
	if !reply.Ok {
		log.Fatal("Create failed", reply)
	}
	clientRpc.Call("ServerHandler.Serve",
		&RpcObj{Jsonrpc: 1.0, Method: "create", Params: []string{"ggg", "555"}, Id: 999},
		&reply)
	if reply.Ok {
		log.Fatal("Double create success", reply)
	}
	clientRpc.Call("ServerHandler.Serve",
		&RpcObj{Jsonrpc: 1.0, Method: "create", Params: []string{"hhh", "12623451902.239174235"}, Id: 999},
		&reply)
	if !reply.Ok {
		log.Fatal("Create hhh failed", reply)
	}
	clientRpc.Call("ServerHandler.Serve",
		&RpcObj{Jsonrpc: 1.0, Method: "sub", Params: []string{"ggg", "hhh"}, Id: 1},
		&reply)
	if !reply.Ok {
		log.Fatal("sub failed", reply)
	}
	clientRpc.Call("ServerHandler.Serve",
		&RpcObj{Jsonrpc: 1.0, Method: "set", Params: []string{"ggg", "3"}, Id: 555},
		&reply)
	if !reply.Ok {
		log.Fatal("set failed", reply)
	}
	clientRpc.Call("ServerHandler.Serve",
		&RpcObj{Jsonrpc: 1.0, Method: "set", Params: []string{"ggg", "asd"}, Id: 555},
		&reply)
	if reply.Ok {
		log.Fatal("set success", reply)
	}
	clientRpc.Call("ServerHandler.Serve",
		&RpcObj{Jsonrpc: 1.0, Method: "set", Params: []string{"popopop", "asd"}, Id: 888},
		&reply)
	if reply.Ok {
		log.Fatal("set popopop success", reply)
	}
	clientRpc.Call("ServerHandler.Serve",
		&RpcObj{Jsonrpc: 1.0, Method: "mul", Params: []string{"ggg", "0.005"}, Id: 1},
		&reply)
	if !reply.Ok {
		log.Fatal("mul failed", reply)
	}
	clientRpc.Call("ServerHandler.Serve",
		&RpcObj{Jsonrpc: 1.0, Method: "add", Params: []string{"1", "0.005"}, Id: 1},
		&reply)
	if !reply.Ok || reply.Result != "1.005" {
		log.Fatal("add failed", reply)
	}
	clientRpc.Call("ServerHandler.Serve",
		&RpcObj{Jsonrpc: 1.0, Method: "add", Params: []string{"1", "abc"}, Id: 1},
		&reply)
	if reply.Ok {
		log.Fatal("add abc success", reply)
	}

	clientRpc.Call("ServerHandler.Serve",
		&RpcObj{Jsonrpc: 1.0, Method: "delete", Params: []string{"ggg", "0.005"}, Id: 1},
		&reply)
	if !reply.Ok {
		log.Fatal("delete failed", reply)
	}
	clientRpc.Call("ServerHandler.Serve",
		&RpcObj{Jsonrpc: 1.0, Method: "delete", Params: []string{"ggg", "0.005"}, Id: 1},
		&reply)
	if reply.Ok {
		log.Fatal("double delete success", reply)
	}

	clientRpc.Call("ServerHandler.Serve",
		&RpcObj{Jsonrpc: 1.0, Method: "mul", Params: []string{"ggg", "hhh"}, Id: 1},
		&reply)
	if reply.Ok {
		log.Fatal("ggg mul hhh success", reply)
	}
	clientRpc.Call("ServerHandler.Serve",
		&RpcObj{Jsonrpc: 1.0, Method: "div", Params: []string{"5", "2.5"}, Id: 10},
		&reply)
	if !reply.Ok || reply.Result != "2" {
		log.Fatal("div failed", reply)
	}

	clientRpc.Call("ServerHandler.Serve",
		&RpcObj{Jsonrpc: 1.0, Method: "xxx", Params: []string{"hhh", "0"}, Id: 12},
		&reply)
	if reply.Ok {
		log.Fatal("method name mismatch", reply)
	}
}

func callRpcByAsynchronous() {

	client, err := net.DialTimeout("tcp", "localhost:9999", 30*time.Second) // 30 seconds
	if err != nil {
		log.Fatal("client\t-", err.Error())
	}
	defer client.Close()

	clientRpc := jsonrpc.NewClient(client)

	endChan := make(chan int, 15)

	for i := 1; i <= 15; i++ {
		var rpcObj = &RpcObj{Jsonrpc: 1.0, Method: "sub", Params: []string{"0.3", "0.005"}, Id: 999}

		divCall := clientRpc.Go("ServerHandler.Serve", &rpcObj, &ReplyObj{}, nil)
		//
		go func(num int) {
			reply := <-divCall.Done
			log.Println("client\t-", "recive remote return by Asynchronous", reply.Reply)
			endChan <- num
		}(i)
	}

	// wait for all the responses
	for i := 1; i <= 15; i++ {
		_ = <-endChan
	}
	log.Println("client\t-", "callRpcByAsynchronous end")
}
