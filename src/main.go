package main

import (
	"common"
	"database/sql"
	ec "errorcheck"
	"flag"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/cenkalti/rpc2"
	_ "github.com/go-sql-driver/mysql"
)

//ClientInfo 客户端的信息
type ClientInfo struct {
	Client  *rpc2.Client
	IsReady bool
}

var clients, taskRunners map[string]*ClientInfo
var db *sql.DB
var mutex sync.Mutex

func main() {
	flag.Parse()
	if flag.NArg() != 2 {
		panic("usage: host port") //检查参数是否正确
	}

	//初始化
	clients = make(map[string]*ClientInfo)
	taskRunners = make(map[string]*ClientInfo)
	srv := rpc2.NewServer()

	//连接数据库
	var err error
	db, err = sql.Open("mysql", "username:passwd@tcp(IP:port)/dbname/charset=utf8") //连接到数据库
	ec.CheckError(err, "Open database: ")

	//连接和断开连接时触发的事件
	srv.OnConnect(func(client *rpc2.Client) {
		fmt.Println("connect:")
	})
	srv.OnDisconnect(disconnect)

	//注册函数，使这些函数可以被远程调用
	srv.Handle("clientRegister", clientRegister)
	srv.Handle("taskRunnerRegister", taskRunnerRegister)
	srv.Handle("clientGetReady", clientGetReadyandAssignTask)
	srv.Handle("taskRunnerGetReady", taskRunnerGetReady)

	//初始化及开始监听
	hostAndPort := fmt.Sprintf("%s:%s", flag.Arg(0), flag.Arg(1)) //ip port
	serverAddr, err := net.ResolveTCPAddr("tcp", hostAndPort)
	ec.CheckError(err, "Resolving address:port failed: '"+hostAndPort+"'")
	lis, err := net.ListenTCP("tcp", serverAddr)
	ec.CheckError(err, "Listen TCP: ")
	go func() {
		srv.Accept(lis)
		defer lis.Close()
	}()

	time.Sleep(1 * time.Second)

	for { //TODO
		fmt.Println("Enter 'q' to quit ...")
		var input string
		fmt.Scanln(&input)

		if input == "q" {
			break
		}
	}
}

func clientRegister(client *rpc2.Client, args *common.Register, res *string) error {
	clients[args.ID] = &ClientInfo{Client: client}
	//sql
	stmt, err := db.Prepare(`INSERT INTO devices VALUES(?, ?, ?)`)
	ec.CheckError(err, "Prepare sql:")
	_, err = stmt.Exec(args.ID, nil, "idle") //TODO

	fmt.Println("Add a client: ")
	*res = "ok"
	return nil
}

func taskRunnerRegister(client *rpc2.Client, args *common.Register, res *string) error {
	taskRunners[args.ID] = &ClientInfo{Client: client}

	stmt, err := db.Prepare(`INSERT INTO taskrunners VALUES(?, ?, ?)`)
	ec.CheckError(err, "Prepare sql: ")
	_, err = stmt.Exec(args.ID, "idle") //TODO

	fmt.Println("Add a task runner: ")
	*res = "ok"
	return nil
}

func disconnect(client *rpc2.Client) {
	for k, v := range clients {
		if v.Client == client {
			delete(clients, k)
			fmt.Println("Disconnect:", k)
			break
		}
	}
}

func clientGetReadyandAssignTask(client *rpc2.Client, res *string) error { //TODO sql
	stmt, err := db.Prepare(`UPDATE ...`) //where 啥的加上
	ec.CheckError(err, "Prepare sql: ")
	_, err = stmt.Exec("ready") //加 where 的参数
	ec.CheckError(err, "Client get ready: ")

	var clientKey string
	for k, v := range clients {
		if v.Client == client {
			v.IsReady = true
			clientKey = k
			break
		}
	}

	var reply string
	client.Call("sendTask", &common.Args{}, &reply) //TODO 添加参数内容
	fmt.Println("Send task: ", reply)               //reply 的内容应为任务的地址

	var find bool
	mutex.Lock() //TODO 后续可优化为 sync.Map
	for _, v := range taskRunners {
		if v.IsReady == true {
			find = true
			v.IsReady = false
			v.Client.Call("getTask", &common.Args{}, &reply) //TODO 添加参数内容

			_, err = stmt.Exec("running") //加 where 的参数
			ec.CheckError(err, "Running task: ")

			fmt.Println("Get task: ", reply)
			break
		}
	}
	mutex.Unlock()
	if find {
		clients[clientKey].IsReady = false
		*res = "ok"
	} else {
		fmt.Println("Do not find ready task runner")
		*res = "fail"
	}

	return nil
}

func taskRunnerGetReady(client *rpc2.Client, args *common.Args, res *string) error {
	stmt, err := db.Prepare(`UPDATE ...`)
	ec.CheckError(err, "Prepare sql: ")
	_, err = stmt.Exec("ready")
	ec.CheckError(err, "Task runner get ready: ")

	for _, v := range taskRunners {
		if v.Client == client {
			v.IsReady = true
			break
		}
	}

	*res = "ok"
	return nil
}
