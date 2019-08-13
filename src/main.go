package main

import (
	"common"
	"database/sql"
	ec "errorcheck"
	"flag"
	"fmt"
	"net"
	"time"

	"github.com/cenkalti/rpc2"
	_ "github.com/go-sql-driver/mysql"
)

type ClientInfo struct {
	Client *rpc2.Client
}

var clients, taskRunners map[string]*ClientInfo
var db *sql.DB

func main() {
	flag.Parse()
	if flag.NArg() != 2 {
		panic("usage: host port") //检查参数是否正确
	}

	clients = make(map[string]*ClientInfo)
	taskRunners = make(map[string]*ClientInfo)

	var err error
	db, err = sql.Open("mysql", "username:passwd@tcp(IP:port)/dbname/charset=utf8") //连接到数据库
	ec.CheckError(err, "Open database: ")

	srv := rpc2.NewServer()

	srv.OnConnect(func(client *rpc2.Client) {
		fmt.Println("connect:")
	})
	srv.OnDisconnect(disconnect)

	srv.Handle("clientRegister", clientRegister)
	srv.Handle("taskRunnerRegister", taskRunnerRegister)
	srv.Handle("clientGetReady", clientGetReady)

	hostAndPort := fmt.Sprintf("%s:%s", flag.Arg(0), flag.Arg(1)) //ip port
	serverAddr, err := net.ResolveTCPAddr("tcp", hostAndPort)
	ec.CheckError(err, "Resolving address:port failed: '"+hostAndPort+"'")
	go func() {
		lis, err := net.ListenTCP("tcp", serverAddr)
		ec.CheckError(err, "Listen TCP: ")
		srv.Accept(lis)
	}()

	time.Sleep(1 * time.Second)

	for { //TODO

	}
}

func clientRegister(client *rpc2.Client, args *common.Register, res *string) error { //TODO
	clients[args.ID] = &ClientInfo{Client: client}
	//sql
	stmt, err := db.Prepare(`INSERT INTO devices VALUES(?, ?, ?)`)
	ec.CheckError(err, "Prepare sql:")
	_, err = stmt.Exec(args.ID, nil, "idle")

	fmt.Println("Add a client: ")
	*res = "ok"
	return nil
}

func taskRunnerRegister(client *rpc2.Client, res *string) error { //TODO
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

func clientGetReady(client *rpc2.Client, res *string) error { //TODO sql
	stmt, err := db.Prepare(`UPDATE ...`)
	ec.CheckError(err, "Prepare sql: ")
	_, err = stmt.Exec("ready")
	ec.CheckError(err, "client get ready: ")

	return nil
}
