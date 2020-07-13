package main

import (
	"bb_crontab/worker"
	"flag"
	"fmt"
	"runtime"
	"time"
)

var (
	confFile string
)

//解析命令行参数
func initArgs() {
	//master-config ./master.json
	flag.StringVar(&confFile, "config", "./worker.json", "指定worker.json")
	flag.Parse()
}

//初始化线程数
func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	var (
		err error
	)
	//初始化命令行参数
	initArgs()
	//初始化线程
	initEnv()

	//加载配置
	if err = worker.InitConfig(confFile); err != nil {
		goto ERR
	}

	//启动执行器
	if err = worker.InitExecutor(); err != nil {
		goto ERR
	}

	//启动调度器
	if err = worker.InitScheduler(); err != nil {
		goto ERR
	}
	//初始化任务管理器
	if err = worker.InitJobMgr(); err != nil {
		goto ERR
	}
	for {
		time.Sleep(1 * time.Second)
	}
ERR:
	fmt.Println(err)
}
