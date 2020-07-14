package master

import (
	"bb_crontab/common"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"
)

//任务的HTTP接口
type ApiServer struct {
	httpServer *http.Server
}

var (
	//单利对象
	G_apiServer *ApiServer
)

//保存任务接口
//POSTjob = {"name":"job1","commond":"echo hello","cronExpr":"* * * * *"}
func handleJobSave(w http.ResponseWriter, r *http.Request) {

	log.Println("进入")
	//任务保存在ETCD中
	var (
		err    error
		posJob string
		job    common.Job
		oldJob *common.Job
		bytes  []byte
	)
	//1.解析POST表单
	if err = r.ParseForm(); err != nil {
		goto ERR
	}
	//2.取表单中的job字段
	posJob = r.PostForm.Get("job")

	//3、反序列化job
	if err = json.Unmarshal([]byte(posJob), &job); err != nil {
		goto ERR
	}
	//4、保存到etcd
	if oldJob, err = G_jobMgr.SaveJob(&job); err != nil {
		goto ERR
	}

	//5、返回正常应答
	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		w.Write(bytes)
	}

	return
ERR:
	//6、返回异常应答
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
}

func handleJobDelete(w http.ResponseWriter, r *http.Request) {
	var (
		err    error
		name   string
		oldJob *common.Job
		bytes  []byte
	)
	//POST
	if err = r.ParseForm(); err != nil {
		goto ERR
	}
	//删除的任务名
	name = r.PostForm.Get("name")

	//去删除任务
	if oldJob, err = G_jobMgr.DeleteJob(name); err != nil {
		goto ERR
	}
	//5、返回正常应答
	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		w.Write(bytes)
	}
	return
ERR:
	//6、返回异常应答
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
}

//获取任务列表
func handleJobList(w http.ResponseWriter, r *http.Request) {
	var (
		jobList []*common.Job
		err     error
		bytes   []byte
	)
	//获取job列表
	if jobList, err = G_jobMgr.ListJob(); err != nil {
		goto ERR
	}

	//5、返回正常应答
	if bytes, err = common.BuildResponse(0, "success", jobList); err == nil {
		w.Write(bytes)
	}
	return
ERR:
	//6、返回异常应答
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
}

func handleJobKill(w http.ResponseWriter, r *http.Request) {
	var (
		err   error
		name  string
		bytes []byte
	)
	if err = r.ParseForm(); err != nil {
		goto ERR
	}
	//解析名字
	name = r.PostForm.Get("name")
	//杀死任务
	G_jobMgr.killJob(name)
	//5、返回正常应答
	if bytes, err = common.BuildResponse(0, "success", nil); err == nil {
		w.Write(bytes)
	}
	return
ERR:
	//6、返回异常应答
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}

}

//初始化服务
func InigApiServer() (err error) {
	var (
		mux           *http.ServeMux
		listen        net.Listener
		httpServer    *http.Server
		staticDir     http.Dir
		staticHandler http.Handler
	)
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDelete)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/kill", handleJobKill)

	//静态文件目录
	staticDir = http.Dir(G_config.WebRoot)
	staticHandler = http.FileServer(staticDir)
	mux.Handle("/", http.StripPrefix("/", staticHandler))

	//启动tcp监听
	if listen, err = net.Listen("tcp", ":"+strconv.Itoa(G_config.Apipoint)); err != nil {
		return
	}
	//创建一个HTTP服务
	httpServer = &http.Server{
		ReadTimeout:  time.Duration(G_config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.ApiWriteTimeout) * time.Millisecond,
		Handler:      mux,
	}
	G_apiServer = &ApiServer{
		httpServer: httpServer,
	}

	go httpServer.Serve(listen)
	return

}
