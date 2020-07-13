package common

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/gorhill/cronexpr"
)

//定时任务
type Job struct {
	Name     string `json:"name"`
	Command  string `json:"command"`
	CronExpr string `json:"cronExpr"`
}

//任务调度计划
type JobSchedulePlan struct {
	Job      *Job                 //调度的任务信息
	Expr     *cronexpr.Expression //解析好的cronexpr表达式
	NextTime time.Time            //下次执行时间
}

//任务执行状态
type JobExecuteInfo struct {
	Job        *Job               //任务信息
	PlanTime   time.Time          //计划执行时间
	RealTime   time.Time          //实际执行时间
	CancelCtx  context.Context    //任务command的context
	CancelFunc context.CancelFunc //用于取消command执行的任务
}

//HTTP 接口应答
type Response struct {
	Errno int         `json:"errno"`
	Msg   string      `json:"msg"`
	Data  interface{} `json:"data"`
}

//任务执行结果
type JobExecuteResult struct {
	ExecuteInfo *JobExecuteInfo //执行状态
	Output      []byte          //脚本输出
	Err         error           //执行错误
	StartTime   time.Time       //启动时间
	EndTime     time.Time       //结束时间
}

type JobEvent struct {
	EventTyoe int // 一种save 一种delete
	Job       *Job
}

func BuildResponse(errno int, msg string, data interface{}) (resp []byte, err error) {
	//1.定义一个Response对象
	var (
		response Response
	)
	response.Errno = errno
	response.Data = data
	response.Msg = msg
	//2、序列化Json
	resp, err = json.Marshal(response)

	return
}

//反序列化Job
func UnpackJob(value []byte) (ret *Job, err error) {
	var (
		job *Job
	)

	job = &Job{}
	if err = json.Unmarshal(value, job); err != nil {
		return
	}
	ret = job
	return
}

//提取任务名
func ExtractJobName(jobKey string) string {
	return strings.TrimPrefix(jobKey, JOB_SAVE_DIR)
}

//提取Killer任务名
func ExtractKillerName(jobKey string) string {
	return strings.TrimPrefix(jobKey, JOB_KILL_DIR)
}

//任务变化事件2中 1）更新任务 2）删除任务
func BuildJobEvent(eventType int, job *Job) (jobEvent *JobEvent) {

	return &JobEvent{
		EventTyoe: eventType,
		Job:       job,
	}
}
func BuildJobSchedulePlan(job *Job) (jobSchedulePlan *JobSchedulePlan, err error) {
	var (
		expr *cronexpr.Expression
	)
	//解析cron的表达式
	if expr, err = cronexpr.Parse(job.CronExpr); err != nil {
		return
	}

	//生成任务计划对象
	jobSchedulePlan = &JobSchedulePlan{
		Job:      job,
		Expr:     expr,
		NextTime: expr.Next(time.Now()),
	}
	return
}

//构造执行状态信息
func BuildJobExecuteInfo(jobSchedulePlan *JobSchedulePlan) (jobExecuteInfo *JobExecuteInfo) {
	jobExecuteInfo = &JobExecuteInfo{
		Job:      jobSchedulePlan.Job,
		PlanTime: jobSchedulePlan.NextTime, //计划调度时间
		RealTime: time.Now(),               //真是调度时间
	}
	jobExecuteInfo.CancelCtx, jobExecuteInfo.CancelFunc = context.WithCancel(context.TODO())
	return

}
