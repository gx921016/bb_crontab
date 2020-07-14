package worker

import (
	"bb_crontab/common"
	"math/rand"
	"os/exec"
	"time"
)

type Executor struct {
}

var (
	G_executor *Executor
)

//执行一个任务
func (executor *Executor) ExecuteJob(info *common.JobExecuteInfo) {
	go func() {
		var (
			cmd     *exec.Cmd
			err     error
			output  []byte
			result  *common.JobExecuteResult
			jobLock *JobLock
		)
		//任务结果
		result = &common.JobExecuteResult{
			ExecuteInfo: info,
			Output:      make([]byte, 0),
		}
		//获取分布式锁
		jobLock = G_jobMgr.CreateJobLock(info.Job.Name)
		//记录任务开始
		result.StartTime = time.Now()
		//上锁
		//随机睡眠(0~1s)
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		err = jobLock.TryLock()
		//释放锁
		defer jobLock.Unlock()
		if err != nil {
			//上锁失败
			result.Err = err
			result.EndTime = time.Now()
		} else {
			//上锁成功后重置任务开始时间
			result.StartTime = time.Now()
			//执行shell命令
			cmd = exec.CommandContext(info.CancelCtx, "/bin/bash", "-c", info.Job.Command)
			//执行病捕获输出
			output, err = cmd.CombinedOutput()
			//记录任务结束
			result.EndTime = time.Now()
			result.Output = output
			result.Err = err
		}
		//把任务执行完成后，把执行的结果返回给Scheduler，Scheduler会从executingTable中删除执行记录
		G_scheduler.PushJobResult(result)
	}()
}

//初始化执行器
func InitExecutor() (err error) {
	G_executor = &Executor{}
	return
}
