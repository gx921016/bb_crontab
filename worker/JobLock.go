package worker

import (
	"bb_crontab/common"
	"context"

	"go.etcd.io/etcd/clientv3"
)

//分布式锁
type JobLock struct {
	//etcd客户端
	kv         clientv3.KV
	lease      clientv3.Lease
	jobName    string             //任务名
	cancelFunc context.CancelFunc //用于终止自动续租
	leaseId    clientv3.LeaseID   //租约ID
	isLocked   bool               //是否上锁成功
}

//初始化一把锁
func InitJobLockl(kv clientv3.KV, lease clientv3.Lease, jobName string) *JobLock {
	return &JobLock{kv: kv, lease: lease, jobName: jobName}
}

//尝试上锁
func (jobLock *JobLock) TryLock() (err error) {
	var (
		leaseGrantRes *clientv3.LeaseGrantResponse
		cancelCtx     context.Context
		cancelFunc    context.CancelFunc
		leaseId       clientv3.LeaseID
		keepResChan   <-chan *clientv3.LeaseKeepAliveResponse
		txn           clientv3.Txn
		lockKey       string
		txnResp       *clientv3.TxnResponse
	)
	//创建租约（5秒）
	leaseGrantRes, err = jobLock.lease.Grant(context.TODO(), 5)
	if err != nil {
		return
	}
	//创建context用于取消自动续租
	cancelCtx, cancelFunc = context.WithCancel(context.TODO())
	//续租id
	leaseId = leaseGrantRes.ID
	//自动续租
	keepResChan, err = jobLock.lease.KeepAlive(cancelCtx, leaseId)
	if err != nil {
		goto FAIL
	}

	go func() {
		var (
			keepResp *clientv3.LeaseKeepAliveResponse
		)
		for {
			select {
			case keepResp = <-keepResChan: //自动续租应答
				if keepResp == nil {
					goto END
				}
			}
		}
	END:
	}()

	//创建事务txn
	txn = jobLock.kv.Txn(context.TODO())
	//锁路径
	lockKey = common.JOB_LOCK_DIR + jobLock.jobName

	txn.If(
		clientv3.Compare(
			clientv3.
				CreateRevision(lockKey), "=", 0)).
		Then(
			clientv3.
				OpPut(lockKey, "", clientv3.WithLease(leaseId))).
		Else(
			clientv3.OpGet(lockKey))
	//提交事务
	txnResp, err = txn.Commit()
	if err != nil {
		goto FAIL
	}
	//成功后返回
	if !txnResp.Succeeded { //锁被占用
		err = common.ERR_LOCK_ALREADY_REQUIRED
		goto FAIL
	}
	//抢锁成功
	jobLock.leaseId = leaseId
	jobLock.cancelFunc = cancelFunc
	jobLock.isLocked = true

	return
FAIL:
	cancelFunc()                                  //取消自动续租
	jobLock.lease.Revoke(context.TODO(), leaseId) //释放租约
	return
}

//释放锁
func (jobLock *JobLock) Unlock() {
	//如果上锁成功
	if jobLock.isLocked {
		jobLock.cancelFunc()                                  //取消程序自动续租的协程
		jobLock.lease.Revoke(context.TODO(), jobLock.leaseId) //释放租约
	}
}
