package worker

import (
	"bb_crontab/common"
	"context"
	"time"

	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"

	"go.mongodb.org/mongo-driver/mongo"
)

//MongoDB存储日志

type LogSink struct {
	client         *mongo.Client
	logColection   *mongo.Collection
	logChan        chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

var (
	//单例
	G_logSink *LogSink
)

func (logSink *LogSink) saveLogs(batch *common.LogBatch) {
	logSink.logColection.InsertMany(context.TODO(), batch.Logs)
}

//日志存储协程
func (logSink *LogSink) writeLoop() {
	var (
		log          *common.JobLog
		logBatch     *common.LogBatch //当前批次
		commitTimer  *time.Timer
		timeoutBatch *common.LogBatch
	)
	for {
		select {
		case log = <-logSink.logChan:
			if logBatch == nil {
				logBatch = &common.LogBatch{}
				//让这个批次超时自动提交（给1秒的时间）
				commitTimer = time.AfterFunc(time.Duration(G_config.JobLogCommitTimeout)*time.Millisecond,
					func(logBatch *common.LogBatch) func() {
						return func() {
							logSink.autoCommitChan <- logBatch
						}
					}(logBatch),
				)
			}
			//把新日志追加到批次中
			logBatch.Logs = append(logBatch.Logs, log)
			if len(logBatch.Logs) >= G_config.JobLogBatchSize {
				//发送日志
				logSink.saveLogs(logBatch)
				//清空logBatch
				logBatch = nil
				//取消定时器
				commitTimer.Stop()
			}
		case timeoutBatch = <-logSink.autoCommitChan: //过期的批次
			if timeoutBatch != logBatch {
				continue //如果当前批次不等于已提交批次，跳过当前批次
			}
			//把批次写入MongoDB
			logSink.saveLogs(timeoutBatch)
			//清空logBatch
			logBatch = nil

		}
	}
}

func InitLogSink() (err error) {
	var (
		client *mongo.Client
	)
	want, err := readpref.New(readpref.SecondaryMode) //表示只使用辅助节点
	if err != nil {
	}
	wc := writeconcern.New(writeconcern.WMajority())
	readconcern.Majority()
	opt := options.Client().ApplyURI(G_config.MongodbUri)
	opt.SetLocalThreshold(3 * time.Second)                                                  //只使用与mongo操作耗时小于3秒的
	opt.SetMaxConnIdleTime(5 * time.Second)                                                 //指定连接可以保持空闲的最大毫秒数
	opt.SetConnectTimeout(time.Duration(G_config.MongodbConnectTimeout) * time.Millisecond) //Timeout
	opt.SetMaxPoolSize(200)                                                                 //使用最大的连接数
	opt.SetReadPreference(want)                                                             //表示只使用辅助节点
	opt.SetReadConcern(readconcern.Majority())                                              //指定查询应返回实例的最新数据确认为，已写入副本集中的大多数成员
	opt.SetWriteConcern(wc)                                                                 //请求确认写操作传播到大多数mongod实例
	if client, err = mongo.Connect(context.TODO(), opt); err != nil {
		return
	}
	//选择DB和collection
	G_logSink = &LogSink{
		client:         client,
		logColection:   client.Database("cron").Collection("log"),
		logChan:        make(chan *common.JobLog),
		autoCommitChan: make(chan *common.LogBatch),
	}
	//启动一个MongoDB处理协程
	go G_logSink.writeLoop()
	return
}

//发送日志
func (logSink *LogSink) Append(jobLog *common.JobLog) {
	select {
	case logSink.logChan <- jobLog:
	default:
		//队列满了就丢弃
	}

}
