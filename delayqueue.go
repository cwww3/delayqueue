package delayqueue

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"runtime"
	"strconv"
	"time"
)

var (
	ErrNilClient = errors.New("nil client")
	ErrQueueType = errors.New("queue type invalid")
)

const (
	None   = "none"
	Zset   = "zset"
	List   = "list"
	String = "string"
)

const (
	migrationCount = 50
	taskCap        = 100
	piord          = 200 //ms
)

type Run func(ctx context.Context, value string) error

func (r Run) Do(ctx context.Context, logger *zap.SugaredLogger, value string) {
	defer func() {
		if err := recover(); err != nil {
			logger.Errorf("executor panic,err=%v", err)
		}
	}()
	err := r(ctx, value)
	if err != nil {
		logger.Errorf("executor failed,err=%v", err)
	}
}

type DelayQueue struct {
	zsetName       string // zset
	listName       string // list
	workerCount    int    // execute task count
	client         *redis.Client
	executor       Run
	blockTime      time.Duration // blpop timeout
	taskCh         chan string   // ch for task
	migrationCount int64         // migration form zset to list count
	logger         *zap.SugaredLogger

	add        chan int64 // notify TODO 没有启用
	stringName string     // string pub/sub TODO 没有启用
}

type Option func(queue *DelayQueue)

func WithBlockTime(duration time.Duration) Option {
	return func(queue *DelayQueue) {
		queue.blockTime = duration
	}
}

func WithWorkerCount(count int) Option {
	return func(queue *DelayQueue) {
		queue.workerCount = count
	}
}

func WithMigrationCount(count int64) Option {
	return func(queue *DelayQueue) {
		queue.migrationCount = count
	}
}

func WithLogger(logger *zap.SugaredLogger) Option {
	return func(queue *DelayQueue) {
		queue.logger = logger
	}
}

type Job struct {
	Score  float64
	Member interface{}
}

func InitDelayQueue(ctx context.Context, client *redis.Client, do Run, opts ...Option) (*DelayQueue, error) {
	if client == nil {
		return nil, ErrNilClient
	}

	dq := &DelayQueue{
		zsetName:       "zset:delayqueue",
		listName:       "list:delayqueue",
		workerCount:    runtime.GOMAXPROCS(0),
		client:         client,
		executor:       do,
		blockTime:      time.Minute,
		taskCh:         make(chan string, taskCap),
		migrationCount: migrationCount,
		logger:         zap.NewExample().Sugar(),

		//stringName:  "string:delayqueue",
		//add: make(chan int64, 1000),
	}
	for i := 0; i < len(opts); i++ {
		opts[i](dq)
	}

	dq.logger.Debugf("use zset %v", dq.zsetName)
	dq.logger.Debugf("use list %v", dq.listName)
	dq.logger.Debugf("worker count %v", dq.workerCount)

	err := dq.checkQueue(ctx)
	if err != nil {
		return nil, err
	}

	return dq, nil
}
func (dq *DelayQueue) Start(ctx context.Context) {
	// block get from list
	go func() {
		for {
			select {
			case <-ctx.Done():
				close(dq.taskCh) // taskCh在这里关闭 因为default会向taskCh发送确保在关闭之后不会发送 且能通知到workers
				return
			default:
				values, err := dq.client.BLPop(ctx, dq.blockTime, dq.listName).Result()
				if errors.Is(err, redis.Nil) {
					continue
				}

				if err != nil {
					dq.logger.Errorf("get from queue failed, err=%v\n", err)
					continue
				}

				if len(values) != 2 {
					dq.logger.Errorf("get value invalid data=%v", values)
					continue
				}

				dq.taskCh <- values[1]
			}
		}
	}()

	// move zset to list
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Millisecond * 200):
				// 为了支持多实例部署 先简单的通过轮询的方式实现
				for {
					// watch + pipeline => LUA脚本实现
					err := dq.client.Watch(ctx, func(tx *redis.Tx) error {
						now := time.Now().Unix()
						ctx := context.Background()
						items, err := tx.ZRevRangeByScore(ctx, dq.zsetName, &redis.ZRangeBy{
							Min: "0", Max: strconv.FormatInt(now, 10), Offset: 0, Count: dq.migrationCount,
						}).Result()
						if err != nil {
							return err
						}

						if len(items) == 0 {
							return redis.Nil
						}

						dq.logger.Debugf("get from zset count %v", len(items))

						tasks := toInterfaceSlice(items)

						_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
							pipe.ZRem(ctx, dq.zsetName, tasks...)
							pipe.LPush(ctx, dq.listName, tasks...)
							return nil
						})

						return err
					}, dq.zsetName)
					if err == redis.Nil {
						break
					}

					if err != nil {
						dq.logger.Errorf("watch to remove failed,err=%v", err)
						break
					}
				}
			}
		}
	}()

	// execute
	for i := 0; i < dq.workerCount; i++ {
		go func() {
			for task := range dq.taskCh {
				dq.executor.Do(context.Background(), dq.logger, task)
			}
		}()
	}
}
func (dq *DelayQueue) Put(ctx context.Context, job Job) error {
	err := dq.client.ZAdd(ctx, dq.zsetName, redis.Z{Score: job.Score, Member: job.Member}).Err()
	if err != nil {
		return err
	}
	//go func() {
	//	// 加了这一步就做不到分布式了
	//	dq.add <- int64(job.Score)
	//}()
	return nil
}
func (dq *DelayQueue) checkQueue(ctx context.Context) error {
	// https://www.runoob.com/redis/keys-type.html
	zsetType, err := dq.client.Type(ctx, dq.zsetName).Result()
	if err != nil {
		return err
	}

	switch zsetType {
	case None, Zset:
		err = nil
	default:
		err = ErrQueueType
	}
	if err != nil {
		return err
	}

	listType, err := dq.client.Type(ctx, dq.listName).Result()
	if err != nil {
		return err
	}

	switch listType {
	case None, List:
		err = nil
	default:
		err = ErrQueueType
	}
	if err != nil {
		return err
	}

	//stringType, err := dq.client.Type(ctx, dq.stringName).Result()
	//if err != nil {
	//	return err
	//}
	//
	//switch stringType {
	//case None, String:
	//	err = nil
	//default:
	//	err = ErrQueueType
	//}
	if err != nil {
		return err
	}

	return nil
}

func (dq *DelayQueue) waitNextZRange() error {
	// 获取下一次获取时间
	// TODO test err when null
	zs, err := dq.client.ZRangeWithScores(context.Background(), dq.zsetName, 0, 0).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	err = nil

	// 默认等一个小时
	now := time.Now()
	endTime := now.Add(time.Hour)
	if len(zs) > 0 {
		t := time.Unix(int64(zs[0].Score), 0)
		// 如果早于一个小时则更新
		if t.Before(endTime) {
			endTime = t
		}
	}
	waitTime := endTime.Sub(now)
	timer := time.NewTimer(waitTime)

Loop:
	for {
		select {
		case <-timer.C:
			break Loop
		case unix := <-dq.add:
			// 如果新加入的早于现在等待的则更新
			if unix < endTime.Unix() {
				timer.Reset(time.Unix(unix, 0).Sub(now))
			}
		}
	}
	timer.Stop()

	return nil
}

func toInterfaceSlice(args []string) []interface{} {
	results := make([]interface{}, 0, len(args))
	for i := 0; i < len(args); i++ {
		results = append(results, args[i])
	}
	return results
}
