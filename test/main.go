package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cwww3/delayqueue"
	"github.com/redis/go-redis/v9"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const parallelCount = 1

var count atomic.Int64
var consumerCount atomic.Int64

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // 没有密码，默认值
		DB:       0,  // 默认DB 0
	})

	do := func(ctx context.Context, value string) error {
		data := StringToBytes(value)
		var j map[string]interface{}
		err := json.Unmarshal(data, &j)
		if err != nil {
			return err
		}
		consumerCount.Add(1)
		fmt.Println("消费时间", time.Now().Format(time.TimeOnly), "期望时间", j["expectTime"].(string))
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dqList := make([]*delayqueue.DelayQueue, 0, parallelCount)
	for i := 0; i < parallelCount; i++ {
		dq, err := delayqueue.InitDelayQueue(context.Background(), rdb, do)
		if err != nil {
			panic(err)
		}
		dq.Start(ctx)
		if err != nil {
			panic(err)
		}
		dqList = append(dqList, dq)
	}

	wg := sync.WaitGroup{}
	// 测试put对migration的影响 延迟会有多大

	// 持续put
	ctx, cancel = context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	now := time.Now()

	for i := 0; i < parallelCount; i++ {
		wg.Add(1)
		go func(dq *delayqueue.DelayQueue) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
				default:
					t := now.Add(time.Minute * time.Duration(rand.Intn(3)))
					id := count.Add(1)
					err := dq.Put(context.Background(), delayqueue.Job{
						Score:  float64(t.Unix()),
						Member: []byte(fmt.Sprintf(`{"id":"%v","expectTime":"%v"}`, id, t.Format(time.TimeOnly))),
					})
					if err != nil {
						fmt.Println("put failed", err)
					}
					time.Sleep(time.Millisecond * 200)
				}
			}
		}(dqList[i])
	}

	wg.Wait()

	fmt.Println("发送完毕")
	<-time.After(time.Minute * 5)
	fmt.Println("produce", count.Load(), "consumer", consumerCount.Load())
}

// BytesToString converts byte slice to string.
func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// StringToBytes converts string to byte slice.
func StringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}
