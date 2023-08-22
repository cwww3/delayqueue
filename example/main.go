package main

import (
	"context"
	"delayqueue"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
	"unsafe"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // 没有密码，默认值
		DB:       0,  // 默认DB 0
	})
	err := rdb.Ping(context.Background()).Err()
	if err != nil {
		panic(err)
	}

	do := func(ctx context.Context, value string) error {
		data := StringToBytes(value)
		var j map[string]interface{}
		err := json.Unmarshal(data, &j)
		if err != nil {
			return err
		}
		fmt.Println(time.Now().Format(time.TimeOnly), j["name"].(string))
		return nil
	}
	dq, err := delayqueue.InitDelayQueue(context.Background(), rdb, do)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dq.Start(ctx)
	if err != nil {
		panic(err)
	}

	now := time.Now()
	for i := 0; i < 10; i++ {
		t := now.Add(time.Duration(10) * time.Second)
		fmt.Println("add", t.Format(time.TimeOnly))
		err := dq.Put(context.Background(), delayqueue.Job{
			Score:  float64(t.Unix()),
			Member: []byte(fmt.Sprintf(`{"name":"%d-%s"}`, i, fmt.Sprintf("%v", t.Format(time.TimeOnly)))),
		})
		if err != nil {
			panic(err)
		}
	}

	select {}
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
