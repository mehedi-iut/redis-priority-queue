package main

import (
	"fmt"
	//"encoding/json"
	//"fmt"
	"github.com/redis/go-redis/v9"
	"redis-priority-queue/job"
	"time"
)

const redisTimeOut = 5 * time.Second

//type Message struct {
//	Id   int    `json:"id"`
//	Name string `json:"name"`
//	Age  string `json:"age"`
//}

func main() {
	//ctx, cancel := context.WithTimeout(context.Background(), redisTimeOut)
	//defer cancel()
	rdb := redis.NewClient(
		&redis.Options{
			Addr:     "localhost:6379",
			DB:       0,
			Password: "",
		})
	// create a variable with struct
	msg := job.Message{
		Id:   1,
		Name: "Mehedi",
		Age:  "30",
	}

	err := job.Enqueue(rdb, msg)
	//
	////p, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	//
	//fmt.Println("Saving record to redis.................")
	//err = rdb.Set(ctx, "info", p, 0).Err()
	//
	////err = rdb.Set(ctx, "name", "mehedi", 0).Err()
	//if err != nil {
	//	panic(err)
	//}

	//fmt.Println("Reading record from redis.................")
	//val, err := rdb.Get(ctx, "info").Result()
	//if err != nil {
	//	panic(err)
	//}
	//var valString job.Message
	//err = json.Unmarshal([]byte(val), &valString)
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println("Raw JSON:", val)
	//fmt.Printf("Unmarshalled data: %+v\n", valString)
	val, err := job.Dequeue(rdb)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(val)

}
