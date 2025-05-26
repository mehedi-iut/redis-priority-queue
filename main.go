package main

import (
	"fmt"
	"github.com/redis/go-redis/v9"
	"redis-priority-queue/job"
	"redis-priority-queue/worker"
	"time"
)

func main() {
	rdb := redis.NewClient(
		&redis.Options{
			Addr:     "localhost:6379",
			DB:       0,
			Password: "",
		})

	// Create a list of job.Message objects
	messages := []job.Message{
		{
			Id:       1,
			Name:     "Ibrahim",
			Age:      "25",
			Priority: 1,
		},
		{
			Id:       2,
			Name:     "Omar",
			Age:      "28",
			Priority: 2,
		},
		{
			Id:       3,
			Name:     "Mehedi",
			Age:      "30",
			Priority: 10,
		},
	}

	// Enqueue all messages
	fmt.Println("Enqueueing messages...")
	for _, msg := range messages {
		err := job.Enqueue(rdb, msg)
		if err != nil {
			panic(err)
		}
	}

	// create pool with 3 workers
	wp := worker.NewWorkerPool(3, rdb)
	wp.Start()
	time.Sleep(100 * time.Second)
	wp.Stop()
}
