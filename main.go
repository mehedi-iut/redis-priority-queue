package main

import (
	"fmt"
	"github.com/redis/go-redis/v9"
	"redis-priority-queue/job"
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
			Name:     "John",
			Age:      "25",
			Priority: 5,
		},
		{
			Id:       2,
			Name:     "Alice",
			Age:      "28",
			Priority: 3,
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

	// Dequeue all messages
	fmt.Println("\nDequeueing messages...")
	for i := 0; i < len(messages); i++ {
		val, err := job.Dequeue(rdb)
		if err != nil {
			fmt.Println("Error dequeueing:", err)
			break
		}
		fmt.Println(val)
	}
}
