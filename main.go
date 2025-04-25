package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"time"
)

type Job struct {
	ID      string `json:"id"`
	Payload string `json:"payload"`
}

const (
	jobQueue     = "jobQueue"
	redisTimeOut = 5 * time.Second
)

func main() {
	conn := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	job := &Job{
		ID:      "job4",
		Payload: "payload4",
	}

	err := EnqueueJob(conn, job)
	if err != nil {
		log.Fatal(err)
	}
	DequeueJob(conn)
}

func EnqueueJob(client *redis.Client, job *Job) error {
	ctx, cancel := context.WithTimeout(context.Background(), redisTimeOut)
	defer cancel()
	jobJSON, err := json.Marshal(job)
	if err != nil {
		return err
	}
	err = client.LPush(ctx, jobQueue, jobJSON).Err()
	if err != nil {
		return err
	}

	fmt.Printf("Enqueued job: %s\n", job.ID)
	return nil
}

func DequeueJob(client *redis.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), redisTimeOut)
	defer cancel()

	for {
		jobJson, err := client.RPop(ctx, jobQueue).Result()
		if err == redis.Nil {
			fmt.Println("Job queue is empty.")
			break
		} else if err != nil {
			log.Println(err)
		}

		var job Job
		err = json.Unmarshal([]byte(jobJson), &job)
		if err != nil {
			log.Println(err)
		}

		processJob(job)
	}
}

func processJob(job Job) {
	fmt.Printf("Processing job: %s\n", job.Payload)
}
