package worker

import (
	"fmt"
	"github.com/redis/go-redis/v9"
	"redis-priority-queue/job"
	"time"
)

type Worker struct {
	ID          string
	RedisClient *redis.Client
}

func NewWorker(id string, redisClient *redis.Client) *Worker {
	return &Worker{
		ID:          id,
		RedisClient: redisClient,
	}
}

func (w *Worker) Start() {
	fmt.Println("Worker started")
}

func (w *Worker) Stop() {
	fmt.Println("Worker stopped")
}

func (w *Worker) Run() {
	w.Start()
	w.Process()
	w.Stop()
}

func (w *Worker) Process() {
	fmt.Println("Worker", w.ID, "started processing......")
	//j, err := job.Dequeue(w.RedisClient)
	//if err != nil {
	//	fmt.Println("Error dequeueing:", err)
	//	return
	//}
	//time.Sleep(10 * time.Second)
	//fmt.Println(j)

	for {
		j, err := job.Dequeue(w.RedisClient)
		if err != nil {
			fmt.Println("Error dequeueing:", err)
			time.Sleep(1 * time.Second) // Wait before try again
			break
		}

		// Process the job
		time.Sleep(10 * time.Second)
		fmt.Println("Worker", w.ID, "processed: ", j)
	}
}
