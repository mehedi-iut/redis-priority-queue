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
	quitChan    chan bool // New: channel to receive stop signal
}

func NewWorker(id string, redisClient *redis.Client) *Worker {
	return &Worker{
		ID:          id,
		RedisClient: redisClient,
		quitChan:    make(chan bool), // Create the channel
	}
}

func (w *Worker) Start() {
	fmt.Println("Worker started")
}

func (w *Worker) Stop() {
	w.quitChan <- true // Send "quit" signal
}

func (w *Worker) Run() {
	w.Start()
	w.Process()
	w.Stop()
}

func (w *Worker) Process() {
	fmt.Println("Worker", w.ID, "started processing......")

	for {
		// Check if we should quit
		select {
		case <-w.quitChan: // If someone sent "quit" signal
			fmt.Println("Worker", w.ID, "received quit signal")
			return
		default:
			// no quit signal, continue processing
		}
		j, err := job.Dequeue(w.RedisClient)
		if err != nil {
			fmt.Println("Error dequeue:", err)
			time.Sleep(1 * time.Second) // Wait before try again
			continue
		}

		// Process the job
		time.Sleep(10 * time.Second)
		fmt.Println("Worker", w.ID, "processed: ", j)
	}
}
