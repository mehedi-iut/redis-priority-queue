package worker

import (
	"fmt"
	"github.com/redis/go-redis/v9"
	"redis-priority-queue/job"
	"time"
)

type WorkerPool struct {
	WorkerCount int
	RedisClient *redis.Client
	jobsChan    chan string
	quitChan    chan bool
}

func NewWorkerPool(workerCount int, redisClient *redis.Client) *WorkerPool {
	return &WorkerPool{
		WorkerCount: workerCount,
		RedisClient: redisClient,
		jobsChan:    make(chan string, 100),
		quitChan:    make(chan bool),
	}
}

type Worker struct {
	ID       string
	quitChan chan bool // New: channel to receive stop signal
	jobsChan chan string
}

func NewWorker(id string, jobChan chan string, quitChan chan bool) *Worker {
	return &Worker{
		ID:       id,
		quitChan: quitChan,
		jobsChan: jobChan,
	}
}

func (w *Worker) Process() {
	fmt.Println("Worker", w.ID, "started processing......")

	for {
		// Check if we should quit
		select {
		case <-w.quitChan: // If someone sent "quit" signal
			fmt.Println("Worker", w.ID, "received quit signal")
			return
		case j := <-w.jobsChan:
			// no quit signal, continue processing
			fmt.Println("Worker", w.ID, "got job", j)
			time.Sleep(10 * time.Second)
			fmt.Println("Worker", w.ID, "finished job", j)
		}

	}
}

func (wp *WorkerPool) dispatchJobs() {
	fmt.Println("Dispatching jobs...")
	for {
		select {
		case <-wp.quitChan:
			fmt.Println("Dispatching jobs stopped")
			close(wp.jobsChan)
			return

		default:
			j, err := job.Dequeue(wp.RedisClient)
			if err != nil {
				fmt.Println("Error dequeue:", err)
				time.Sleep(1 * time.Second) // Wait before try again
				continue
			}

			select {
			case wp.jobsChan <- j:
				fmt.Println("Job dispatched: ", j)
			default:
				fmt.Println("All workers busy, skipping job:", j)
			}
		}
	}
}

func (wp *WorkerPool) Start() {
	fmt.Println("Starting worker pool...")
	for i := 0; i < wp.WorkerCount; i++ {
		workerID := fmt.Sprintf("worker-%d", i+1)
		worker := NewWorker(workerID, wp.jobsChan, wp.quitChan)
		go worker.Process()
	}

	// start job dispatcher
	go wp.dispatchJobs()

	fmt.Println("Worker pool started")
}

func (wp *WorkerPool) Stop() {
	fmt.Println("Stopping worker pool...")
	close(wp.quitChan)
}
