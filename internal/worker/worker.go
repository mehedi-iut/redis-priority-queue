package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"math/rand"
	"redis-priority-queue/internal/job"
	"redis-priority-queue/internal/queue"
	"strings"
	"time"
)

type Worker struct {
	id     string
	queue  *queue.Queue
	client *redis.Client
}

func NewWorker(client *redis.Client) *Worker {
	return &Worker{
		id:     fmt.Sprintf("worker-%d", time.Now().UnixNano()),
		queue:  queue.NewQueue(client),
		client: client,
	}
}

func (w *Worker) Start() {
	fmt.Printf("Starting worker %s\n", w.id)
	for {
		j, err := w.queue.DequeueJob()
		if err != nil {
			if strings.Contains(err.Error(), "failed to dequeue job from queue") {
				fmt.Println("No jobs to process")
				time.Sleep(1 * time.Second)
				continue
			}
			log.Printf("Failed to dequeue job: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}
		j.WorkerID = w.id
		success := w.processJob(j)
		w.updateJobStatus(j, success)
	}
}

func (w *Worker) processJob(j *job.Job) bool {
	var processingTime time.Duration
	switch {
	case j.Priority >= job.PriorityHigh:
		processingTime = 1 * time.Second
	case j.Priority >= job.PriorityNormal:
		processingTime = 2 * time.Second
	default:
		processingTime = 3 * time.Second
	}

	fmt.Printf("Processing job: %s (Type: %s, Priority: %d)\n",
		j.ID, j.Type, j.Priority)

	// Simulate job processing with timeout
	time.Sleep(processingTime)

	// For demonstration, we'll succeed 80% of the time
	success := rand.Float32() < 0.8

	if success {
		fmt.Printf("Job %s processed successfully\n", j.ID)
		j.Result = fmt.Sprintf("Processed successfully in %v", processingTime)
	} else {
		fmt.Printf("Job %s processing failed\n", j.ID)
		j.Result = "Processing failed"
	}

	return success
}

func (w *Worker) updateJobStatus(j *job.Job, success bool) {
	ctx, cancel := context.WithTimeout(context.Background(), job.RedisTimeout)
	defer cancel()

	if success {
		j.Status = job.StatusCompleted
	} else {
		if j.RetryCount < j.MaxRetries {
			j.RetryCount++
			j.Status = job.StatusPending

			// For retries, slightly increase priority to prevent starvation
			// But cap it at PriorityHighest
			if j.Priority < job.PriorityHighest {
				j.Priority += 5
			}

			// Re-enqueue for retry
			if err := w.queue.EnqueueJob(j); err != nil {
				log.Printf("Failed to re-enqueue job %s: %v", j.ID, err)
				return
			}
			fmt.Printf("Job %s scheduled for retry (%d/%d) with priority %d\n",
				j.ID, j.RetryCount, j.MaxRetries, j.Priority)
			return
		} else {
			j.Status = job.StatusFailed
			// Move to dead letter queue
			jobJSON, _ := json.Marshal(j)
			w.client.LPush(ctx, job.JobFailed, jobJSON)
			fmt.Printf("Job %s moved to failed queue after %d retries\n",
				j.ID, j.RetryCount)
		}
	}

	j.UpdatedAt = time.Now()

	// Update job metadata in Redis
	jobKey := fmt.Sprintf("job:%s", j.ID)
	_, err := w.client.HSet(ctx, jobKey,
		"status", j.Status,
		"updated_at", j.UpdatedAt.Format(time.RFC3339),
		"result", j.Result,
		"retry_count", j.RetryCount,
		"priority", j.Priority,
	).Result()

	if err != nil {
		log.Printf("Failed to update job status: %v", err)
	}
}
