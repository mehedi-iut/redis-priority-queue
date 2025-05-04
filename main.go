package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"math/rand"
	"strings"
	"time"
)

type Job struct {
	ID         string    `json:"id"`
	Payload    string    `json:"payload"`
	Type       string    `json:"type"`
	Priority   int       `json:"priority"`
	Status     string    `json:"status"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
	RetryCount int       `json:"retry_count"`
	MaxRetries int       `json:"max_retries"`
	Result     string    `json:"result"`
	WorkerID   string    `json:"worker_id"`
}

// Job status constants
const (
	jobQueue         = "jobQueue"
	redisTimeOut     = 5 * time.Second
	StatusPending    = "pending"
	StatusProcessing = "processing"
	StatusCompleted  = "completed"
	StatusFailed     = "failed"
)

// Priority levels with clearer values
const (
	PriorityLowest  = 10
	PriorityLow     = 20
	PriorityNormal  = 50
	PriorityHigh    = 80
	PriorityHighest = 100
)

// Queue names - will be useful later for different features
const (
	jobProcessing    = "jobProcessing"
	jobCompleted     = "jobCompleted"
	jobFailed        = "jobFailed"
	jobPriorityQueue = "jobPriorityQueue"
)

// NewJob creates a new job with default values
func NewJob(jobType, payload string, priority int) *Job {
	now := time.Now()
	return &Job{
		ID:         generateID(),
		Type:       jobType,
		Payload:    payload,
		Priority:   priority,
		Status:     StatusPending,
		CreatedAt:  now,
		UpdatedAt:  now,
		RetryCount: 0,
		MaxRetries: 3,
		Result:     "",
		WorkerID:   "",
	}
}

// Helper function to generate unique IDs
func generateID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func main() {
	// Seed random number generator
	rand.Seed(time.Now().UnixNano())
	conn := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), redisTimeOut)
	defer cancel()

	if err := conn.Ping(ctx).Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to redis server")

	// Create and enqueue test jobs with different priorities
	jobTypes := []string{"email", "processing", "notification", "backup", "report"}
	priorities := []int{PriorityLowest, PriorityLow, PriorityNormal, PriorityHigh, PriorityHighest}

	for i := 1; i <= 10; i++ {
		jobType := jobTypes[i%len(jobTypes)]
		priority := priorities[i%len(priorities)]

		payload := fmt.Sprintf("Sample payload for job %d", i)

		job := NewJob(jobType, payload, priority)
		if err := EnqueueJob(conn, job); err != nil {
			log.Printf("Failed to enqueue job %d: %v", i, err)
		}
	}

	// Print queue stats before processing
	stats, err := GetQueueStats(conn)
	if err != nil {
		log.Printf("Failed to get queue stats: %v", err)
	} else {
		fmt.Println("Queue statistics before processing:")
		for category, count := range stats {
			fmt.Printf("  %s: %d\n", category, count)
		}
	}

	// Start a worker
	workerID := fmt.Sprintf("worker-%d", time.Now().UnixNano())
	WorkerLoop(conn, workerID)
}

func EnqueueJob(client *redis.Client, job *Job) error {
	ctx, cancel := context.WithTimeout(context.Background(), redisTimeOut)
	defer cancel()

	// Set initial status if not already set
	if job.Status == "" {
		job.Status = StatusPending
	}

	// Update timestamps
	now := time.Now()
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	job.UpdatedAt = now

	// serialize job to JSON
	jobJSON, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	// Store job in Redis priority queue (sorted set)
	// Higher priority = higher score = processed first
	// For same priority, we use creation time as tiebreaker
	// Calculate score: priority * 1000000 + (max_timestamp - created_at_unix)
	// This ensures higher priority jobs come first, and for same priority, older jobs come first

	maxTimestamp := float64(100000000000) // Some future timestamp
	createdAtUnix := float64(job.CreatedAt.Unix())
	score := float64(job.Priority)*1000000 + (maxTimestamp - createdAtUnix)

	err = client.ZAdd(ctx, jobPriorityQueue, redis.Z{
		Score:  score,
		Member: jobJSON,
	}).Err()

	if err != nil {
		return fmt.Errorf("failed to add job to queue: %w", err)
	}

	// store job metadata in a redis hash for easy access
	jobKey := fmt.Sprintf("job:%s", job.ID)
	_, err = client.HSet(ctx, jobKey,
		"id", job.ID,
		"type", job.Type,
		"status", job.Status,
		"priority", job.Priority,
		"created_at", job.CreatedAt.Format(time.RFC3339),
		"updated_at", job.UpdatedAt.Format(time.RFC3339),
	).Result()

	if err != nil {
		return fmt.Errorf("failed to store job metadata: %w", err)
	}

	fmt.Printf("Enqueued job: %s (Type: %s, Priority: %d)\n", job.ID, job.Type, job.Priority)
	return nil
}

func DequeueJob(client *redis.Client) (*Job, error) {
	ctx, cancel := context.WithTimeout(context.Background(), redisTimeOut)
	defer cancel()

	// Get highest priority job (highest score) from sorted set
	// ZPOPMAX gets the member with the highest score
	result, err := client.ZPopMax(ctx, jobPriorityQueue).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to dequeue job from queue: %w", err)
	}

	if len(result) == 0 {
		return nil, errors.New("failed to dequeue job from queue")
	}

	jobJSON := result[0].Member.(string)

	// Deserialize job
	var job Job
	err = json.Unmarshal([]byte(jobJSON), &job)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	// update job status
	job.Status = StatusProcessing
	job.UpdatedAt = time.Now()

	// Update job metadata in Redis
	jobKey := fmt.Sprintf("job:%s", job.ID)
	_, err = client.HSet(ctx, jobKey,
		"status", job.Status,
		"updated_at", job.UpdatedAt.Format(time.RFC3339),
	).Result()

	if err != nil {
		return nil, fmt.Errorf("failed to store job metadata: %w", err)
	}

	fmt.Printf("Dequeued job: %s (Type: %s, Priority: %d)\n", job.ID, job.Type, job.Priority)
	//processJob(job)
	return &job, nil
}

func processJob(job Job) {
	fmt.Printf("Processing job: %s (Type: %s)\n", job.ID, job.Type)
	// Actual job processing would happen here
	// ...

	// For now, just simulate processing
	fmt.Printf("Job %s processed successfully\n", job.ID)
}

func GetQueueStats(client *redis.Client) (map[string]int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), redisTimeOut)
	defer cancel()

	// Get all jobs in the priority queue
	jobs, err := client.ZRange(ctx, jobPriorityQueue, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch queue stats: %w", err)
	}

	stats := map[string]int{
		"total":  len(jobs),
		"low":    0,
		"high":   0,
		"normal": 0,
	}

	// count jobs by priority
	for _, jobJSON := range jobs {
		var job Job
		if err = json.Unmarshal([]byte(jobJSON), &job); err != nil {
			continue // skip jobs that can't be unmarshal
		}

		switch {
		case job.Priority <= PriorityLow:
			stats["low"]++
		case job.Priority <= PriorityNormal:
			stats["normal"]++
		default:
			stats["high"]++
		}
	}

	return stats, nil
}

func WorkerLoop(client *redis.Client, workerID string) {
	fmt.Printf("Starting worker loop for worker: %s\n", workerID)
	for {
		// Get a job from queue
		job, err := DequeueJob(client)
		if err != nil {
			if strings.Contains(err.Error(), "job queue is empty") {
				// Queue is empty, wait before trying again
				fmt.Println("Job queue is empty...")
				time.Sleep(5 * time.Second)
				continue
			}

			// Handle other errors
			log.Printf("Failed to dequeue job from queue: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}

		// Set worker ID on the job
		job.WorkerID = workerID

		// Process the job - processing time could depend on priority
		fmt.Printf("Worker %s processing job %s (Priority: %d)\n", workerID, job.ID, job.Priority)

		// Simulate different processing times based on priority
		// Higher priority jobs get more resources ( less waiting )
		var processingTime time.Duration
		switch {
		case job.Priority >= PriorityHigh:
			processingTime = 1 * time.Second
		case job.Priority >= PriorityNormal:
			processingTime = 2 * time.Second
		default:
			processingTime = 3 * time.Second
		}

		success := processJobWithTimeout(job, processingTime)

		// Update job status based on result
		updateJobStatus(client, job, success)
	}
}

// Process a job with a timeout based on priority
func processJobWithTimeout(job *Job, timeout time.Duration) bool {
	fmt.Printf("Processing job: %s (Type: %s, Priority: %d)\n",
		job.ID, job.Type, job.Priority)

	// Simulate job processing with timeout
	time.Sleep(timeout)

	// For demonstration, we'll succeed 80% of the time
	success := rand.Float32() < 0.8

	if success {
		fmt.Printf("Job %s processed successfully\n", job.ID)
		job.Result = fmt.Sprintf("Processed successfully in %v", timeout)
	} else {
		fmt.Printf("Job %s processing failed\n", job.ID)
		job.Result = "Processing failed"
	}

	return success
}

func updateJobStatus(client *redis.Client, job *Job, success bool) {
	ctx, cancel := context.WithTimeout(context.Background(), redisTimeOut)
	defer cancel()

	// Update job status
	if success {
		job.Status = StatusCompleted
	} else {
		if job.RetryCount < job.MaxRetries {
			job.RetryCount++
			job.Status = StatusPending

			// For retries, slightly increase priority to prevent starvation
			// But cap it at PriorityHighest
			if job.Priority < PriorityHighest {
				job.Priority += 5
			}

			// Re-enqueue for retry
			err := EnqueueJob(client, job)
			if err != nil {
				return
			}
			fmt.Printf("Job %s scheduled for retry (%d/%d) with priority %d\n",
				job.ID, job.RetryCount, job.MaxRetries, job.Priority)
			return
		} else {
			job.Status = StatusFailed
			// Move to dead letter queue here
			jobJSON, _ := json.Marshal(job)
			client.LPush(ctx, jobFailed, jobJSON)
			fmt.Printf("Job %s moved to failed queue after %d retries\n",
				job.ID, job.RetryCount)
		}
	}

	job.UpdatedAt = time.Now()

	// Update job metadata in Redis
	jobKey := fmt.Sprintf("job:%s", job.ID)
	_, err := client.HSet(ctx, jobKey,
		"status", job.Status,
		"updated_at", job.UpdatedAt.Format(time.RFC3339),
		"result", job.Result,
		"retry_count", job.RetryCount,
		"priority", job.Priority,
	).Result()

	if err != nil {
		log.Printf("Failed to update job status: %v", err)
	}
}
