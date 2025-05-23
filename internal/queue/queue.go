package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"redis-priority-queue/internal/job"
	"time"
)

type Queue struct {
	client *redis.Client
}

func NewQueue(client *redis.Client) *Queue {
	return &Queue{
		client: client,
	}
}

func (q *Queue) EnqueueJob(j *job.Job) error {
	ctx, cancel := context.WithTimeout(context.Background(), job.RedisTimeout)
	defer cancel()

	// set initial status if not already set
	if j.Status == "" {
		j.Status = job.StatusPending
	}

	// Update timestamps
	now := time.Now()
	if j.CreatedAt.IsZero() {
		j.CreatedAt = now
	}

	j.UpdatedAt = now

	jobJSON, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	maxTimestamp := float64(100000000000)
	createdAtUnix := float64(j.CreatedAt.Unix())
	score := float64(j.Priority)*1000000 + (maxTimestamp - createdAtUnix)

	err = q.client.ZAdd(ctx, job.JobPriorityQueue, redis.Z{
		Score:  score,
		Member: jobJSON,
	}).Err()

	if err != nil {
		return fmt.Errorf("failed to add job to queue: %w", err)
	}

	return q.updateJobMetadata(j)
}

func (q *Queue) DequeueJob() (*job.Job, error) {
	ctx, cancel := context.WithTimeout(context.Background(), job.RedisTimeout)
	defer cancel()

	// Get highest priority job (highest score) from sorted set
	// ZPOPMAX gets the member with the highest score
	result, err := q.client.ZPopMax(ctx, job.JobPriorityQueue).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to dequeue job from queue: %w", err)
	}

	if len(result) == 0 {
		return nil, errors.New("failed to dequeue job from queue")
	}

	jobJSON := result[0].Member.(string)

	// Deserialize job
	var j job.Job
	err = json.Unmarshal([]byte(jobJSON), &j)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	// update job status
	j.Status = job.StatusProcessing
	j.UpdatedAt = time.Now()

	if err := q.updateJobMetadata(&j); err != nil {
		return nil, fmt.Errorf("failed to store job metadata: %w", err)
	}
	//processJob(job)
	return &j, nil
}

func (q *Queue) updateJobMetadata(j *job.Job) error {
	ctx, cancel := context.WithTimeout(context.Background(), job.RedisTimeout)
	defer cancel()

	jobKey := fmt.Sprintf("job:%s", j.ID)
	_, err := q.client.HSet(ctx, jobKey,
		"id", j.ID,
		"type", j.Type,
		"status", j.Status,
		"priority", j.Priority,
		"created_at", j.CreatedAt.Format(time.RFC3339),
		"updated_at", j.UpdatedAt.Format(time.RFC3339),
		"result", j.Result,
		"retry_count", j.RetryCount,
	).Result()

	if err != nil {
		return fmt.Errorf("failed to store job metadata: %w", err)
	}
	return nil
}

func (q *Queue) GetQueueStats() (map[string]int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), job.RedisTimeout)
	defer cancel()

	// Get all jobs in the priority queue
	jobs, err := q.client.ZRange(ctx, job.JobPriorityQueue, 0, -1).Result()
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
		var j job.Job
		if err = json.Unmarshal([]byte(jobJSON), &j); err != nil {
			continue // skip jobs that can't be unmarshal
		}

		switch {
		case j.Priority <= job.PriorityLow:
			stats["low"]++
		case j.Priority <= job.PriorityNormal:
			stats["normal"]++
		default:
			stats["high"]++
		}
	}

	return stats, nil
}
