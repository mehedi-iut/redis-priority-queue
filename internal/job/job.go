package job

import (
	"redis-priority-queue/pkg/utils"
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

// NewJob creates a new job with default values
func NewJob(jobType, payload string, priority int) *Job {
	now := time.Now()
	return &Job{
		ID:         utils.GenerateID(),
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
