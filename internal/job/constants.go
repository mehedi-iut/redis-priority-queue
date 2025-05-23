package job

import "time"

// Queue names
const (
	JobQueue         = "jobQueue"
	RedisTimeout     = 5 * time.Second
	JobProcessing    = "jobProcessing"
	JobCompleted     = "jobCompleted"
	JobFailed        = "jobFailed"
	JobPriorityQueue = "jobPriorityQueue"
)

// Status constants
const (
	StatusPending    = "pending"
	StatusProcessing = "processing"
	StatusCompleted  = "completed"
	StatusFailed     = "failed"
)

// Priority levels
const (
	PriorityLowest  = 10
	PriorityLow     = 20
	PriorityNormal  = 50
	PriorityHigh    = 80
	PriorityHighest = 100
)
