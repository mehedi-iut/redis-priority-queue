package job

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

type Message struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
	Age  string `json:"age"`
}

const redisTimeOut = 5 * time.Second

func Enqueue(c *redis.Client, msg Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), redisTimeOut)
	defer cancel()
	val, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	//err = c.Set(ctx, "info", val, 0).Err()
	err = c.RPush(ctx, "info", val).Err()
	if err != nil {
		return err
	}

	fmt.Println("Message Enqueued")
	return nil
}
