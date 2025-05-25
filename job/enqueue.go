package job

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

type Message struct {
	Id       int    `json:"id"`
	Name     string `json:"name"`
	Age      string `json:"age"`
	Priority int    `json:"priority"`
}

const redisTimeOut = 5 * time.Second

func Enqueue(c *redis.Client, msg Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), redisTimeOut)
	defer cancel()
	val, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	//err = c.RPush(ctx, "info", val).Err()
	err = c.ZAdd(ctx, "info", redis.Z{
		Score:  float64(msg.Priority),
		Member: val,
	}).Err()
	if err != nil {
		return err
	}

	fmt.Println("Message Enqueued")
	return nil
}
