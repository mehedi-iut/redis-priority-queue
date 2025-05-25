package job

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
)

func Dequeue(c *redis.Client) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), redisTimeOut)
	defer cancel()

	var result Message
	msgString, err := c.ZPopMin(ctx, "info", 1).Result()

	if err != nil {
		return "", err
	}

	if len(msgString) == 0 {
		return "", fmt.Errorf("No message in queue")
	}

	for _, item := range msgString {
		err = json.Unmarshal([]byte(item.Member.(string)), &result)
		if err != nil {
			return "", err
		}
	}

	return fmt.Sprintf("Info: %#v", result), nil
}
