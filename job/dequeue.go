package job

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
)

func Dequeue(c *redis.Client) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), redisTimeOut)
	defer cancel()
	msgString, err := c.LPop(ctx, "info").Result()

	if errors.Is(err, redis.Nil) {
		return "", fmt.Errorf("No message in queue")
	} else if err != nil {
		return "", err
	} else {
		return msgString, nil
	}
	//var msgStruct Message
	//err = json.Unmarshal([]byte(msgString), &msgStruct)
	//if err != nil {
	//	return "", err
	//}

}
