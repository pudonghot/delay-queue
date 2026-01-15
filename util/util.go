package util

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	tolerance = time.Minute * 30
)

type msg struct {
	Time    int64  `json:"time"`
	Payload []byte `json:"payload"`
}

func Wrap(data []byte, time time.Time) ([]byte, error) {
	return json.Marshal(msg{Time: time.UnixNano(), Payload: data})
}

func Unwrap(data []byte) ([]byte, error) {
	msg := msg{}
	err := json.Unmarshal(data, &msg)
	if err != nil {
		return nil, err
	}

	t := time.Unix(0, msg.Time)
	if t.Add(tolerance).Before(time.Now()) {
		return nil, fmt.Errorf("outdated: %q", string(data))
	}

	return msg.Payload, nil
}
