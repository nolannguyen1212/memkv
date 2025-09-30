package datastructure

import "time"

const (
	maxSampleSize   = 20
	maxSampleRounds = 3
)

type Item struct {
	Value     string
	Members   map[string]struct{}
	ExpiredAt time.Time
}
