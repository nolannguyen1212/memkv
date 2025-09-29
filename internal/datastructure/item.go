package datastructure

import "time"

type Item struct {
	Value     string
	ExpiredAt time.Time
}
