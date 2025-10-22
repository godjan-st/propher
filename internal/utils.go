package internal

import "time"

func NowMS() int64 {
	return time.Now().UnixNano() / 1_000_000
}

func NowMicros() int64 {
	return time.Now().UnixNano() / 1_000
}
