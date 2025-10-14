package internal

import "time"

func NowMS() int64 {
	return time.Now().UnixNano() / 1_000_000
}
