package taskq

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/go-redis/redis_rate/v9"
)

type QueueOptions struct {
	// Queue name.
	Name string

	// Minimum number of goroutines processing messages.
	// Default is 1.
	MinNumWorker int32
	// Maximum number of goroutines processing messages.
	// Default is 32 * number of CPUs.
	MaxNumWorker int32
	// Global limit of concurrently running workers across all servers.
	// Overrides MaxNumWorker.
	WorkerLimit int32
	// Maximum number of goroutines fetching messages.
	// Default is 8 * number of CPUs.
	MaxNumFetcher int32

	// Number of messages reserved by a fetcher in the queue in one request.
	// Default is 10 messages.
	ReservationSize int
	// Time after which the reserved message is returned to the queue.
	// Default is 5 minutes.
	ReservationTimeout time.Duration
	// Time that a long polling receive call waits for a message to become
	// available before returning an empty response.
	// Default is 10 seconds.
	WaitTimeout time.Duration
	// Size of the buffer where reserved messages are stored.
	// Default is the same as ReservationSize.
	BufferSize int

	// Number of consecutive failures after which queue processing is paused.
	// Default is 100 failures.
	PauseErrorsThreshold int

	// Processing rate limit.
	RateLimit redis_rate.Limit

	// Optional rate limiter. The default is to use Redis.
	RateLimiter *redis_rate.Limiter

	// Redis client that is used for storing metadata.
	Redis Redis

	// Optional storage interface. The default is to use Redis.
	Storage Storage

	// Optional message handler. The default is the global Tasks registry.
	Handler Handler

	inited bool
}

func (opt *QueueOptions) Init() {
	if opt.inited {
		return
	}
	opt.inited = true

	if opt.Name == "" {
		panic("QueueOptions.Name is required")
	}

	if opt.WorkerLimit > 0 {
		opt.MinNumWorker = opt.WorkerLimit
		opt.MaxNumWorker = opt.WorkerLimit
	} else {
		if opt.MinNumWorker == 0 {
			opt.MinNumWorker = 1
		}
		if opt.MaxNumWorker == 0 {
			opt.MaxNumWorker = 32 * int32(runtime.NumCPU())
		}
	}
	if opt.MaxNumFetcher == 0 {
		opt.MaxNumFetcher = 8 * int32(runtime.NumCPU())
	}

	switch opt.PauseErrorsThreshold {
	case -1:
		opt.PauseErrorsThreshold = 0
	case 0:
		opt.PauseErrorsThreshold = 100
	}

	if opt.ReservationSize == 0 {
		opt.ReservationSize = 10
	}
	if opt.ReservationTimeout == 0 {
		opt.ReservationTimeout = 5 * time.Minute
	}
	if opt.BufferSize == 0 {
		opt.BufferSize = opt.ReservationSize
	}
	if opt.WaitTimeout == 0 {
		opt.WaitTimeout = 10 * time.Second
	}

	if opt.Storage == nil {
		if opt.Redis != nil {
			opt.Storage = newRedisStorage(opt.Redis)
		} else {
			opt.Storage = localStorage{}
		}
	}

	if !opt.RateLimit.IsZero() && opt.RateLimiter == nil && opt.Redis != nil {
		opt.RateLimiter = redis_rate.NewLimiter(opt.Redis)
	}

	if opt.Handler == nil {
		opt.Handler = &Tasks
	}
}

//------------------------------------------------------------------------------

type Queue interface {
	fmt.Stringer
	Name() string
	Options() *QueueOptions
	Consumer() *Consumer

	Len() (int, error)
	Add(msg *Message) error
	ReserveN(ctx context.Context, n int, waitTimeout time.Duration) ([]Message, error)
	Release(msg *Message) error
	Delete(msg *Message) error
	Purge() error
	Close() error
	CloseTimeout(timeout time.Duration) error
}
