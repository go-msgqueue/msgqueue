package processor_test

import (
	"errors"
	"log"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"golang.org/x/time/rate"

	"github.com/go-msgqueue/msgqueue"
	"github.com/go-msgqueue/msgqueue/processor"
)

func queueName(s string) string {
	version := strings.Split(runtime.Version(), " ")[0]
	version = strings.Replace(version, ".", "", -1)
	return "test-" + s + "-" + version
}

func printStats(p *processor.Processor) {
	var old *msgqueue.Stats
	for _ = range time.Tick(3 * time.Second) {
		st := p.Stats()
		if st == nil {
			break
		}
		if old != nil && *st == *old {
			continue
		}
		old = st

		log.Printf(
			"%s: inFlight=%d deleting=%d processed=%d fails=%d retries=%d avg_dur=%s\n",
			p, st.InFlight, st.Deleting, st.Processed, st.Fails, st.Retries, st.AvgDuration,
		)
	}
}

func redisRing() *redis.Ring {
	ring := redis.NewRing(&redis.RingOptions{
		Addrs:    map[string]string{"0": ":6379"},
		PoolSize: 100,
	})
	err := ring.FlushDb().Err()
	if err != nil {
		panic(err)
	}
	return ring
}

func testProcessor(t *testing.T, q processor.Queuer) {
	t.Parallel()

	_ = q.Purge()

	ch := make(chan time.Time)
	handler := func(hello, world string) error {
		if hello != "hello" {
			t.Fatalf("got %s, wanted hello", hello)
		}
		if world != "world" {
			t.Fatalf("got %s, wanted world", world)
		}
		ch <- time.Now()
		return nil
	}

	msg := msgqueue.NewMessage("hello", "world")
	err := q.Add(msg)
	if err != nil {
		t.Fatal(err)
	}

	p := processor.Start(q, &msgqueue.Options{
		Handler: handler,
	})

	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("message was not processed")
	}

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
}

func testDelay(t *testing.T, q processor.Queuer) {
	t.Parallel()

	_ = q.Purge()

	handlerCh := make(chan time.Time, 10)
	handler := func() {
		handlerCh <- time.Now()
	}

	start := time.Now()

	msg := msgqueue.NewMessage()
	msg.Delay = 5 * time.Second
	err := q.Add(msg)
	if err != nil {
		t.Fatal(err)
	}

	p := processor.Start(q, &msgqueue.Options{
		Handler: handler,
	})

	tm := <-handlerCh
	sub := tm.Sub(start)
	if !durEqual(sub, msg.Delay) {
		t.Fatalf("message was delayed by %s, wanted %s", sub, msg.Delay)
	}

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
}

func testRetry(t *testing.T, q processor.Queuer) {
	t.Parallel()

	_ = q.Purge()

	handlerCh := make(chan time.Time, 10)
	handler := func(hello, world string) error {
		handlerCh <- time.Now()
		return errors.New("fake error")
	}

	var fallbackCount int64
	fallbackHandler := func() error {
		atomic.AddInt64(&fallbackCount, 1)
		return nil
	}

	msg := msgqueue.NewMessage("hello", "world")
	err := q.Add(msg)
	if err != nil {
		t.Fatal(err)
	}

	p := processor.Start(q, &msgqueue.Options{
		Handler:         handler,
		FallbackHandler: fallbackHandler,
		RetryLimit:      3,
		MinBackoff:      time.Second,
	})

	timings := []time.Duration{0, time.Second, 3 * time.Second}
	testTimings(t, handlerCh, timings)

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}

	if n := atomic.LoadInt64(&fallbackCount); n != 1 {
		t.Fatalf("fallback handler is called %d times, wanted 1", n)
	}
}

func testNamedMessage(t *testing.T, q processor.Queuer) {
	t.Parallel()

	_ = q.Purge()

	ch := make(chan time.Time, 10)
	handler := func() error {
		ch <- time.Now()
		return nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := msgqueue.NewMessage()
			msg.Name = "the-name"
			err := q.Add(msg)
			if err != nil && err != msgqueue.ErrDuplicate {
				t.Fatal(err)
			}
		}()
	}
	wg.Wait()

	p := processor.Start(q, &msgqueue.Options{
		Handler: handler,
	})

	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("message was not processed")
	}

	select {
	case <-ch:
		t.Fatalf("message was processed twice")
	default:
	}

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
}

func testCallOnce(t *testing.T, q processor.Queuer) {
	t.Parallel()

	_ = q.Purge()
	ring := redisRing()

	ch := make(chan time.Time, 10)
	handler := func() error {
		ch <- time.Now()
		return nil
	}

	go func() {
		for i := 0; i < 3; i++ {
			for j := 0; j < 10; j++ {
				err := q.CallOnce(time.Second)
				if err != nil && err != msgqueue.ErrDuplicate {
					t.Fatal(err)
				}
			}

			time.Sleep(time.Second)
		}
	}()

	p := processor.Start(q, &msgqueue.Options{
		Handler: handler,
		Redis:   ring,
	})
	go printStats(p)

	for i := 0; i < 3; i++ {
		select {
		case <-ch:
		case <-time.After(10 * time.Second):
			t.Fatalf("message was not processed")
		}
	}

	select {
	case <-ch:
		t.Fatalf("message was processed twice")
	default:
	}

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
}

func testRateLimit(t *testing.T, q processor.Queuer) {
	t.Parallel()

	_ = q.Purge()
	ring := redisRing()

	var count int64
	handler := func() error {
		atomic.AddInt64(&count, 1)
		return nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := msgqueue.NewMessage()
			err := q.Add(msg)
			if err != nil {
				t.Fatal(err)
			}
		}()
	}
	wg.Wait()

	p := processor.Start(q, &msgqueue.Options{
		Handler:      handler,
		WorkerNumber: 2,
		RateLimit:    rate.Every(time.Second),
		Redis:        ring,
	})
	go printStats(p)

	time.Sleep(5 * time.Second)

	if n := atomic.LoadInt64(&count); n-5 > 2 {
		t.Fatalf("processed %d messages, wanted 5", n)
	}

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
}

type RateLimitError string

func (e RateLimitError) Error() string {
	return string(e)
}

func (RateLimitError) Delay() time.Duration {
	return 5 * time.Second
}

func testDelayer(t *testing.T, q processor.Queuer) {
	t.Parallel()

	_ = q.Purge()

	handlerCh := make(chan time.Time, 10)
	handler := func() error {
		handlerCh <- time.Now()
		return RateLimitError("fake error")
	}

	err := q.Call()
	if err != nil {
		t.Fatal(err)
	}

	p := processor.Start(q, &msgqueue.Options{
		Handler:    handler,
		MinBackoff: time.Second,
		RetryLimit: 3,
	})

	timings := []time.Duration{0, 5 * time.Second, 5 * time.Second}
	testTimings(t, handlerCh, timings)

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
}

func durEqual(d1, d2 time.Duration) bool {
	return d1 >= d2 && d2-d1 < 3*time.Second
}

func testTimings(t *testing.T, ch chan time.Time, timings []time.Duration) {
	start := time.Now()
	for i, timing := range timings {
		tm := <-ch
		since := tm.Sub(start)
		if !durEqual(since, timing) {
			t.Fatalf("#%d: timing is %s, wanted %s", i+1, since, timing)
		}
	}
}
