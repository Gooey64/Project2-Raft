package pkg

import (
	"testing"
	"time"
)

// randomTimeout(min) must return a channel that fires once, after a random
// delay in [min, 2*min].

func TestRandomTimeout_NotNil(t *testing.T) {
	if randomTimeout(time.Millisecond) == nil {
		t.Fatal("randomTimeout returned a nil channel, which never fires")
	}
}

func TestRandomTimeout_FiresWithinBounds(t *testing.T) {
	const min = 30 * time.Millisecond
	// Allow scheduling and timer-resolution slack above 2*min only. The lower
	// bound is strict apart from a millisecond of clock-granularity tolerance.
	const slack = 40 * time.Millisecond

	for i := 0; i < 10; i++ {
		start := time.Now()
		select {
		case <-randomTimeout(min):
		case <-time.After(waitTimeout):
			t.Fatal("randomTimeout never fired")
		}
		d := time.Since(start)
		if d < min-time.Millisecond {
			t.Errorf("fired after %v, earlier than the minimum %v", d, min)
		}
		if d > 2*min+slack {
			t.Errorf("fired after %v, later than 2*min (%v) plus slack", d, 2*min)
		}
	}
}

func TestRandomTimeout_IsRandomized(t *testing.T) {
	// With a fixed timeout, every node would time out together and split votes
	// forever. Over 10 samples drawn from a 50ms-wide window, the spread should
	// comfortably exceed a few milliseconds.
	const min = 50 * time.Millisecond
	var lo, hi time.Duration
	for i := 0; i < 10; i++ {
		start := time.Now()
		select {
		case <-randomTimeout(min):
		case <-time.After(waitTimeout):
			t.Fatal("randomTimeout never fired")
		}
		d := time.Since(start)
		if i == 0 || d < lo {
			lo = d
		}
		if d > hi {
			hi = d
		}
	}
	if hi-lo < 3*time.Millisecond {
		t.Errorf("10 timeouts spanned only %v (min %v, max %v); timeouts do not look randomized", hi-lo, lo, hi)
	}
}

func TestRandomTimeout_NonPositiveFiresImmediately(t *testing.T) {
	// rand.Int63n panics on n <= 0, so a zero or negative minimum must be
	// handled explicitly and must still produce a channel that fires.
	for _, min := range []time.Duration{0, -time.Second} {
		select {
		case <-randomTimeout(min):
		case <-time.After(waitTimeout):
			t.Fatalf("randomTimeout(%v) never fired", min)
		}
	}
}

func TestRandomTimeout_IndependentChannels(t *testing.T) {
	a := randomTimeout(5 * time.Millisecond)
	b := randomTimeout(5 * time.Millisecond)
	if a == b {
		t.Fatal("each call must return its own channel")
	}
	for _, ch := range []<-chan time.Time{a, b} {
		select {
		case <-ch:
		case <-time.After(waitTimeout):
			t.Fatal("channel never fired")
		}
	}
}
