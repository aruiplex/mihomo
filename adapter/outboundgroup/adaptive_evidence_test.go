package outboundgroup

import (
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

// Matches the live failure: an old stream resets after newer connections on
// the same sole eligible route have already established successfully.
func TestAdaptiveEarlierConnectionCannotCoolNewRoute(t *testing.T) {
	old := &adaptiveTestConn{readError: syscall.ECONNRESET}
	calls := 0
	g := adaptiveTestGroup(t, AdaptiveOption{}, adaptiveTestNode("only", func(context.Context, *C.Metadata) (C.Conn, error) {
		calls++
		if calls == 1 {
			return old, nil
		}
		return &adaptiveTestConn{}, nil
	}))
	now := time.Now()
	g.now = func() time.Time { return now }
	metadata := adaptiveTestMetadata("service.test")
	first, err := g.DialContext(context.Background(), metadata)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	now = now.Add(15 * time.Minute)
	latest, err := g.DialContext(context.Background(), metadata)
	if err != nil {
		t.Fatal(err)
	}
	defer latest.Close()
	if _, err := first.Read(make([]byte, 1)); !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("missing original stream error: %v", err)
	}
	if _, err := latest.Write([]byte("still healthy")); err != nil {
		t.Fatalf("healthy established stream changed: %v", err)
	}
	next, err := g.DialContext(context.Background(), metadata)
	if err != nil {
		t.Fatalf("old connection failure cooled a newer healthy route: %v", err)
	}
	defer next.Close()
	if calls != 3 || old.closes != 0 {
		t.Fatalf("unexpected replay or closing: calls=%d oldCloses=%d", calls, old.closes)
	}
}

func TestAdaptiveEarlierConnectionCannotReplaceFeedback(t *testing.T) {
	raw := &adaptiveTestConn{readError: syscall.ECONNRESET}
	g := adaptiveTestGroup(t, AdaptiveOption{}, adaptiveTestNode("only", func(context.Context, *C.Metadata) (C.Conn, error) { return raw, nil }))
	c, err := g.DialContext(context.Background(), adaptiveTestMetadata("feedback.test"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := g.Feedback("feedback.test:443", "unusable"); err != nil {
		t.Fatal(err)
	}
	before := *g.records["feedback.test:443\x00only"]
	c.Read(make([]byte, 1))
	after := g.records["feedback.test:443\x00only"]
	if after.Reason != before.Reason || !after.Until.Equal(before.Until) || after.Failures != before.Failures+1 {
		t.Fatalf("earlier stream replaced explicit feedback: before=%+v after=%+v", before, after)
	}
}

func TestAdaptiveEarlierDialFailureCannotCoolNewRoute(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	g := adaptiveTestGroup(t, AdaptiveOption{}, adaptiveTestNode("only", func(context.Context, *C.Metadata) (C.Conn, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-finish
			return nil, syscall.ECONNRESET
		}
		return &adaptiveTestConn{}, nil
	}))
	metadata := adaptiveTestMetadata("parallel.test")
	failed := make(chan error, 1)
	go func() { _, err := g.DialContext(context.Background(), metadata); failed <- err }()
	<-started
	current, err := g.DialContext(context.Background(), metadata)
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	close(finish)
	if err := <-failed; !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("original caller lost failure: %v", err)
	}
	next, err := g.DialContext(context.Background(), metadata)
	if err != nil {
		t.Fatalf("earlier dial failure cooled newer route: %v", err)
	}
	defer next.Close()
	if calls.Load() != 3 {
		t.Fatalf("unexpected connection replay: %d", calls.Load())
	}
}

func TestAdaptiveEarlierDialSuccessCannotEraseFeedback(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	g := adaptiveTestGroup(t, AdaptiveOption{}, adaptiveTestNode("only", func(context.Context, *C.Metadata) (C.Conn, error) {
		if calls.Add(1) == 2 {
			close(started)
			<-finish
		}
		return &adaptiveTestConn{}, nil
	}))
	metadata := adaptiveTestMetadata("feedback.test")
	first, err := g.DialContext(context.Background(), metadata)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	done := make(chan C.Conn, 1)
	go func() {
		c, err := g.DialContext(context.Background(), metadata)
		if err != nil {
			done <- nil
			return
		}
		done <- c
	}()
	<-started
	if err := g.Feedback("feedback.test:443", "unusable"); err != nil {
		t.Fatal(err)
	}
	before := *g.records["feedback.test:443\x00only"]
	close(finish)
	c := <-done
	if c == nil {
		t.Fatal("already submitted successful establishment was discarded")
	}
	defer c.Close()
	after := g.records["feedback.test:443\x00only"]
	if after.Reason != before.Reason || !after.Until.Equal(before.Until) {
		t.Fatalf("earlier in-flight success erased explicit feedback: before=%+v after=%+v", before, after)
	}
	if _, err := g.DialContext(context.Background(), metadata); err == nil {
		t.Fatal("feedback should still cool new requests")
	}
}

func TestAdaptiveEvidenceRemainsScopedToTarget(t *testing.T) {
	raw := &adaptiveTestConn{readError: syscall.ECONNRESET}
	g := adaptiveTestGroup(t, AdaptiveOption{}, adaptiveTestNode("only", func(_ context.Context, m *C.Metadata) (C.Conn, error) {
		if m.Host == "failed.test" {
			return raw, nil
		}
		return &adaptiveTestConn{}, nil
	}))
	first, err := g.DialContext(context.Background(), adaptiveTestMetadata("failed.test"))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	other, err := g.DialContext(context.Background(), adaptiveTestMetadata("other.test"))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	first.Read(make([]byte, 1))
	if _, err := g.DialContext(context.Background(), adaptiveTestMetadata("failed.test")); err == nil {
		t.Fatal("another target must not supersede this route failure")
	}
	next, err := g.DialContext(context.Background(), adaptiveTestMetadata("other.test"))
	if err != nil {
		t.Fatal(err)
	}
	next.Close()
}

func TestAdaptiveEarlierDialCannotPenalizeReplacementRecord(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	g := adaptiveTestGroup(t, AdaptiveOption{}, adaptiveTestNode("only", func(context.Context, *C.Metadata) (C.Conn, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-finish
			return nil, syscall.ECONNRESET
		}
		return &adaptiveTestConn{}, nil
	}))
	done := make(chan error, 1)
	metadata := adaptiveTestMetadata("replacement.test")
	go func() { _, err := g.DialContext(context.Background(), metadata); done <- err }()
	<-started
	g.mu.Lock()
	delete(g.records, "replacement.test:443\x00only")
	replacement := g.record("replacement.test:443", "only")
	g.mu.Unlock()
	close(finish)
	if err := <-done; !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("original failure lost: %v", err)
	}
	if replacement.Failures != 0 || replacement.Reason != "" || !replacement.Until.IsZero() {
		t.Fatalf("earlier identity/evicted record failure attributed to replacement: %+v", replacement)
	}
	c, err := g.DialContext(context.Background(), metadata)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
}

func TestAdaptiveEarlierStreamCannotPenalizeReplacementRecord(t *testing.T) {
	raw := &adaptiveTestConn{readError: syscall.ECONNRESET}
	g := adaptiveTestGroup(t, AdaptiveOption{}, adaptiveTestNode("only", func(context.Context, *C.Metadata) (C.Conn, error) { return raw, nil }))
	c, err := g.DialContext(context.Background(), adaptiveTestMetadata("replacement.test"))
	if err != nil {
		t.Fatal(err)
	}
	delete(g.records, "replacement.test:443\x00only")
	replacement := g.record("replacement.test:443", "only")
	c.Read(make([]byte, 1))
	c.Close()
	if replacement.Failures != 0 || replacement.Closed != 0 || !replacement.Until.IsZero() {
		t.Fatalf("earlier stream attributed to replacement record: %+v", replacement)
	}
}

func TestAdaptiveEarlierSuccessCannotEraseNewerTransportFailure(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	g := adaptiveTestGroup(t, AdaptiveOption{}, adaptiveTestNode("only", func(context.Context, *C.Metadata) (C.Conn, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-finish
			return &adaptiveTestConn{}, nil
		}
		return &adaptiveTestConn{readError: syscall.ECONNRESET}, nil
	}))
	metadata := adaptiveTestMetadata("parallel.test")
	done := make(chan C.Conn, 1)
	go func() { c, _ := g.DialContext(context.Background(), metadata); done <- c }()
	<-started
	current, err := g.DialContext(context.Background(), metadata)
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	current.Read(make([]byte, 1))
	close(finish)
	earlier := <-done
	if earlier == nil {
		t.Fatal("earlier caller lost successful connection")
	}
	defer earlier.Close()
	if _, err := g.DialContext(context.Background(), metadata); err == nil {
		t.Fatal("earlier success erased current transport failure")
	}
}

func TestAdaptiveCurrentFailureRefreshesEvidenceTTL(t *testing.T) {
	raw := &adaptiveTestConn{readError: syscall.ECONNRESET}
	g := adaptiveTestGroup(t, AdaptiveOption{QualityTTL: 60}, adaptiveTestNode("only", func(context.Context, *C.Metadata) (C.Conn, error) { return raw, nil }))
	now := time.Now()
	g.now = func() time.Time { return now }
	c, err := g.DialContext(context.Background(), adaptiveTestMetadata("long.test"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	now = now.Add(61 * time.Second)
	c.Read(make([]byte, 1))
	if _, err := g.DialContext(context.Background(), adaptiveTestMetadata("long.test")); err == nil {
		t.Fatal("fresh current transport failure discarded as expired history")
	}
}

func TestAdaptiveSupersededFailureDoesNotRefreshEvidenceTTL(t *testing.T) {
	raw := &adaptiveTestConn{readError: syscall.ECONNRESET}
	calls := 0
	g := adaptiveTestGroup(t, AdaptiveOption{QualityTTL: 60}, adaptiveTestNode("only", func(context.Context, *C.Metadata) (C.Conn, error) {
		calls++
		if calls == 1 {
			return raw, nil
		}
		return &adaptiveTestConn{}, nil
	}))
	now := time.Now()
	g.now = func() time.Time { return now }
	metadata := adaptiveTestMetadata("long.test")
	old, err := g.DialContext(context.Background(), metadata)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	now = now.Add(time.Second)
	current, err := g.DialContext(context.Background(), metadata)
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	record := g.records["long.test:443\x00only"]
	touched := record.Touched
	now = now.Add(30 * time.Second)
	old.Read(make([]byte, 1))
	if !record.Touched.Equal(touched) {
		t.Fatal("superseded failure extended newer evidence TTL")
	}
}

func TestAdaptiveEarlierSuccessCannotRetargetFeedback(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	a := adaptiveTestNode("A", func(context.Context, *C.Metadata) (C.Conn, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-finish
			return &adaptiveTestConn{}, nil
		}
		return nil, syscall.ECONNRESET
	})
	g := adaptiveTestGroup(t, AdaptiveOption{}, a, adaptiveTestNode("B", adaptiveTestOK))
	metadata := adaptiveTestMetadata("two.test")
	done := make(chan C.Conn, 1)
	go func() { c, _ := g.DialContext(context.Background(), metadata); done <- c }()
	<-started
	current, err := g.DialContext(context.Background(), metadata)
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	if err := g.Feedback("two.test:443", "unusable"); err != nil {
		t.Fatal(err)
	}
	close(finish)
	earlier := <-done
	if earlier == nil {
		t.Fatal("earlier caller lost connection")
	}
	defer earlier.Close()
	if err := g.Feedback("two.test:443", "disconnect"); err != nil {
		t.Fatal(err)
	}
	if g.records["two.test:443\x00B"].Reason != "feedback:disconnect" {
		t.Fatal("earlier dial success retargeted feedback away from newer request's B route")
	}
}
